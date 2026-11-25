"""Свечи с биржи, маркет-дельта и сигналы для страницы графика."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.chart.align import TF_MS, align_market_delta
from app.chart.messages import signal_text
from app.delta.models import DeltaBucket, DeltaPair
from app.storage.models import ChartDrawing, Signal, WatchlistBinding, WatchlistItem

logger = logging.getLogger(__name__)

BINANCE_INTERVAL = {"5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
BYBIT_INTERVAL = {"5m": "5", "15m": "15", "1h": "60", "4h": "240", "1d": "D"}


async def list_coins(session: AsyncSession) -> list[str]:
    rows = await session.execute(select(WatchlistItem.base_symbol).distinct())
    return sorted({item.upper() for item in rows.scalars().all()})


async def coin_names(session: AsyncSession, coin: str) -> set[str]:
    """Имена в базах: отображаемый тикер и локальные тикеры привязок."""
    coin = coin.upper()
    names = {coin}
    result = await session.execute(
        select(WatchlistBinding.base_symbol, WatchlistItem.base_symbol)
        .join(WatchlistItem, WatchlistItem.id == WatchlistBinding.watchlist_id)
    )
    groups: dict[str, set[str]] = {}
    for local, display in result.all():
        groups.setdefault(display.upper(), set()).add(local.upper())
        groups.setdefault(local.upper(), set()).add(display.upper())
    changed = True
    while changed:
        changed = False
        for name in list(names):
            for extra in groups.get(name, ()):
                if extra not in names:
                    names.add(extra)
                    changed = True
    return names


async def fetch_klines(coin: str, tf: str, hours: int) -> tuple[list[dict], str]:
    """Сначала USDT-фьючерсы Binance и Bybit, затем спот тех же бирж."""
    limit = min(1000, max(20, int(hours * 3_600_000 / TF_MS[tf]) + 2))
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        for source, loader in (
            ("Binance futures", _binance_futures_klines),
            ("Bybit futures", _bybit_linear_klines),
            ("Binance spot", _binance_klines),
            ("Bybit spot", _bybit_klines),
        ):
            candles = await loader(http, coin, tf, limit)
            if candles:
                return candles, source
    return [], ""


async def _binance_futures_klines(http: aiohttp.ClientSession, coin: str, tf: str, limit: int) -> list[dict]:
    url = "https://fapi.binance.com/fapi/v1/klines"
    try:
        async with http.get(url, params={"symbol": f"{coin}USDT", "interval": BINANCE_INTERVAL[tf], "limit": limit}) as resp:
            if resp.status != 200:
                return []
            rows = await resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("binance futures klines %s: %s", coin, exc)
        return []
    if not isinstance(rows, list) or not rows:
        return []
    return [_kline(int(row[0]), row[1], row[2], row[3], row[4], row[5]) for row in rows]


async def _bybit_linear_klines(http: aiohttp.ClientSession, coin: str, tf: str, limit: int) -> list[dict]:
    return await _bybit_klines(http, coin, tf, limit, category="linear")


async def _binance_klines(http: aiohttp.ClientSession, coin: str, tf: str, limit: int) -> list[dict]:
    url = "https://api.binance.com/api/v3/klines"
    try:
        async with http.get(url, params={"symbol": f"{coin}USDT", "interval": BINANCE_INTERVAL[tf], "limit": limit}) as resp:
            if resp.status != 200:
                return []
            rows = await resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("binance klines %s: %s", coin, exc)
        return []
    if not isinstance(rows, list) or not rows:
        return []
    return [_kline(int(row[0]), row[1], row[2], row[3], row[4], row[5]) for row in rows]


async def _bybit_klines(
    http: aiohttp.ClientSession, coin: str, tf: str, limit: int, category: str = "spot"
) -> list[dict]:
    url = "https://api.bybit.com/v5/market/kline"
    try:
        async with http.get(
            url,
            params={"category": category, "symbol": f"{coin}USDT", "interval": BYBIT_INTERVAL[tf], "limit": limit},
        ) as resp:
            if resp.status != 200:
                return []
            body = await resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("bybit klines %s: %s", coin, exc)
        return []
    rows = list(reversed((body.get("result") or {}).get("list") or []))
    return [_kline(int(row[0]), row[1], row[2], row[3], row[4], row[5]) for row in rows]


def _kline(ts: int, open_, high, low, close, volume) -> dict:
    return {
        "timestamp": ts,
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume),
    }


async def market_delta_by_exchange(
    session: AsyncSession,
    names: set[str],
    candle_opens_ms: list[int],
    tf: str,
) -> dict[str, list[float | None]]:
    if not candle_opens_ms or not names:
        return {}
    start = datetime.fromtimestamp(candle_opens_ms[0] / 1000, tz=timezone.utc)
    end = datetime.fromtimestamp((candle_opens_ms[-1] + TF_MS[tf]) / 1000, tz=timezone.utc)
    stmt = (
        select(DeltaPair.exchange, DeltaBucket.bucket_start, DeltaBucket.buy_usd, DeltaBucket.sell_usd)
        .join(DeltaBucket, DeltaBucket.pair_id == DeltaPair.id)
        .where(DeltaPair.coin.in_(names))
        .where(DeltaBucket.bucket_start >= start)
        .where(DeltaBucket.bucket_start < end)
    )
    rows = (await session.execute(stmt)).all()
    packed: list[tuple[str, int, float]] = []
    for exchange, bucket_start, buy_usd, sell_usd in rows:
        if bucket_start.tzinfo is None:
            bucket_start = bucket_start.replace(tzinfo=timezone.utc)
        packed.append((exchange, int(bucket_start.timestamp() * 1000), float(buy_usd or 0) - float(sell_usd or 0)))
    return align_market_delta(candle_opens_ms, packed, TF_MS[tf])


async def load_signals(
    session: AsyncSession,
    names: set[str],
    start: datetime,
    end: datetime,
) -> list[dict]:
    stmt = (
        select(Signal)
        .where(Signal.base_symbol.in_(names))
        .where(Signal.ts_utc >= start)
        .where(Signal.ts_utc < end)
        .order_by(Signal.ts_utc)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    events: list[dict] = []
    for row in rows:
        codes = json.loads(row.types_json or "[]")
        text = signal_text(row)
        ts = row.ts_utc if row.ts_utc.tzinfo else row.ts_utc.replace(tzinfo=timezone.utc)
        price = row.price_at_signal
        for stack, code in enumerate(codes):
            events.append(
                {
                    "id": f"{row.id}-{code}",
                    "timestamp": int(ts.timestamp() * 1000),
                    "price": price,
                    "code": code,
                    "exchange": row.exchange,
                    "window": row.window,
                    "stack": stack,
                    "text": text,
                }
            )
    return events


def chart_window(hours: int) -> tuple[datetime, datetime]:
    end = datetime.now(timezone.utc) + timedelta(minutes=1)
    return end - timedelta(hours=hours), end


async def load_drawings(session: AsyncSession, coin: str, tf: str) -> list[dict]:
    stmt = (
        select(ChartDrawing)
        .where(ChartDrawing.coin == coin.upper(), ChartDrawing.timeframe == tf)
        .order_by(ChartDrawing.id)
    )
    rows = (await session.execute(stmt)).scalars().all()
    return [{"name": row.name, "points": json.loads(row.points_json)} for row in rows]


async def save_drawings(session: AsyncSession, coin: str, tf: str, items: list[dict]) -> None:
    coin = coin.upper()
    await session.execute(delete(ChartDrawing).where(ChartDrawing.coin == coin, ChartDrawing.timeframe == tf))
    now = datetime.now(timezone.utc)
    for item in items:
        name = str(item.get("name") or "")
        points = item.get("points") or []
        if name not in {"horizontalStraightLine", "segment", "rect"} or not points:
            continue
        session.add(
            ChartDrawing(
                coin=coin,
                timeframe=tf,
                name=name,
                points_json=json.dumps(points),
                updated_at=now,
            )
        )
    await session.commit()
