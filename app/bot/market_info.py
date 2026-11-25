"""Live lookup of OI across configured exchanges + report helpers."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import aiohttp
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy.ext.asyncio import AsyncSession

from app.collectors import build_collectors
from app.config import AppConfig
from app.core.metrics import funding_to_8h, oi_tokens
from app.core.normalize import parse_window_to_seconds
from app.core.timeutils import ensure_utc, utcnow
from app.storage import repo

logger = logging.getLogger(__name__)

TZ_UTC7 = timezone(timedelta(hours=7))

EXCHANGE_LABELS = {
    "binance": "Binance",
    "bybit": "Bybit",
    "bitget": "Bitget",
    "hyperliquid": "Hyperliquid",
    "aster": "Aster",
    "bingx": "BingX",
    "gate": "Gate",
    "mexc": "Mexc",
}


def exchange_label(name: str) -> str:
    return EXCHANGE_LABELS.get(name.lower(), name.capitalize())


def format_dt_utc7(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ_UTC7).strftime("%d.%m %H:%M")

REPORT_WINDOWS = ["5m", "15m", "1h", "4h", "1d", "1w"]


@dataclass
class ExchangeOiRow:
    exchange: str
    available: bool
    oi_usd: float | None
    price: float | None = None
    long_pct: float | None = None
    short_pct: float | None = None
    funding_rate: float | None = None
    contract_symbol: str | None = None
    error: str | None = None


def format_oi(v: float | None) -> str:
    if v is None:
        return "—"
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.2f}B"
    if abs_v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if abs_v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.4g}"


def format_usd_short(v: float | None) -> str:
    """4.12M$ style."""
    if v is None:
        return "—"
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B$"
    if abs_v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M$"
    if abs_v >= 1_000:
        return f"{v / 1_000:.1f}K$"
    return f"{v:.4g}$"


def format_qty(v: float | None) -> str:
    if v is None:
        return "—"
    abs_v = abs(v)
    if abs_v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if abs_v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if abs_v >= 1_000:
        return f"{v / 1_000:.1f}K"
    return f"{v:.4g}"


def format_price(v: float | None) -> str:
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6g}"


def format_funding(rate: float | None, exchange: str) -> str:
    value = funding_to_8h(rate, exchange)
    if value is None:
        return "—"
    return f"{value:+.3f}%"


def format_ls(long_pct: float | None, short_pct: float | None) -> str:
    if long_pct is None:
        return "—"
    short = short_pct if short_pct is not None else 100.0 - long_pct
    return f"{long_pct:.1f}/{short:.1f}"


def format_ls_ratio(long_pct: float | None, short_pct: float | None) -> str:
    if long_pct is None:
        return "—"
    short = short_pct if short_pct is not None else 100.0 - long_pct
    if short is None or short == 0:
        return "—"
    return f"{long_pct / short:.2f}"


def format_pct(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v:+.2f}%"


async def lookup_base_on_exchanges(base: str, cfg: AppConfig) -> list[ExchangeOiRow]:
    """Probe configured exchanges for base asset perpetual OI + price + L/S."""
    base = base.upper()
    rows: list[ExchangeOiRow] = []
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        collectors = build_collectors(session, cfg)
        sem = asyncio.Semaphore(cfg.resources.max_concurrent_requests)

        async def one(name: str, coll) -> ExchangeOiRow:
            async with sem:
                try:
                    snaps = await coll.discover({base})
                    match = [s for s in snaps if s.base_symbol.upper() == base]
                    if not match:
                        return ExchangeOiRow(
                            exchange=name, available=False, oi_usd=None, price=None
                        )
                    best = max(match, key=lambda s: s.oi_usd or 0.0)
                    price = best.mark_price if best.mark_price is not None else best.last_price
                    return ExchangeOiRow(
                        exchange=name,
                        available=True,
                        oi_usd=best.oi_usd,
                        price=price,
                        long_pct=best.long_pct,
                        short_pct=best.short_pct,
                        funding_rate=best.funding_rate,
                        contract_symbol=best.contract_symbol,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("lookup %s on %s: %s", base, name, e)
                    return ExchangeOiRow(
                        exchange=name,
                        available=False,
                        oi_usd=None,
                        price=None,
                        error=str(e),
                    )

        results = await asyncio.gather(*[one(n, c) for n, c in collectors.items()])
        by_name = {r.exchange: r for r in results}
        for name in cfg.exchanges:
            rows.append(
                by_name.get(name.lower())
                or ExchangeOiRow(
                    exchange=name.lower(), available=False, oi_usd=None, price=None
                )
            )
    return rows


@dataclass
class SpotMarketRow:
    exchange: str
    available: bool
    price: float | None = None
    volume_usd: float | None = None
    symbol: str | None = None


def _as_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


async def lookup_spot_markets(base: str) -> list[SpotMarketRow]:
    """USDT-спот на биржах, где есть публичный тикер. Нет пары — available=False."""
    base = base.upper()
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        results = await asyncio.gather(
            _spot_binance(http, base),
            _spot_bybit(http, base),
            _spot_bitget(http, base),
            _spot_gate(http, base),
            _spot_mexc(http, base),
            _spot_bingx(http, base),
        )
    return list(results)


async def _spot_binance(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}USDT"
    try:
        async with http.get("https://api.binance.com/api/v3/ticker/24hr", params={"symbol": symbol}) as resp:
            if resp.status != 200:
                return SpotMarketRow("binance", False, symbol=symbol)
            data = await resp.json()
        return SpotMarketRow(
            "binance",
            True,
            price=_as_float(data.get("lastPrice")),
            volume_usd=_as_float(data.get("quoteVolume")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot binance %s: %s", base, exc)
        return SpotMarketRow("binance", False, symbol=symbol)


async def _spot_bybit(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}USDT"
    try:
        async with http.get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "spot", "symbol": symbol},
        ) as resp:
            body = await resp.json()
        items = ((body.get("result") or {}).get("list") or []) if resp.status == 200 else []
        if not items:
            return SpotMarketRow("bybit", False, symbol=symbol)
        row = items[0]
        return SpotMarketRow(
            "bybit",
            True,
            price=_as_float(row.get("lastPrice")),
            volume_usd=_as_float(row.get("turnover24h")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot bybit %s: %s", base, exc)
        return SpotMarketRow("bybit", False, symbol=symbol)


async def _spot_bitget(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}USDT"
    try:
        async with http.get(
            "https://api.bitget.com/api/v2/spot/market/tickers",
            params={"symbol": symbol},
        ) as resp:
            body = await resp.json()
        data = body.get("data") or []
        if resp.status != 200 or body.get("code") not in (None, "00000") or not data:
            return SpotMarketRow("bitget", False, symbol=symbol)
        row = data[0]
        return SpotMarketRow(
            "bitget",
            True,
            price=_as_float(row.get("lastPr")),
            volume_usd=_as_float(row.get("quoteVolume") or row.get("usdtVolume")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot bitget %s: %s", base, exc)
        return SpotMarketRow("bitget", False, symbol=symbol)


async def _spot_gate(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}_USDT"
    try:
        async with http.get(
            "https://api.gateio.ws/api/v4/spot/tickers",
            params={"currency_pair": symbol},
        ) as resp:
            if resp.status != 200:
                return SpotMarketRow("gate", False, symbol=symbol)
            data = await resp.json()
        if not isinstance(data, list) or not data:
            return SpotMarketRow("gate", False, symbol=symbol)
        row = data[0]
        return SpotMarketRow(
            "gate",
            True,
            price=_as_float(row.get("last")),
            volume_usd=_as_float(row.get("quote_volume")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot gate %s: %s", base, exc)
        return SpotMarketRow("gate", False, symbol=symbol)


async def _spot_mexc(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}USDT"
    try:
        async with http.get("https://api.mexc.com/api/v3/ticker/24hr", params={"symbol": symbol}) as resp:
            if resp.status != 200:
                return SpotMarketRow("mexc", False, symbol=symbol)
            data = await resp.json()
        if not isinstance(data, dict) or not data.get("lastPrice"):
            return SpotMarketRow("mexc", False, symbol=symbol)
        return SpotMarketRow(
            "mexc",
            True,
            price=_as_float(data.get("lastPrice")),
            volume_usd=_as_float(data.get("quoteVolume")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot mexc %s: %s", base, exc)
        return SpotMarketRow("mexc", False, symbol=symbol)


async def _spot_bingx(http: aiohttp.ClientSession, base: str) -> SpotMarketRow:
    symbol = f"{base}-USDT"
    try:
        async with http.get(
            "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
            params={"symbol": symbol},
        ) as resp:
            body = await resp.json()
        data = body.get("data")
        row = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
        if resp.status != 200 or not row or row.get("lastPrice") in (None, ""):
            return SpotMarketRow("bingx", False, symbol=symbol)
        return SpotMarketRow(
            "bingx",
            True,
            price=_as_float(row.get("lastPrice")),
            volume_usd=_as_float(row.get("quoteVolume")),
            symbol=symbol,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("spot bingx %s: %s", base, exc)
        return SpotMarketRow("bingx", False, symbol=symbol)


def format_checkall_report(
    base: str,
    spot_rows: list[SpotMarketRow],
    futures_rows: list[ExchangeOiRow],
) -> str:
    lines = [f"Токен: {base}", "", "Спотовый рынок", f"{'Биржа':<12} {'Цена':<12} {'Объём 24ч'}"]
    spot_found = [row for row in spot_rows if row.available]
    if not spot_found:
        lines.append("USDT-спот не найден")
    else:
        for row in sorted(spot_found, key=lambda item: item.volume_usd or 0.0, reverse=True):
            lines.append(
                f"{exchange_label(row.exchange):<12} "
                f"{format_price(row.price):<12} "
                f"{format_oi(row.volume_usd)}"
            )
    missing_spot = [exchange_label(row.exchange) for row in spot_rows if not row.available]
    if missing_spot:
        lines.append("нет спота: " + ", ".join(missing_spot))

    lines.extend(["", "Фьючерсный рынок", f"{'Биржа':<12} {'Цена':<12} {'OI':<12} {'F'}"])
    futures_found = [row for row in futures_rows if row.available]
    if not futures_found:
        lines.append("USDT-фьючерс не найден")
    else:
        for row in sorted(futures_found, key=lambda item: item.oi_usd or 0.0, reverse=True):
            lines.append(
                f"{exchange_label(row.exchange):<12} "
                f"{format_price(row.price):<12} "
                f"{format_oi(row.oi_usd):<12} "
                f"{format_funding(row.funding_rate, row.exchange)}"
            )
    missing_fut = [exchange_label(row.exchange) for row in futures_rows if not row.available]
    if missing_fut:
        lines.append("нет фьючерса: " + ", ".join(missing_fut))
    return "\n".join(lines)


def format_check_report(base: str, rows: list[ExchangeOiRow]) -> str:
    available = [r for r in rows if r.available]
    lines = [f"Токен: {base}", ""]
    if not available:
        lines.append("На наблюдаемых биржах контракт не найден.")
        return "\n".join(lines)

    lines.append(f"{'Exch':<12} {'Price':<12} {'OI':<12} {'Long/Short':<12} {'F'}")
    lines.append("-" * 60)
    for r in sorted(available, key=lambda row: row.oi_usd or 0.0, reverse=True):
        lines.append(
            f"{r.exchange.capitalize():<12} "
            f"{format_price(r.price):<12} "
            f"{format_oi(r.oi_usd):<12} "
            f"{format_ls(r.long_pct, r.short_pct):<12} "
            f"{format_funding(r.funding_rate, r.exchange)}"
        )
    return "\n".join(lines)


def check_add_keyboard(base: str, rows: list[ExchangeOiRow]) -> InlineKeyboardMarkup | None:
    available = [r for r in rows if r.available]
    if not available:
        return None
    buttons: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=f"добавить все ({base})",
                callback_data=f"chkaddall:{base}",
            )
        ]
    ]
    for r in available:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"добавить {base} {r.exchange.capitalize()}",
                    callback_data=f"chkadd:{base}:{r.exchange}",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def format_added_at_utc7(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local = dt.astimezone(TZ_UTC7)
    return local.strftime("%d.%m.%Y %H:%M")


def format_list_report(
    entries: list[tuple[str, datetime | None]],
    exch_map: dict[str, list[str]],
) -> str:
    lines = ["Отслеживаются", ""]
    if not entries:
        lines.append("(пусто)")
        return "\n".join(lines)
    for sym, created_at in entries:
        exchanges = exch_map.get(sym) or []
        ex_part = ", ".join(exchanges) if exchanges else "биржи пока не определены"
        when = format_added_at_utc7(created_at)
        lines.append(f"{sym} - {ex_part}, {when}")
    return "\n".join(lines)


def _tick_price(tick) -> float | None:
    if tick is None:
        return None
    return tick.mark_price if tick.mark_price is not None else tick.last_price


def _qt_qs(qty: float | None, usd: float | None) -> str:
    return f"{format_qty(qty)}/{format_usd_short(usd)}"


def _row(
    name: str,
    qt_qs: str,
    oi_pct: str,
    price_pct: str,
    ls: str,
    voll: str,
) -> str:
    return f"{name:<9} {qt_qs:<16} {oi_pct:<8} {price_pct:<8} {ls:<5} {voll}"


async def build_otchet_report(
    session: AsyncSession,
    user_id: int | None,
    ticker: str,
) -> str:
    """Табличная сводка: now + таймфреймы, QT/Q$, L/S ratio."""
    display, legs = await repo.resolve_report_legs(session, user_id, ticker)
    if not legs:
        return f"{display}\n\nНет данных в истории. Добавьте тикер в мониторинг и подождите сбора."

    now = utcnow()
    live_legs: list[tuple[str, str, object]] = []
    for exchange, base in legs:
        tick = await repo.latest_tick_for_base(session, exchange, base)
        if tick:
            tick.ts_utc = ensure_utc(tick.ts_utc)
            live_legs.append((exchange, base, tick))
    if not live_legs:
        return f"{display}\n\nНет свежих тиков по тикеру."

    ath, atl = await repo.aggregate_oi_ath_atl(
        session, [(ex, base) for ex, base, _ in live_legs]
    )
    added_at = None
    if user_id is not None:
        item = await repo.get_watchlist_item(session, user_id, display)
        if item is not None:
            added_at = item.created_at

    lines = [display, format_dt_utc7(added_at), ""]

    def _extreme(label: str, point) -> None:
        if point is None:
            return
        usd, qty, ts = point
        lines.append(
            f"{label}  {_qt_qs(qty, usd)}  {format_dt_utc7(ts)}"
        )

    _extreme("OI_aggr_all_time_low ", atl)
    _extreme("OI_aggr_all_time_high", ath)
    if ath is not None or atl is not None:
        lines.append("")

    # —— now: текущие QT/Q$, без % изменения ——
    lines.append(_row("now", "QT/Q$", "ОИ", "Price", "L/S", "Voll"))
    now_rows = []
    for exchange, _base, tick_now in live_legs:
        qty = oi_tokens(tick_now)
        usd = tick_now.oi_usd
        long_pct = tick_now.long_pct
        short_pct = tick_now.short_pct if tick_now.short_pct is not None else (
            100.0 - long_pct if long_pct is not None else None
        )
        vol = tick_now.volume_24h
        now_rows.append((usd or 0.0, exchange, qty, usd, long_pct, short_pct, vol))
    now_rows.sort(key=lambda r: r[0], reverse=True)
    for _usd, exchange, qty, usd, long_pct, short_pct, vol in now_rows:
        lines.append(
            _row(
                exchange_label(exchange),
                _qt_qs(qty, usd),
                "—",
                "—",
                format_ls_ratio(long_pct, short_pct),
                format_usd_short(vol),
            )
        )
    lines.append("")

    any_window = False
    for window in REPORT_WINDOWS:
        sec = parse_window_to_seconds(window)
        before_ts = now - timedelta(seconds=sec)
        per_ex: list[
            tuple[
                str,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ] = []
        oi_qty_now_sum = 0.0
        oi_qty_before_sum = 0.0
        oi_usd_before_sum = 0.0
        oi_usd_now_sum = 0.0
        price_chgs: list[float] = []
        ls_longs: list[float] = []
        ls_shorts: list[float] = []
        vol_sum = 0.0
        usable = 0

        for exchange, base, tick_now in live_legs:
            before = await repo.tick_at_or_before_base(session, exchange, base, before_ts)
            if before is None:
                continue
            before.ts_utc = ensure_utc(before.ts_utc)
            age = (ensure_utc(tick_now.ts_utc) - before.ts_utc).total_seconds()
            if age < sec * 0.5:
                continue
            q_now = oi_tokens(tick_now)
            q_before = oi_tokens(before)
            usd_now = tick_now.oi_usd
            usd_before = before.oi_usd
            if q_before is None or q_before <= 0 or q_now is None:
                # fallback $ growth if no qty
                if usd_before is None or usd_before <= 0 or usd_now is None:
                    continue
                oi_chg = (usd_now / usd_before - 1.0) * 100.0
            else:
                oi_chg = (q_now / q_before - 1.0) * 100.0
            pn = _tick_price(tick_now)
            pb = _tick_price(before)
            price_chg = None
            if pn is not None and pb and pb > 0:
                price_chg = (pn / pb - 1.0) * 100.0
                price_chgs.append(price_chg)
            long_pct = before.long_pct
            short_pct = before.short_pct if before.short_pct is not None else (
                100.0 - long_pct if long_pct is not None else None
            )
            if long_pct is not None and short_pct is not None:
                ls_longs.append(long_pct)
                ls_shorts.append(short_pct)
            vol = before.volume_24h
            per_ex.append(
                (
                    exchange_label(exchange),
                    q_before,
                    usd_before,
                    oi_chg,
                    price_chg,
                    long_pct,
                    short_pct,
                    vol,
                )
            )
            if q_now:
                oi_qty_now_sum += q_now
            if q_before:
                oi_qty_before_sum += q_before
            if usd_now:
                oi_usd_now_sum += usd_now
            if usd_before:
                oi_usd_before_sum += usd_before
            if vol:
                vol_sum += vol
            usable += 1

        if usable == 0:
            continue
        any_window = True
        agg_oi_chg = None
        if oi_qty_before_sum > 0 and oi_qty_now_sum > 0:
            agg_oi_chg = (oi_qty_now_sum / oi_qty_before_sum - 1.0) * 100.0
        elif oi_usd_before_sum > 0 and oi_usd_now_sum > 0:
            agg_oi_chg = (oi_usd_now_sum / oi_usd_before_sum - 1.0) * 100.0
        agg_price = sum(price_chgs) / len(price_chgs) if price_chgs else None
        agg_ls = "—"
        if ls_longs and ls_shorts:
            avg_l = sum(ls_longs) / len(ls_longs)
            avg_s = sum(ls_shorts) / len(ls_shorts)
            agg_ls = format_ls_ratio(avg_l, avg_s)

        lines.append(_row(window, "QT/Q$", "ОИ", "Price", "L/S", "Voll"))
        lines.append(
            _row(
                "OI",
                _qt_qs(
                    oi_qty_before_sum if oi_qty_before_sum else None,
                    oi_usd_before_sum if oi_usd_before_sum else None,
                ),
                format_pct(agg_oi_chg),
                format_pct(agg_price),
                agg_ls,
                format_usd_short(vol_sum if vol_sum else None),
            )
        )
        per_ex.sort(key=lambda r: r[2] or 0.0, reverse=True)
        for name, q_ago, usd_ago, oi_chg, price_chg, long_pct, short_pct, vol in per_ex:
            lines.append(
                _row(
                    name,
                    _qt_qs(q_ago, usd_ago),
                    format_pct(oi_chg),
                    format_pct(price_chg),
                    format_ls_ratio(long_pct, short_pct),
                    format_usd_short(vol),
                )
            )
        lines.append("")

    if not any_window:
        lines.append("Недостаточно истории для таймфреймов (нужны тики старше окна).")
    return "\n".join(lines).rstrip() + "\n"
