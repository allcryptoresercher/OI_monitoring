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
from app.core.metrics import oi_tokens
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


def format_check_report(base: str, rows: list[ExchangeOiRow]) -> str:
    available = [r for r in rows if r.available]
    lines = [f"Токен: {base}", ""]
    if not available:
        lines.append("На наблюдаемых биржах контракт не найден.")
        return "\n".join(lines)

    lines.append(f"{'Exch':<12} {'Price':<12} {'OI':<12} {'Long/Short'}")
    lines.append("-" * 52)
    for r in available:
        lines.append(
            f"{r.exchange.capitalize():<12} "
            f"{format_price(r.price):<12} "
            f"{format_oi(r.oi_usd):<12} "
            f"{format_ls(r.long_pct, r.short_pct)}"
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
    return local.strftime("%d.%m.%Y %H:%M UTC+7")


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
