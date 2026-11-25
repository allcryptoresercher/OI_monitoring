"""Metrics calculation for OI windows."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Sequence

from app.config import AppConfig
from app.core.normalize import parse_window_to_seconds
from app.storage.models import RawTick


@dataclass
class WindowMetrics:
    window: str
    oi_now: float | None
    oi_before: float | None
    oi_qty_now: float | None
    oi_qty_before: float | None
    oi_growth_pct: float | None
    oi_log_change: float | None
    oi_z: float | None
    oi_pctl: float | None
    price_now: float | None
    price_before: float | None
    price_change_pct: float | None
    price_z: float | None
    gap: float | None
    ratio: float | None
    funding_8h: float | None
    oi_to_volume: float | None
    long_pct: float | None
    short_pct: float | None
    long_pct_before: float | None
    short_pct_before: float | None
    volume_24h: float | None
    history_days: float
    new_history: bool
    oi_volume_pctl: float | None = None


def _median(xs: list[float]) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2


def _mad(xs: list[float], med: float) -> float:
    if not xs:
        return float("nan")
    return _median([abs(x - med) for x in xs])


def robust_z(value: float, history: list[float]) -> float | None:
    if len(history) < 3:
        return None
    med = _median(history)
    mad = _mad(history, med)
    if mad == 0 or math.isnan(mad):
        return 0.0 if value == med else None
    return 0.6745 * (value - med) / mad


def percentile_rank(value: float, history: list[float]) -> float | None:
    if not history:
        return None
    below = sum(1 for x in history if x <= value)
    return 100.0 * below / len(history)


def window_history_ok(now: datetime, before: datetime | None, window_sec: int) -> bool:
    """Окно можно сравнивать, только если точка «до» лежит у его начала.

    Слишком короткий ряд (бот только запущен) и слишком длинный разрыв
    (простой в несколько часов) не считаются этим таймфреймом.
    """
    if before is None or window_sec <= 0:
        return False
    age = (now - before).total_seconds()
    return window_sec * 0.85 <= age <= window_sec * 1.5


def funding_to_8h(funding_rate: float | None, exchange: str) -> float | None:
    """Normalize funding to 8h period. Most perps quote per 8h already; HL may differ."""
    if funding_rate is None:
        return None
    rate_pct = funding_rate * 100.0
    ex = exchange.lower()
    if ex == "hyperliquid":
        return rate_pct * 8.0
    return rate_pct


def _price(t: RawTick) -> float | None:
    return t.mark_price if t.mark_price is not None else t.last_price


def oi_tokens(t: RawTick | None) -> float | None:
    if t is None:
        return None
    qty = getattr(t, "oi_qty", None)
    if qty:
        return float(qty)
    px = _price(t)
    if t.oi_usd and px and px > 0:
        return float(t.oi_usd) / px
    return None


def _ls_pct(tick: RawTick | None) -> tuple[float | None, float | None]:
    if tick is None:
        return None, None
    long_pct = tick.long_pct
    short_pct = tick.short_pct
    if short_pct is None and long_pct is not None:
        short_pct = 100.0 - long_pct
    return long_pct, short_pct


def growth_series(ticks: Sequence[RawTick], window_sec: int) -> list[float]:
    """OI growth % samples over history for z/pctl (по количеству токенов)."""
    if len(ticks) < 2:
        return []
    by_ts = list(ticks)
    out: list[float] = []
    step = max(1, len(by_ts) // 500)
    for i in range(0, len(by_ts), step):
        now = by_ts[i]
        qn = oi_tokens(now)
        if qn is None:
            continue
        target = now.ts_utc.timestamp() - window_sec
        before = None
        for j in range(i, -1, -1):
            qb = oi_tokens(by_ts[j])
            if by_ts[j].ts_utc.timestamp() <= target and qb:
                before = by_ts[j]
                break
        qb = oi_tokens(before)
        if qb and qb > 0:
            out.append((qn / qb - 1.0) * 100.0)
    return out


def price_growth_series(ticks: Sequence[RawTick], window_sec: int) -> list[float]:
    if len(ticks) < 2:
        return []
    by_ts = list(ticks)
    out: list[float] = []
    step = max(1, len(by_ts) // 500)
    for i in range(0, len(by_ts), step):
        now = by_ts[i]
        pn = _price(now)
        if pn is None:
            continue
        target = now.ts_utc.timestamp() - window_sec
        before = None
        for j in range(i, -1, -1):
            pj = _price(by_ts[j])
            if by_ts[j].ts_utc.timestamp() <= target and pj:
                before = by_ts[j]
                break
        if before:
            pb = _price(before)
            if pb and pb > 0:
                out.append((pn / pb - 1.0) * 100.0)
    return out


def oi_to_volume_series(ticks: Sequence[RawTick]) -> list[float]:
    out = []
    for t in ticks:
        if t.oi_usd and t.volume_24h and t.volume_24h > 0:
            out.append(t.oi_usd / t.volume_24h)
    return out


def compute_window_metrics(
    now: RawTick,
    before: RawTick | None,
    history_ticks: Sequence[RawTick],
    window: str,
    cfg: AppConfig,
    exchange: str,
) -> WindowMetrics:
    window_sec = parse_window_to_seconds(window)
    if history_ticks:
        first = history_ticks[0].ts_utc
        last = history_ticks[-1].ts_utc
        if first.tzinfo is None:
            first = first.replace(tzinfo=timezone.utc)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        history_days = max(0.0, (last - first).total_seconds() / 86400.0)
    else:
        history_days = 0.0
    new_history = history_days < cfg.history.minimum_days

    oi_now = now.oi_usd
    oi_before = before.oi_usd if before else None
    oi_qty_now = oi_tokens(now)
    oi_qty_before = oi_tokens(before)
    price_now = _price(now)
    price_before = _price(before) if before else None

    # Рост OI — по количеству токенов (fallback на $ если qty нет)
    oi_growth = None
    oi_log = None
    if oi_qty_now is not None and oi_qty_before and oi_qty_before > 0:
        oi_growth = (oi_qty_now / oi_qty_before - 1.0) * 100.0
        oi_log = math.log(oi_qty_now / oi_qty_before)
    elif oi_now is not None and oi_before and oi_before > 0:
        oi_growth = (oi_now / oi_before - 1.0) * 100.0
        oi_log = math.log(oi_now / oi_before)

    price_change = None
    if price_now is not None and price_before and price_before > 0:
        price_change = (price_now / price_before - 1.0) * 100.0

    oi_z = oi_pctl = price_z = None
    oi_vol_pctl = None
    if not new_history and oi_growth is not None:
        oi_hist = growth_series(history_ticks, window_sec)
        oi_z = robust_z(oi_growth, oi_hist)
        oi_pctl = percentile_rank(oi_growth, oi_hist)
    if not new_history and price_change is not None:
        p_hist = price_growth_series(history_ticks, window_sec)
        price_z = robust_z(price_change, p_hist)
    if not new_history and oi_now and now.volume_24h and now.volume_24h > 0:
        ov = oi_now / now.volume_24h
        ov_hist = oi_to_volume_series(history_ticks)
        oi_vol_pctl = percentile_rank(ov, ov_hist)

    gap = None
    ratio = None
    if oi_growth is not None and price_change is not None:
        gap = oi_growth - price_change
        if oi_growth != 0:
            ratio = price_change / oi_growth
        else:
            ratio = None

    oi_to_vol = None
    if oi_now and now.volume_24h and now.volume_24h > 0:
        oi_to_vol = oi_now / now.volume_24h

    long_pct, short_pct = _ls_pct(now)
    long_before, short_before = _ls_pct(before)

    return WindowMetrics(
        window=window,
        oi_now=oi_now,
        oi_before=oi_before,
        oi_qty_now=oi_qty_now,
        oi_qty_before=oi_qty_before,
        oi_growth_pct=oi_growth,
        oi_log_change=oi_log,
        oi_z=oi_z,
        oi_pctl=oi_pctl,
        price_now=price_now,
        price_before=price_before,
        price_change_pct=price_change,
        price_z=price_z,
        gap=gap,
        ratio=ratio,
        funding_8h=funding_to_8h(now.funding_rate, exchange),
        oi_to_volume=oi_to_vol,
        long_pct=long_pct,
        short_pct=short_pct,
        long_pct_before=long_before,
        short_pct_before=short_before,
        volume_24h=now.volume_24h,
        history_days=history_days,
        new_history=new_history,
        oi_volume_pctl=oi_vol_pctl,
    )


def history_cutoff(cfg: AppConfig) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=cfg.history.zscore_days)
