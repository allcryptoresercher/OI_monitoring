"""Чтение дельты за окно из базы oi_delta."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.delta.models import DeltaBucket, DeltaPair


async def spot_delta_by_exchange(
    session: AsyncSession,
    coins: set[str],
    start: datetime,
    end: datetime,
) -> tuple[dict[str, float], float]:
    """Сумма (buy_usd − sell_usd) по биржам и покрытие окна в секундах.

    Покрытие — от первого минутного бакета до последнего. Короткий хвост
    после запуска не считается полным часом или четырьмя часами.
    """
    names = {c.upper() for c in coins if c}
    if not names:
        return {}, 0.0
    stmt = (
        select(
            DeltaPair.exchange,
            func.sum(DeltaBucket.buy_usd - DeltaBucket.sell_usd),
            func.min(DeltaBucket.bucket_start),
            func.max(DeltaBucket.bucket_start),
        )
        .join(DeltaBucket, DeltaBucket.pair_id == DeltaPair.id)
        .where(DeltaPair.coin.in_(names))
        .where(DeltaBucket.bucket_start >= start)
        .where(DeltaBucket.bucket_start < end)
        .group_by(DeltaPair.exchange)
    )
    rows = (await session.execute(stmt)).all()
    totals: dict[str, float] = {}
    first = None
    last = None
    for exchange, value, bucket_min, bucket_max in rows:
        totals[str(exchange)] = float(value or 0.0)
        if bucket_min is not None and (first is None or bucket_min < first):
            first = bucket_min
        if bucket_max is not None and (last is None or bucket_max > last):
            last = bucket_max
    if first is None or last is None:
        return totals, 0.0
    span = (last - first).total_seconds() + 60.0
    return totals, max(0.0, span)
