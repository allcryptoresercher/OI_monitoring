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
) -> dict[str, float]:
    """Сумма (buy_usd − sell_usd) по биржам за [start, end)."""
    names = {c.upper() for c in coins if c}
    if not names:
        return {}
    stmt = (
        select(
            DeltaPair.exchange,
            func.sum(DeltaBucket.buy_usd - DeltaBucket.sell_usd),
        )
        .join(DeltaBucket, DeltaBucket.pair_id == DeltaPair.id)
        .where(DeltaPair.coin.in_(names))
        .where(DeltaBucket.bucket_start >= start)
        .where(DeltaBucket.bucket_start < end)
        .group_by(DeltaPair.exchange)
    )
    rows = (await session.execute(stmt)).all()
    return {str(exchange): float(value or 0.0) for exchange, value in rows}
