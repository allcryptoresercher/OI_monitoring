"""Схема базы кумулятивной дельты. Не смешивается с таблицами сигналов OI."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DeltaBase(DeclarativeBase):
    pass


class DeltaPair(DeltaBase):
    __tablename__ = "delta_pairs"
    __table_args__ = (UniqueConstraint("coin", "exchange", name="uq_delta_coin_exchange"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    coin: Mapped[str] = mapped_column(String(64), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    symbol: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DeltaState(DeltaBase):
    """Накопленная дельта с момента начала записи пары."""

    __tablename__ = "delta_state"

    pair_id: Mapped[int] = mapped_column(
        ForeignKey("delta_pairs.id", ondelete="CASCADE"), primary_key=True
    )
    cum_delta_base: Mapped[float] = mapped_column(Float, default=0.0)
    cum_delta_usd: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DeltaBucket(DeltaBase):
    """Минутный бакет объёма покупок и продаж (как delta_buckets в spot_parser)."""

    __tablename__ = "delta_buckets"
    __table_args__ = (
        UniqueConstraint("pair_id", "bucket_start", "bucket_seconds", name="uq_delta_bucket"),
        Index("ix_delta_bucket_pair_start", "pair_id", "bucket_start"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pair_id: Mapped[int] = mapped_column(ForeignKey("delta_pairs.id", ondelete="CASCADE"), index=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    bucket_seconds: Mapped[int] = mapped_column(Integer, default=60)
    buy_vol: Mapped[float] = mapped_column(Float, default=0.0)
    sell_vol: Mapped[float] = mapped_column(Float, default=0.0)
    buy_usd: Mapped[float] = mapped_column(Float, default=0.0)
    sell_usd: Mapped[float] = mapped_column(Float, default=0.0)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)
    notional_sum: Mapped[float] = mapped_column(Float, default=0.0)
