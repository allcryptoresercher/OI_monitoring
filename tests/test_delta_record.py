"""Запись кумулятивной дельты в отдельную базу, без сети."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

from app.config import AppConfig, ConfigStore
from app.delta.models import DeltaBucket, DeltaState
from app.delta.recorder import DeltaRecorder
from app.storage.db import create_engine, init_db, init_delta_db, make_session_factory
from app.storage import repo
from sqlalchemy import select, text


def test_watchlist_targets_and_delta_flush(tmp_path: Path) -> None:
    asyncio.run(_run(tmp_path))


async def _run(tmp_path: Path) -> None:
    oi_url = f"sqlite+aiosqlite:///{tmp_path / 'oi.db'}"
    delta_url = f"sqlite+aiosqlite:///{tmp_path / 'delta.db'}"
    oi_engine = create_engine(oi_url)
    delta_engine = create_engine(delta_url)
    await init_db(oi_engine)
    await init_delta_db(delta_engine)
    oi_factory = make_session_factory(oi_engine)
    delta_factory = make_session_factory(delta_engine)

    async with oi_engine.connect() as conn:
        oi_tables = set((await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))).scalars())
    async with delta_engine.connect() as conn:
        delta_tables = set(
            (await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))).scalars()
        )
    assert "signals" in oi_tables or "raw_ticks" in oi_tables
    assert "delta_buckets" in delta_tables
    assert "signals" not in delta_tables
    assert "delta_buckets" not in oi_tables

    async with oi_factory() as session:
        user = await repo.get_or_create_user(session, 1, set())
        await repo.add_watchlist(session, user.id, "BTC")
        await repo.add_watchlist_with_bindings(
            session, user.id, "GEN", [("binance", "KGEN"), ("aster", "KGEN")]
        )
        targets = await repo.watchlist_cd_targets(session, ["binance", "bybit"])

    assert ("binance", "BTC") in targets
    assert ("bybit", "BTC") in targets
    assert ("binance", "KGEN") in targets
    assert ("aster", "KGEN") not in targets
    assert ("bybit", "GEN") not in targets

    store = ConfigStore.__new__(ConfigStore)
    store._config = AppConfig()
    recorder = DeltaRecorder(oi_factory, delta_factory, store)
    pair_id = await recorder._ensure_pair("binance", "BTC", "BTC/USDT")
    ts = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    recorder.acc.on_trade(pair_id, side="buy", amount=2.0, cost=100.0, timestamp=ts)
    recorder.acc.on_trade(pair_id, side="sell", amount=0.5, cost=26.0, timestamp=ts)
    await recorder.flush_once()

    async with delta_factory() as session:
        state = await session.get(DeltaState, pair_id)
        assert state is not None
        assert state.cum_delta_base == 1.5
        assert state.cum_delta_usd == 74.0
        buckets = (await session.execute(select(DeltaBucket))).scalars().all()
        assert len(buckets) == 1
        assert buckets[0].buy_vol == 2.0
        assert buckets[0].sell_vol == 0.5
        assert buckets[0].buy_usd == 100.0
        assert buckets[0].sell_usd == 26.0

    await oi_engine.dispose()
    await delta_engine.dispose()
