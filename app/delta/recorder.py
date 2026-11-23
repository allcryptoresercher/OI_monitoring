"""Синхронизация вотчлиста OI и запись кумулятивной дельты во вторую базу."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import ConfigStore
from app.delta.accumulator import DeltaAccumulator
from app.delta.adapter import CCXT_IDS, SpotTradeAdapter
from app.delta.models import DeltaBucket, DeltaPair, DeltaState
from app.storage import repo

logger = logging.getLogger(__name__)


class DeltaRecorder:
    def __init__(
        self,
        oi_factory: async_sessionmaker[AsyncSession],
        delta_factory: async_sessionmaker[AsyncSession],
        config_store: ConfigStore,
    ) -> None:
        self.oi_factory = oi_factory
        self.delta_factory = delta_factory
        self.config_store = config_store
        cfg = config_store.config.cumulative_delta
        self.acc = DeltaAccumulator(bucket_seconds=int(cfg.bucket_seconds))
        self._adapters: dict[str, SpotTradeAdapter] = {}
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._pair_ids: dict[tuple[str, str], int] = {}
        self._missing: set[tuple[str, str]] = set()
        self._stop = asyncio.Event()

    async def start(self) -> None:
        cfg = self.config_store.config.cumulative_delta
        if not cfg.enabled:
            logger.info("Cumulative delta recording disabled in config")
            return
        flush_s = float(cfg.flush_interval_sec)
        sync_s = float(cfg.sync_interval_sec)
        logger.info("Cumulative delta recorder started flush=%ss sync=%ss", flush_s, sync_s)
        await asyncio.gather(self._sync_loop(sync_s), self._flush_loop(flush_s))

    async def _sync_loop(self, interval: float) -> None:
        while not self._stop.is_set():
            try:
                await self.sync_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CD watchlist sync failed")
            await asyncio.sleep(interval)

    async def _flush_loop(self, interval: float) -> None:
        while not self._stop.is_set():
            try:
                await self.flush_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CD flush failed")
            await asyncio.sleep(interval)

    def _enabled_exchanges(self) -> list[str]:
        cfg = self.config_store.config.cumulative_delta
        out: list[str] = []
        for name in cfg.exchanges:
            key = name.strip().lower()
            if key in CCXT_IDS and key not in out:
                out.append(key)
            elif key and key not in CCXT_IDS:
                logger.warning("CD exchange skipped, no spot ccxt adapter: %s", key)
        return out

    async def sync_once(self) -> None:
        exchanges = self._enabled_exchanges()
        async with self.oi_factory() as session:
            targets = await repo.watchlist_cd_targets(session, exchanges)
        desired = {(ex, coin) for ex, coin in targets}
        for key in list(self._tasks):
            if key not in desired:
                await self._stop_pair(key)
        for exchange, coin in sorted(desired):
            if (exchange, coin) in self._tasks:
                continue
            await self._start_pair(exchange, coin)

    async def _adapter(self, exchange: str) -> SpotTradeAdapter:
        adapter = self._adapters.get(exchange)
        if adapter is None:
            adapter = SpotTradeAdapter(exchange)
            await adapter.load_markets()
            self._adapters[exchange] = adapter
        return adapter

    async def _start_pair(self, exchange: str, coin: str) -> None:
        try:
            adapter = await self._adapter(exchange)
        except Exception:
            logger.exception("CD adapter failed %s", exchange)
            return
        symbol = adapter.resolve_symbol(coin)
        if symbol is None:
            if (exchange, coin) not in self._missing:
                logger.info("CD spot market missing %s %s", exchange, coin)
            self._missing.add((exchange, coin))
            return
        self._missing.discard((exchange, coin))
        pair_id = await self._ensure_pair(exchange, coin, symbol)
        self._pair_ids[(exchange, coin)] = pair_id
        task = asyncio.create_task(
            self._stream(exchange, coin, symbol, pair_id),
            name=f"cd-{exchange}-{coin}",
        )
        self._tasks[(exchange, coin)] = task
        logger.info("CD stream %s %s -> %s pair_id=%s", exchange, coin, symbol, pair_id)

    async def _ensure_pair(self, exchange: str, coin: str, symbol: str) -> int:
        async with self.delta_factory() as session:
            result = await session.execute(
                select(DeltaPair).where(DeltaPair.exchange == exchange, DeltaPair.coin == coin)
            )
            pair = result.scalar_one_or_none()
            if pair is None:
                pair = DeltaPair(
                    coin=coin,
                    exchange=exchange,
                    symbol=symbol,
                    created_at=datetime.now(timezone.utc),
                )
                session.add(pair)
                await session.flush()
                session.add(
                    DeltaState(
                        pair_id=pair.id,
                        cum_delta_base=0.0,
                        cum_delta_usd=0.0,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
                await session.commit()
                self.acc.restore(pair.id, 0.0, 0.0)
                return pair.id
            if pair.symbol != symbol:
                pair.symbol = symbol
            state = await session.get(DeltaState, pair.id)
            base = state.cum_delta_base if state else 0.0
            usd = state.cum_delta_usd if state else 0.0
            if state is None:
                session.add(
                    DeltaState(
                        pair_id=pair.id,
                        cum_delta_base=0.0,
                        cum_delta_usd=0.0,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            await session.commit()
            self.acc.restore(pair.id, base, usd)
            return pair.id

    async def _stream(self, exchange: str, coin: str, symbol: str, pair_id: int) -> None:
        delay = 2.0
        seen: set[str] = set()
        while not self._stop.is_set() and (exchange, coin) in self._tasks:
            try:
                adapter = await self._adapter(exchange)
                async for batch in adapter.watch_trades(symbol):
                    if self._stop.is_set() or (exchange, coin) not in self._tasks:
                        return
                    for trade in batch:
                        if trade.trade_id in seen:
                            continue
                        seen.add(trade.trade_id)
                        if len(seen) > 10000:
                            seen = set(list(seen)[-5000:])
                        self.acc.on_trade(
                            pair_id,
                            side=trade.side,
                            amount=trade.amount,
                            cost=trade.cost,
                            timestamp=trade.timestamp,
                        )
                    delay = 2.0
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("CD stream error %s %s, retry in %.0fs", exchange, coin, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 120.0)

    async def _stop_pair(self, key: tuple[str, str]) -> None:
        task = self._tasks.pop(key, None)
        self._missing.discard(key)
        pair_id = self._pair_ids.pop(key, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if pair_id is not None:
            await self.flush_once()
            self.acc.drop(pair_id)
        logger.info("CD stream stopped %s %s", key[0], key[1])

    async def flush_once(self) -> None:
        dirty = self.acc.pop_dirty()
        buckets = self.acc.pop_buckets()
        if not dirty and not buckets:
            return
        now = datetime.now(timezone.utc)
        try:
            await self._write_flush(dirty, buckets, now)
        except Exception:
            self.acc.requeue(dirty, buckets)
            raise

    async def _write_flush(self, dirty, buckets, now: datetime) -> None:
        async with self.delta_factory() as session:
            for pair_id, (base, usd) in dirty.items():
                state = await session.get(DeltaState, pair_id)
                if state is None:
                    session.add(
                        DeltaState(
                            pair_id=pair_id,
                            cum_delta_base=base,
                            cum_delta_usd=usd,
                            updated_at=now,
                        )
                    )
                else:
                    state.cum_delta_base = base
                    state.cum_delta_usd = usd
                    state.updated_at = now
            for row in buckets:
                existing = await session.execute(
                    select(DeltaBucket).where(
                        DeltaBucket.pair_id == row["pair_id"],
                        DeltaBucket.bucket_start == row["bucket_start"],
                        DeltaBucket.bucket_seconds == row["bucket_seconds"],
                    )
                )
                bucket = existing.scalar_one_or_none()
                if bucket is None:
                    session.add(DeltaBucket(**row))
                else:
                    bucket.buy_vol += row["buy_vol"]
                    bucket.sell_vol += row["sell_vol"]
                    bucket.buy_usd += row["buy_usd"]
                    bucket.sell_usd += row["sell_usd"]
                    bucket.trade_count += row["trade_count"]
                    bucket.notional_sum += row["notional_sum"]
            await session.commit()
