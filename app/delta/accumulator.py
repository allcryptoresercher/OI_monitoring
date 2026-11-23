"""Накопление кумулятивной дельты по сделкам.

Схема бакета совпадает с spot_parser: покупка увеличивает дельту, продажа уменьшает.
Пишется всегда, без порога алерта.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class RunningDelta:
    cum_delta_base: float = 0.0
    cum_delta_usd: float = 0.0
    dirty: bool = False


@dataclass
class DeltaAccumulator:
    bucket_seconds: int = 60
    _state: dict[int, RunningDelta] = field(default_factory=dict)
    _buckets: dict[int, dict[int, dict[str, float]]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    def restore(self, pair_id: int, cum_base: float, cum_usd: float) -> None:
        self._state[pair_id] = RunningDelta(
            cum_delta_base=cum_base, cum_delta_usd=cum_usd, dirty=False
        )

    def requeue(self, dirty: dict[int, tuple[float, float]], buckets: list[dict[str, Any]]) -> None:
        for pair_id, (base, usd) in dirty.items():
            rt = self._state.setdefault(pair_id, RunningDelta())
            rt.cum_delta_base = base
            rt.cum_delta_usd = usd
            rt.dirty = True
        for row in buckets:
            start = int(row["bucket_start"].timestamp())
            acc = self._buckets[row["pair_id"]].setdefault(
                start,
                {
                    "buy_vol": 0.0,
                    "sell_vol": 0.0,
                    "buy_usd": 0.0,
                    "sell_usd": 0.0,
                    "trade_count": 0.0,
                    "notional_sum": 0.0,
                },
            )
            acc["buy_vol"] += row["buy_vol"]
            acc["sell_vol"] += row["sell_vol"]
            acc["buy_usd"] += row["buy_usd"]
            acc["sell_usd"] += row["sell_usd"]
            acc["trade_count"] += row["trade_count"]
            acc["notional_sum"] += row["notional_sum"]

    def drop(self, pair_id: int) -> None:
        self._state.pop(pair_id, None)
        self._buckets.pop(pair_id, None)

    def on_trade(
        self,
        pair_id: int,
        *,
        side: str,
        amount: float,
        cost: float,
        timestamp: datetime,
    ) -> None:
        rt = self._state.setdefault(pair_id, RunningDelta())
        signed = 1.0 if side == "buy" else -1.0
        rt.cum_delta_base += signed * amount
        rt.cum_delta_usd += signed * cost
        rt.dirty = True

        ts = timestamp.timestamp() if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc).timestamp()
        start = int(ts // self.bucket_seconds) * self.bucket_seconds
        acc = self._buckets[pair_id].setdefault(
            start,
            {
                "buy_vol": 0.0,
                "sell_vol": 0.0,
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "trade_count": 0.0,
                "notional_sum": 0.0,
            },
        )
        if side == "buy":
            acc["buy_vol"] += amount
            acc["buy_usd"] += cost
        else:
            acc["sell_vol"] += amount
            acc["sell_usd"] += cost
        acc["trade_count"] += 1
        acc["notional_sum"] += abs(cost)

    def pop_dirty(self) -> dict[int, tuple[float, float]]:
        out: dict[int, tuple[float, float]] = {}
        for pair_id, rt in self._state.items():
            if rt.dirty:
                out[pair_id] = (rt.cum_delta_base, rt.cum_delta_usd)
                rt.dirty = False
        return out

    def pop_buckets(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for pair_id, buckets in list(self._buckets.items()):
            for start_ts, acc in buckets.items():
                rows.append(
                    {
                        "pair_id": pair_id,
                        "bucket_start": datetime.fromtimestamp(start_ts, tz=timezone.utc),
                        "bucket_seconds": self.bucket_seconds,
                        "buy_vol": acc["buy_vol"],
                        "sell_vol": acc["sell_vol"],
                        "buy_usd": acc["buy_usd"],
                        "sell_usd": acc["sell_usd"],
                        "trade_count": int(acc["trade_count"]),
                        "notional_sum": acc["notional_sum"],
                    }
                )
            self._buckets[pair_id] = {}
        return rows
