"""Gate USDT perpetual collector."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from app.collectors.base import MarketSnapshot, now_utc
from app.config import NormalizationCfg
from app.core.normalize import base_from_contract, detect_multiplier, is_excluded_base

logger = logging.getLogger(__name__)

BASE = "https://api.gateio.ws/api/v4"


def _internal(name: str) -> str:
    return name.upper().replace("_", "").replace("-", "")


class GateCollector:
    name = "gate"

    def __init__(self, session: aiohttp.ClientSession, norm: NormalizationCfg):
        self.session = session
        self.norm = norm
        self._contracts: dict[str, dict] | None = None

    async def close(self) -> None:
        return

    async def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{BASE}{path}"
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _ensure(self) -> dict[str, dict]:
        if self._contracts is None:
            rows = await self._get("/futures/usdt/contracts")
            items: dict[str, dict] = {}
            for row in rows or []:
                if row.get("in_delisting"):
                    continue
                name = row.get("name") or ""
                if not name.endswith("_USDT"):
                    continue
                items[_internal(name)] = row
            self._contracts = items
        return self._contracts

    async def discover(self, bases: set[str]) -> list[MarketSnapshot]:
        info = await self._ensure()
        wanted = {b.upper() for b in bases}
        out: list[MarketSnapshot] = []
        for sym, meta in info.items():
            base = base_from_contract(sym, self.norm)
            if base not in wanted or is_excluded_base(base, self.norm, self.name):
                continue
            snap = await self.fetch_one(sym, base)
            if snap:
                snap.quote = "USDT"
                out.append(snap)
        return out

    async def fetch_one(self, contract_symbol: str, base_symbol: str) -> MarketSnapshot | None:
        try:
            info = await self._ensure()
            meta = info.get(contract_symbol.upper().replace("_", ""))
            gate_name = (meta or {}).get("name") or contract_symbol
            if "_" not in gate_name and gate_name.endswith("USDT"):
                gate_name = gate_name[:-4] + "_USDT"
            tickers = await self._get("/futures/usdt/tickers", {"contract": gate_name})
            t = tickers[0] if isinstance(tickers, list) and tickers else tickers
            if not t:
                return None
            last = float(t.get("last") or 0) or None
            mark = float(t.get("mark_price") or 0) or None
            funding = float(t.get("funding_rate") or 0)
            vol = float(t.get("volume_24h_quote") or t.get("volume_24h_usd") or 0) or None
            px = mark or last or 0
            quanto = float((meta or {}).get("quanto_multiplier") or 1) or 1.0
            size = float(t.get("total_size") or 0) or None
            prefix_mult = detect_multiplier(contract_symbol, self.norm)
            oi_qty = size * quanto * prefix_mult if size else None
            oi_usd = oi_qty * px if oi_qty and px else None
            long_pct = short_pct = None
            try:
                stats = await self._get(
                    "/futures/usdt/contract_stats",
                    {"contract": gate_name, "interval": "5m", "limit": 1},
                )
                row = stats[-1] if isinstance(stats, list) and stats else None
                if row and row.get("lsr_account"):
                    ratio = float(row["lsr_account"])
                    if ratio > 0:
                        long_pct = 100.0 * ratio / (1.0 + ratio)
                        short_pct = 100.0 - long_pct
            except Exception:  # noqa: BLE001
                pass
            return MarketSnapshot(
                exchange=self.name,
                contract_symbol=_internal(gate_name),
                base_symbol=base_symbol.upper(),
                quote="USDT",
                multiplier=prefix_mult,
                ts_utc=now_utc(),
                oi_usd=oi_usd,
                oi_qty=oi_qty,
                last_price=last,
                mark_price=mark,
                funding_rate=funding,
                volume_24h=vol,
                long_pct=long_pct,
                short_pct=short_pct,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("gate fetch %s: %s", contract_symbol, e)
            return None
