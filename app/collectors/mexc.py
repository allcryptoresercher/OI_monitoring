"""MEXC USDT perpetual collector."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from app.collectors.base import MarketSnapshot, now_utc
from app.config import NormalizationCfg
from app.core.normalize import base_from_contract, detect_multiplier, is_excluded_base

logger = logging.getLogger(__name__)

BASE = "https://contract.mexc.com"


def _internal(name: str) -> str:
    return name.upper().replace("_", "").replace("-", "")


class MexcCollector:
    name = "mexc"

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
            data = await resp.json()
            if not data.get("success", True) or int(data.get("code", 0)) not in (0, 200):
                raise RuntimeError(data.get("message") or data.get("msg") or "mexc error")
            return data.get("data")

    async def _ensure(self) -> dict[str, dict]:
        if self._contracts is None:
            rows = await self._get("/api/v1/contract/detail")
            if isinstance(rows, dict):
                rows = [rows]
            items: dict[str, dict] = {}
            for row in rows or []:
                # state 0 = enabled
                if row.get("state") not in (None, 0, "0"):
                    continue
                sym = row.get("symbol") or ""
                if not sym.endswith("_USDT"):
                    continue
                items[_internal(sym)] = row
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
                out.append(snap)
        return out

    async def fetch_one(self, contract_symbol: str, base_symbol: str) -> MarketSnapshot | None:
        try:
            info = await self._ensure()
            key = _internal(contract_symbol)
            meta = info.get(key)
            mexc_sym = (meta or {}).get("symbol")
            if not mexc_sym:
                if key.endswith("USDT"):
                    mexc_sym = key[:-4] + "_USDT"
                else:
                    return None
            t = await self._get("/api/v1/contract/ticker", {"symbol": mexc_sym})
            if isinstance(t, list):
                t = t[0] if t else None
            if not t:
                return None
            last = float(t.get("lastPrice") or 0) or None
            mark = float(t.get("fairPrice") or 0) or None
            funding = float(t.get("fundingRate") or 0)
            vol = float(t.get("amount24") or 0) or None
            px = mark or last or 0
            contract_size = float((meta or {}).get("contractSize") or 1) or 1.0
            hold = float(t.get("holdVol") or 0) or None
            prefix_mult = detect_multiplier(contract_symbol, self.norm)
            oi_qty = hold * contract_size * prefix_mult if hold else None
            oi_usd = oi_qty * px if oi_qty and px else None
            return MarketSnapshot(
                exchange=self.name,
                contract_symbol=_internal(mexc_sym),
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
                long_pct=None,
                short_pct=None,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("mexc fetch %s: %s", contract_symbol, e)
            return None
