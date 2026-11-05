"""BingX USDT-M perpetual collector (swap v2 public API)."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from app.collectors.base import MarketSnapshot, now_utc
from app.config import NormalizationCfg
from app.core.normalize import base_from_contract, detect_multiplier, is_excluded_base

logger = logging.getLogger(__name__)

BASE = "https://open-api.bingx.com"


def _to_bingx_symbol(contract: str) -> str:
    """BTCUSDT → BTC-USDT; already dashed kept."""
    s = contract.upper().replace("_", "-")
    if "-" in s:
        return s
    for q in ("USDT", "USDC"):
        if s.endswith(q) and len(s) > len(q):
            return f"{s[: -len(q)]}-{q}"
    return s


def _from_bingx_symbol(symbol: str) -> str:
    """BTC-USDT → BTCUSDT."""
    return symbol.upper().replace("-", "").replace("_", "")


class BingxCollector:
    name = "bingx"

    def __init__(self, session: aiohttp.ClientSession, norm: NormalizationCfg):
        self.session = session
        self.norm = norm
        self._contracts: dict[str, dict] | None = None  # BTCUSDT -> meta

    async def close(self) -> None:
        return

    async def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{BASE}{path}"
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            if int(data.get("code", -1)) != 0:
                raise RuntimeError(data.get("msg", "bingx error"))
            return data.get("data")

    async def _ensure(self) -> dict[str, dict]:
        if self._contracts is None:
            rows = await self._get("/openApi/swap/v2/quote/contracts")
            items: dict[str, dict] = {}
            for row in rows or []:
                sym = row.get("symbol") or ""
                if not sym:
                    continue
                # status: 1 trading on many BingX payloads; also accept missing
                st = row.get("status")
                if st is not None and str(st) not in ("1", "TRADING", "trading", "online"):
                    continue
                internal = _from_bingx_symbol(sym)
                quote = "USDC" if internal.endswith("USDC") else "USDT"
                if not internal.endswith(("USDT", "USDC")):
                    continue
                items[internal] = {**row, "quote": quote, "bingx_symbol": sym}
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
                snap.quote = meta.get("quote", "USDT")
                out.append(snap)
        return out

    async def fetch_one(self, contract_symbol: str, base_symbol: str) -> MarketSnapshot | None:
        try:
            info = await self._ensure()
            meta = info.get(contract_symbol.upper()) or info.get(_from_bingx_symbol(contract_symbol))
            bingx_sym = (meta or {}).get("bingx_symbol") or _to_bingx_symbol(contract_symbol)
            ticker = await self._get("/openApi/swap/v2/quote/ticker", {"symbol": bingx_sym})
            if isinstance(ticker, list):
                ticker = ticker[0] if ticker else None
            if not ticker:
                return None
            premium = None
            try:
                premium = await self._get("/openApi/swap/v2/quote/premiumIndex", {"symbol": bingx_sym})
                if isinstance(premium, list):
                    premium = premium[0] if premium else None
            except Exception:  # noqa: BLE001
                premium = None
            oi_data = None
            try:
                oi_data = await self._get("/openApi/swap/v2/quote/openInterest", {"symbol": bingx_sym})
                if isinstance(oi_data, list):
                    oi_data = oi_data[0] if oi_data else None
            except Exception:  # noqa: BLE001
                oi_data = None

            last = float(ticker.get("lastPrice") or ticker.get("price") or 0) or None
            mark = float((premium or {}).get("markPrice") or ticker.get("markPrice") or 0) or None
            funding = float((premium or {}).get("lastFundingRate") or ticker.get("lastFundingRate") or 0)
            vol = float(ticker.get("quoteVolume") or ticker.get("volume") or 0) or None
            px = mark or last or 0
            mult = detect_multiplier(contract_symbol, self.norm)

            # BingX openInterest is typically notional USDT; derive qty from price.
            oi_raw = float((oi_data or {}).get("openInterest") or ticker.get("openInterest") or 0) or None
            oi_usd = oi_qty = None
            if oi_raw and px:
                # Heuristic: if value >> price*1000 treat as USD notional
                if oi_raw > px * 100:
                    oi_usd = oi_raw
                    oi_qty = (oi_raw / px) * mult
                else:
                    oi_qty = oi_raw * mult
                    oi_usd = oi_qty * px

            long_pct = short_pct = None
            try:
                # Best-effort public ratio endpoints (may be absent on some regions)
                for path, params in (
                    (
                        "/openApi/swap/v1/topLongShortAccountRatio",
                        {"symbol": bingx_sym, "period": "5m"},
                    ),
                    (
                        "/openApi/swap/v2/quote/ticker",
                        {"symbol": bingx_sym},
                    ),
                ):
                    if path.endswith("ticker"):
                        continue
                    data = await self._get(path, params)
                    row = data[0] if isinstance(data, list) and data else data
                    if not isinstance(row, dict):
                        continue
                    lr = row.get("longRatio") or row.get("longAccountRatio") or row.get("buyRatio")
                    if lr is None:
                        continue
                    lr_f = float(lr)
                    long_pct = lr_f * 100 if lr_f <= 1 else lr_f
                    short_pct = 100.0 - long_pct
                    break
            except Exception:  # noqa: BLE001
                pass

            return MarketSnapshot(
                exchange=self.name,
                contract_symbol=contract_symbol.upper().replace("-", ""),
                base_symbol=base_symbol.upper(),
                quote="USDC" if contract_symbol.upper().endswith("USDC") else "USDT",
                multiplier=mult,
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
            logger.warning("bingx fetch %s: %s", contract_symbol, e)
            return None
