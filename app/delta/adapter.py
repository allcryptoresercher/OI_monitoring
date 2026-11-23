"""Тонкая обёртка ccxt.pro: спотовые сделки, как в spot_parser."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

logger = logging.getLogger(__name__)

Side = Literal["buy", "sell"]

CCXT_IDS: dict[str, str] = {
    "binance": "binance",
    "bybit": "bybit",
    "bitget": "bitget",
    "gate": "gate",
    "mexc": "mexc",
    "bingx": "bingx",
    "hyperliquid": "hyperliquid",
}

QUOTE_PREFERENCE: dict[str, list[str]] = {
    "hyperliquid": ["USDC", "USDT", "USD"],
}
DEFAULT_QUOTES = ["USDT", "USDC", "USD"]


@dataclass(slots=True)
class SpotTrade:
    trade_id: str
    price: float
    amount: float
    cost: float
    side: Side
    timestamp: datetime


def quotes_for(exchange: str) -> list[str]:
    return QUOTE_PREFERENCE.get(exchange.lower(), DEFAULT_QUOTES)


def _ms_to_dt(ms: int | float | None) -> datetime:
    if ms is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)


def normalize_side(trade: dict[str, Any]) -> Side:
    side = trade.get("side")
    if side in ("buy", "sell"):
        return side  # type: ignore[return-value]
    info = trade.get("info") or {}
    if "m" in info:
        return "sell" if info["m"] else "buy"
    if "isBuyerMaker" in info:
        return "sell" if info["isBuyerMaker"] else "buy"
    return "buy"


class SpotTradeAdapter:
    def __init__(self, name: str) -> None:
        if name not in CCXT_IDS:
            raise ValueError(f"Биржа без спотового стрима ccxt: {name}")
        import ccxt.pro as ccxtpro

        self.name = name
        exchange_cls = getattr(ccxtpro, CCXT_IDS[name])
        self._exchange = exchange_cls(
            {"enableRateLimit": True, "options": {"defaultType": "spot"}}
        )
        self._markets_loaded = False

    async def load_markets(self) -> None:
        if self._markets_loaded:
            return
        await self._exchange.load_markets()
        self._markets_loaded = True
        logger.info("CD markets loaded %s (%s)", self.name, len(self._exchange.markets or {}))

    def resolve_symbol(self, coin: str) -> str | None:
        coin = coin.upper().strip()
        candidates = [coin] if "/" in coin else [f"{coin}/{q}" for q in quotes_for(self.name)]
        if not self._markets_loaded:
            return candidates[0]
        markets = self._exchange.markets or {}
        for symbol in candidates:
            market = markets.get(symbol)
            if not market or market.get("active") is False:
                continue
            if market.get("spot") is True or market.get("type") == "spot":
                return symbol
            if market.get("swap") is not True and market.get("type") not in ("swap", "future", "option"):
                return symbol
        return None

    def _to_trade(self, trade: dict[str, Any]) -> SpotTrade:
        price = float(trade["price"])
        amount = float(trade["amount"])
        cost = float(trade["cost"]) if trade.get("cost") is not None else price * amount
        trade_id = str(trade.get("id") or f"{trade.get('timestamp')}-{price}-{amount}")
        return SpotTrade(
            trade_id=trade_id,
            price=price,
            amount=amount,
            cost=cost,
            side=normalize_side(trade),
            timestamp=_ms_to_dt(trade.get("timestamp")),
        )

    async def watch_trades(self, symbol: str) -> AsyncIterator[list[SpotTrade]]:
        await self.load_markets()
        market = (self._exchange.markets or {}).get(symbol)
        if not market or market.get("active") is False:
            raise ValueError(f"нет спотового рынка {symbol} на {self.name}")
        while True:
            raw = await self._exchange.watch_trades(symbol)
            yield [self._to_trade(item) for item in raw]

    async def close(self) -> None:
        close = getattr(self._exchange, "close", None)
        if close:
            await close()
