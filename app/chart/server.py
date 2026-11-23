"""Страница графика: свечи, линия маркет-дельты, точки сигналов."""

from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web

from app.chart.data import (
    chart_window,
    coin_names,
    fetch_klines,
    list_coins,
    load_drawings,
    load_signals,
    market_delta_by_exchange,
    save_drawings,
)
from app.chart.align import TF_MS
from app.settings import get_settings
from app.storage.db import create_engine, init_db, make_session_factory

logger = logging.getLogger(__name__)
STATIC = Path(__file__).resolve().parent / "static"


def create_app(oi_factory, delta_factory) -> web.Application:
    app = web.Application(middlewares=[_auth])
    app["oi_factory"] = oi_factory
    app["delta_factory"] = delta_factory
    app["token"] = (get_settings().chart_token or "").strip()
    app.router.add_get("/", _page)
    app.router.add_get("/api/coins", _coins)
    app.router.add_get("/api/chart", _chart)
    app.router.add_get("/api/drawings", _drawings_get)
    app.router.add_put("/api/drawings", _drawings_put)
    return app


@web.middleware
async def _auth(request: web.Request, handler):
    token = request.app["token"]
    if token and request.path.startswith("/api"):
        got = request.headers.get("X-Chart-Token") or request.query.get("token") or ""
        if got != token:
            return web.json_response({"error": "unauthorized"}, status=401)
    return await handler(request)


async def _page(_request: web.Request) -> web.Response:
    return web.FileResponse(STATIC / "index.html")


async def _coins(request: web.Request) -> web.Response:
    async with request.app["oi_factory"]() as session:
        coins = await list_coins(session)
    return web.json_response({"coins": coins})


async def _chart(request: web.Request) -> web.Response:
    coin = (request.query.get("coin") or "").upper().strip()
    tf = request.query.get("tf") or "15m"
    try:
        hours = max(1, min(24 * 30, int(request.query.get("hours") or "48")))
    except ValueError:
        hours = 48
    if not coin or tf not in TF_MS:
        return web.json_response({"error": "нужны coin и tf: 5m, 15m, 1h, 4h, 1d"}, status=400)

    candles, source = await fetch_klines(coin, tf, hours)
    if not candles:
        return web.json_response({"error": f"нет спотовых свечей {coin}USDT на Binance и Bybit"}, status=404)

    start, end = chart_window(hours)
    async with request.app["oi_factory"]() as session:
        names = await coin_names(session, coin)
        signals = await load_signals(session, names, start, end)
    delta: dict = {}
    delta_factory = request.app["delta_factory"]
    if delta_factory is not None:
        try:
            async with delta_factory() as session:
                delta = await market_delta_by_exchange(
                    session, names, [item["timestamp"] for item in candles], tf
                )
        except Exception:
            logger.exception("market delta read failed")
    return web.json_response(
        {
            "coin": coin,
            "tf": tf,
            "source": source,
            "candles": candles,
            "delta": delta,
            "signals": signals,
        }
    )


async def _drawings_get(request: web.Request) -> web.Response:
    coin = (request.query.get("coin") or "").upper().strip()
    tf = request.query.get("tf") or "15m"
    if not coin or tf not in TF_MS:
        return web.json_response({"error": "нужны coin и tf"}, status=400)
    async with request.app["oi_factory"]() as session:
        items = await load_drawings(session, coin, tf)
    return web.json_response({"items": items})


async def _drawings_put(request: web.Request) -> web.Response:
    coin = (request.query.get("coin") or "").upper().strip()
    tf = request.query.get("tf") or "15m"
    if not coin or tf not in TF_MS:
        return web.json_response({"error": "нужны coin и tf"}, status=400)
    body = await request.json()
    items = body.get("items") if isinstance(body, dict) else None
    if not isinstance(items, list):
        return web.json_response({"error": "items должен быть списком"}, status=400)
    async with request.app["oi_factory"]() as session:
        await save_drawings(session, coin, tf, items)
    return web.json_response({"ok": True})


async def start() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    oi_engine = create_engine(settings.database_url)
    await init_db(oi_engine)
    oi_factory = make_session_factory(oi_engine)
    delta_factory = None
    delta_url = (settings.delta_database_url or "").strip()
    if delta_url:
        delta_engine = create_engine(delta_url)
        delta_factory = make_session_factory(delta_engine)
    app = create_app(oi_factory, delta_factory)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, settings.chart_host, settings.chart_port)
    await site.start()
    logger.info("Chart http://%s:%s", settings.chart_host, settings.chart_port)
    import asyncio

    await asyncio.Event().wait()


def main() -> None:
    import asyncio

    asyncio.run(start())


if __name__ == "__main__":
    main()
