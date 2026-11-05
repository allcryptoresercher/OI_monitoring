"""Price alert checker — fires once then deletes."""

from __future__ import annotations

import logging

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.pushover import send_pushover
from app.core.timeutils import ensure_utc
from app.settings import Settings
from app.storage import repo
from app.storage.db import with_db_retry
from app.storage.models import User
from app.storage.write_lock import db_write_lock

logger = logging.getLogger(__name__)


async def check_price_alerts(
    *,
    bot: Bot,
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
) -> None:
    async with session_factory() as session:
        alerts = await repo.list_all_price_alerts(session)
    if not alerts:
        return

    prices: dict[tuple[str, str], float] = {}
    async with session_factory() as session:
        for a in alerts:
            key = (a.exchange.lower(), a.base_symbol.upper())
            if key in prices:
                continue
            tick = await repo.latest_tick_for_base(session, a.exchange, a.base_symbol)
            if not tick:
                continue
            tick.ts_utc = ensure_utc(tick.ts_utc)
            px = tick.mark_price if tick.mark_price is not None else tick.last_price
            if px is not None:
                prices[key] = float(px)

    for alert in alerts:
        key = (alert.exchange.lower(), alert.base_symbol.upper())
        px = prices.get(key)
        if px is None:
            continue
        hit = False
        if alert.direction == "up" and px >= alert.target_price:
            hit = True
        elif alert.direction == "down" and px <= alert.target_price:
            hit = True
        if not hit:
            continue

        direction_ru = "Рост" if alert.direction == "up" else "Падение"
        text = (
            f"🎯 Ценовой алерт\n"
            f"{alert.base_symbol}/{alert.quote} на {alert.exchange.upper()} (фьючерс)\n"
            f"Цель: {alert.target_price} | Тип: {direction_ru}\n"
            f"Текущая цена: {px}"
        )
        alert_id = alert.id
        want_po = alert.pushover
        user_id = alert.user_id
        base = alert.base_symbol
        target = alert.target_price

        async with session_factory() as session:
            user = await session.get(User, user_id)
            telegram_id = user.telegram_id if user else None

        if telegram_id:
            try:
                await bot.send_message(telegram_id, text)
            except Exception as e:  # noqa: BLE001
                logger.error("price alert tg %s: %s", telegram_id, e)
        if want_po:
            await send_pushover(
                settings,
                title=f"{base} {direction_ru} {target}",
                message=text,
            )

        async def _del(aid=alert_id) -> None:
            async with db_write_lock():
                async with session_factory() as session:
                    await repo.delete_price_alert(session, aid)

        try:
            await with_db_retry(_del)
        except Exception as e:  # noqa: BLE001
            logger.error("price alert delete %s: %s", alert_id, e)
