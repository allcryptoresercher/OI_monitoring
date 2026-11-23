"""Рассылка confirmed-сигналов во второй бот и Pushover с паузой на монету."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from aiogram import Bot

from app.core.pushover import send_pushover
from app.settings import Settings
from app.storage import repo
from app.storage.db import with_db_retry
from app.storage.write_lock import db_write_lock
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)


def pushover_due(last: datetime | None, now: datetime, hours: float) -> bool:
    if last is None:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now - last >= timedelta(hours=hours)


async def deliver_confirmed(
    *,
    bot: Bot | None,
    chat_ids: list[int],
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    coin: str,
    text: str,
    cooldown_hours: float,
    now: datetime,
) -> None:
    """Telegram во второй бот всегда. Pushover — если по монете не было срабатывания за cooldown."""
    if bot is not None and chat_ids:
        for chat_id in chat_ids:
            try:
                await bot.send_message(chat_id, text)
            except Exception as e:  # noqa: BLE001
                logger.error("confirmed bot chat %s: %s", chat_id, e)
    elif bot is None:
        logger.debug("confirmed bot token is empty, telegram skip")

    async def _last() -> datetime | None:
        async with session_factory() as session:
            return await repo.last_confirmed_pushover(session, coin)

    last = await with_db_retry(_last)
    if not pushover_due(last, now, cooldown_hours):
        logger.info("confirmed pushover skipped for %s until cooldown ends", coin.upper())
        return

    sent = await send_pushover(settings, f"{coin.upper()} confirmed", text)
    if not sent:
        return

    async def _mark() -> None:
        async with db_write_lock():
            async with session_factory() as session:
                await repo.mark_confirmed_pushover(session, coin, now)

    await with_db_retry(_mark)
    logger.info("confirmed pushover sent for %s", coin.upper())
