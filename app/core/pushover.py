"""Pushover notifications."""

from __future__ import annotations

import logging

import aiohttp

from app.settings import Settings

logger = logging.getLogger(__name__)


async def send_pushover(settings: Settings, title: str, message: str) -> bool:
    token = (settings.pushover_token or "").strip()
    user = (settings.pushover_user or "").strip()
    if not token or not user:
        logger.warning("pushover skipped: missing PUSHOVER_TOKEN / PUSHOVER_USER")
        return False
    payload = {"token": token, "user": user, "title": title[:250], "message": message[:1024]}
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post("https://api.pushover.net/1/messages.json", data=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("pushover http %s: %s", resp.status, body[:200])
                    return False
                return True
    except Exception as e:  # noqa: BLE001
        logger.warning("pushover send failed: %s", e)
        return False
