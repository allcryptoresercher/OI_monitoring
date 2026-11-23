#!/usr/bin/env python3
"""OI Telegram monitoring bot — entrypoint."""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand

# ensure project root on path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.bot.handlers import setup_bot_routes
from app.config import ConfigStore
from app.settings import get_settings
from app.delta.recorder import DeltaRecorder
from app.storage.db import create_engine, init_db, init_delta_db, make_session_factory
from app.storage import repo
from app.workers.collector_loop import Pipeline
from app.workers.config_watcher import ConfigWatcher
from app.workers.post_factum import PostFactumWorker


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


async def amain() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    config_path = Path(settings.config_path)
    if not config_path.exists():
        example = ROOT / "config.example.yaml"
        if example.exists():
            shutil.copy(example, config_path)
        else:
            raise SystemExit(f"Missing config: {config_path}")

    config_store = ConfigStore(config_path)
    config_store.load(force=True)

    engine = create_engine(settings.database_url)
    await init_db(engine)
    session_factory = make_session_factory(engine)

    async with session_factory() as session:
        await repo.save_config_meta(session, config_store.hash)

    bot = Bot(token=settings.bot_token)
    await bot.set_my_commands(
        [
            BotCommand(command="check", description="Проверить тикер (OI, L/S)"),
            BotCommand(command="add", description="Добавить пары биржа+тикер"),
            BotCommand(command="otchet", description="Сводка по таймфреймам"),
            BotCommand(command="alert", description="Создать ценовой алерт"),
            BotCommand(command="alerts", description="Список ценовых алертов"),
            BotCommand(command="list", description="Список мониторинга"),
            BotCommand(command="remove", description="Удалить из мониторинга"),
        ]
    )
    dp = Dispatcher(storage=MemoryStorage())
    setup_bot_routes(dp, session_factory, config_store, settings)

    pipeline = Pipeline(bot, session_factory, config_store)
    confirmed_token = (settings.confirmed_bot_token or "").strip()
    if confirmed_token:
        pipeline.confirmed_bot = Bot(token=confirmed_token)
        pipeline.confirmed_chat_ids = settings.confirmed_chat_id_list
        logging.getLogger(__name__).info(
            "Confirmed bot enabled, chats=%s", pipeline.confirmed_chat_ids
        )
        if not pipeline.confirmed_chat_ids:
            logging.getLogger(__name__).warning(
                "CONFIRMED_CHAT_IDS is empty: confirmed Telegram messages have no destination"
            )
    else:
        logging.getLogger(__name__).warning(
            "CONFIRMED_BOT_TOKEN is empty: confirmed channel is off"
        )
    post_factum = PostFactumWorker(session_factory, config_store)
    watcher = ConfigWatcher(config_store, session_factory)

    tasks = [
        dp.start_polling(bot),
        pipeline.start(),
        post_factum.start(),
        watcher.start(),
    ]
    delta_url = (settings.delta_database_url or "").strip()
    if delta_url:
        delta_engine = create_engine(delta_url)
        await init_delta_db(delta_engine)
        delta_factory = make_session_factory(delta_engine)
        recorder = DeltaRecorder(session_factory, delta_factory, config_store)
        pipeline.delta_factory = delta_factory
        tasks.append(recorder.start())
        logging.getLogger(__name__).info(
            "Delta DB connected (%s)", delta_engine.url.render_as_string(hide_password=True)
        )
    else:
        logging.getLogger(__name__).warning(
            "DELTA_DATABASE_URL is empty: cumulative delta is not recorded"
        )

    logging.getLogger(__name__).info(
        "Starting OI bot config_hash=%s mode=%s",
        config_store.hash,
        config_store.config.monitoring_mode,
    )

    await asyncio.gather(*tasks)


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
