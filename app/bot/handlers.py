"""Telegram bot handlers."""

from __future__ import annotations

import asyncio
import logging
import re

import psutil
from aiogram import Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.bot.market_info import (
    build_otchet_report,
    check_add_keyboard,
    format_check_report,
    format_checkall_report,
    format_list_report,
    lookup_base_on_exchanges,
    lookup_spot_markets,
)
from app.config import ConfigStore
from app.core.normalize import normalize_user_input
from app.settings import Settings
from app.storage import repo
from app.storage.models import ExchangeHealth, WatchlistBinding

logger = logging.getLogger(__name__)

router = Router()

KNOWN_EXCHANGES = {
    "binance",
    "bybit",
    "bitget",
    "hyperliquid",
    "aster",
    "bingx",
    "gate",
    "mexc",
    "hl",
}


class Form(StatesGroup):
    waiting_check = State()
    waiting_checkall = State()
    waiting_add = State()
    waiting_otchet = State()
    waiting_alert = State()


def _is_admin(message: Message, settings: Settings) -> bool:
    return bool(message.from_user and message.from_user.id in settings.admin_id_set)


def _remove_keyboard(symbols: list[str]) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=f"✖ {sym}", callback_data=f"rm:{sym}")]
        for sym in symbols
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _normalize_exchange(raw: str) -> str | None:
    ex = raw.strip().lower()
    if ex == "hl":
        ex = "hyperliquid"
    if ex in KNOWN_EXCHANGES - {"hl"}:
        return ex
    return None


def _parse_exchange_ticker_pairs(text: str, norm_cfg) -> tuple[list[tuple[str, str]], list[str]]:
    """Parse lines / commas: 'binance, KGEN' or 'bybit GEN'."""
    pairs: list[tuple[str, str]] = []
    errors: list[str] = []
    chunks = re.split(r"[\n;]+", text.strip())
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        if "," in chunk:
            left, right = chunk.split(",", 1)
            ex_raw, tick_raw = left.strip(), right.strip()
        else:
            parts = chunk.split()
            if len(parts) < 2:
                errors.append(chunk)
                continue
            ex_raw, tick_raw = parts[0], parts[1]
        ex = _normalize_exchange(ex_raw)
        tick = normalize_user_input(tick_raw, norm_cfg)
        if not ex or not tick:
            errors.append(chunk)
            continue
        pairs.append((ex, tick))
    return pairs, errors


def setup_bot_routes(
    dp: Dispatcher,
    session_factory: async_sessionmaker[AsyncSession],
    config_store: ConfigStore,
    settings: Settings,
) -> None:
    @router.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext) -> None:
        await state.clear()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
        await message.answer(
            "OI Monitor бот запущен.\n\n"
            "Команды:\n"
            "/check — проверить фьючерсный тикер (OI, L/S)\n"
            "/checkall — спот и фьючерсы по тикеру\n"
            "/add — добавить пары биржа+тикер (разные имена)\n"
            "/otchet — сводка OI/цены по таймфреймам\n"
            "/alert — ценовой алерт (биржа тикер цена)\n"
            "/alerts — список ценовых алертов\n"
            "/remove — убрать тикер (кнопки)\n"
            "/list — список мониторинга"
        )
        logger.info("user registered telegram_id=%s db_id=%s", message.from_user.id, user.id)

    # —— /check ——
    @router.message(Command("check"))
    async def cmd_check(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.waiting_check)
        await message.answer("Введите тикер")

    @router.message(F.text.regexp(re.compile(r"^/сheck(?:@\w+)?$", re.IGNORECASE)))
    async def cmd_check_cyr(message: Message, state: FSMContext) -> None:
        await cmd_check(message, state)

    @router.message(Form.waiting_check, F.text)
    async def on_check_ticker(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("Ожидание тикера сброшено. Выберите команду заново.")
            return
        cfg = config_store.config
        base = normalize_user_input(text, cfg.normalization)
        if not base:
            await message.answer("Некорректный тикер. Введите ещё раз или /start")
            return
        await message.answer(f"Ищу {base} на биржах…")
        try:
            rows = await lookup_base_on_exchanges(base, cfg)
            kb = check_add_keyboard(base, rows)
            await message.answer(
                f"<pre>{format_check_report(base, rows)}</pre>",
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("check failed")
            await message.answer(f"Ошибка запроса: {e}")
        await state.clear()

    @router.message(Command("checkall"))
    async def cmd_checkall(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.waiting_checkall)
        await message.answer("Введите тикер")

    @router.message(Form.waiting_checkall, F.text)
    async def on_checkall_ticker(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("Ожидание тикера сброшено. Выберите команду заново.")
            return
        cfg = config_store.config
        base = normalize_user_input(text, cfg.normalization)
        if not base:
            await message.answer("Некорректный тикер. Введите ещё раз или /start")
            return
        await message.answer(f"Ищу спот и фьючерсы {base}…")
        try:
            spot_rows, fut_rows = await asyncio.gather(
                lookup_spot_markets(base),
                lookup_base_on_exchanges(base, cfg),
            )
            await message.answer(
                f"<pre>{format_checkall_report(base, spot_rows, fut_rows)}</pre>",
                parse_mode="HTML",
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("checkall failed")
            await message.answer(f"Ошибка запроса: {e}")
        await state.clear()

    @router.callback_query(F.data.startswith("chkaddall:"))
    async def on_check_add_all(query: CallbackQuery) -> None:
        base = (query.data or "").split(":", 1)[-1].upper()
        cfg = config_store.config
        rows = await lookup_base_on_exchanges(base, cfg)
        exchanges = [r.exchange for r in rows if r.available]
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, query.from_user.id, settings.admin_id_set
            )
            status = await repo.add_watchlist_all_exchanges(
                session, user.id, base, exchanges or list(cfg.exchanges)
            )
        if status == "added":
            await query.answer(f"Добавлено: {base}")
            await query.message.answer(f"Добавлено в мониторинг: {base} (все найденные биржи)")
        else:
            await query.answer("Уже в списке")
            await query.message.answer(f"{base} уже в мониторинге")

    @router.callback_query(F.data.startswith("chkadd:"))
    async def on_check_add_one(query: CallbackQuery) -> None:
        parts = (query.data or "").split(":")
        if len(parts) < 3:
            await query.answer("Ошибка")
            return
        base = parts[1].upper()
        exchange = parts[2].lower()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, query.from_user.id, settings.admin_id_set
            )
            status = await repo.add_watchlist_exchange(session, user.id, base, exchange)
        if status == "added_exchange":
            await query.answer("Добавлено")
            await query.message.answer(
                f"Добавлено: {base} на {exchange.capitalize()}"
            )
        elif status == "already_all":
            await query.answer("Уже на всех биржах")
            await query.message.answer(f"{base} уже отслеживается на всех биржах")
        else:
            await query.answer("Уже есть")
            await query.message.answer(
                f"{base} уже отслеживается на {exchange.capitalize()}"
            )

    # —— /add ——
    @router.message(Command("add"))
    async def cmd_add(message: Message, state: FSMContext, command: CommandObject) -> None:
        cfg = config_store.config
        if command.args:
            pairs, errors = _parse_exchange_ticker_pairs(command.args, cfg.normalization)
            if pairs and not errors:
                await _do_add_pairs(message, pairs)
                await state.clear()
                return
            # совместимость: /add BTC
            base = normalize_user_input(command.args.split()[0], cfg.normalization)
            if base and _normalize_exchange(command.args.split()[0]) is None:
                async with session_factory() as session:
                    user = await repo.get_or_create_user(
                        session, message.from_user.id, settings.admin_id_set
                    )
                    added = await repo.add_watchlist(session, user.id, base)
                await message.answer(
                    f"Добавлено в мониторинг: {base}"
                    if added
                    else f"{base} уже в списке"
                )
                await state.clear()
                return
        await state.set_state(Form.waiting_add)
        await message.answer(
            "Введите пары бирж и тикеров\n"
            "Пример (одним сообщением):\n"
            "binance, KGEN\n"
            "bybit, GEN\n\n"
            "OI по таким парам считается как по одной монете."
        )

    @router.message(Form.waiting_add, F.text)
    async def on_add_pairs(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("Ожидание сброшено. Выберите команду заново.")
            return
        cfg = config_store.config
        pairs, errors = _parse_exchange_ticker_pairs(text, cfg.normalization)
        if not pairs:
            await message.answer(
                "Не удалось разобрать пары. Формат: <биржа>, <тикер>\n"
                "Пример:\nbinance, BTC\nbybit, BTC"
            )
            return
        await _do_add_pairs(message, pairs, errors)
        await state.clear()

    async def _do_add_pairs(
        message: Message,
        pairs: list[tuple[str, str]],
        errors: list[str] | None = None,
    ) -> None:
        display = pairs[0][1]
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
            symbol, created = await repo.add_watchlist_with_bindings(
                session, user.id, display, pairs
            )
        lines = [
            f"{'Добавлена' if created else 'Обновлена'} группа: {symbol}",
            "Пары (OI агрегируется как одна монета):",
        ]
        for ex, tick in pairs:
            lines.append(f"  {ex.capitalize()}, {tick}")
        if errors:
            lines.append("Не распознано: " + "; ".join(errors))
        await message.answer("\n".join(lines))

    # —— /otchet ——
    @router.message(Command("otchet"))
    async def cmd_otchet(message: Message, state: FSMContext) -> None:
        await state.set_state(Form.waiting_otchet)
        await message.answer("Введите тикер для отчёта")

    @router.message(Command("отчёт"))
    async def cmd_otchet_ru(message: Message, state: FSMContext) -> None:
        await cmd_otchet(message, state)

    @router.message(Form.waiting_otchet, F.text)
    async def on_otchet_ticker(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("Ожидание тикера сброшено. Выберите команду заново.")
            return
        cfg = config_store.config
        base = normalize_user_input(text, cfg.normalization)
        if not base:
            await message.answer("Некорректный тикер. Введите ещё раз.")
            return
        await message.answer(f"Собираю отчёт по {base}…")
        try:
            async with session_factory() as session:
                user = await repo.get_or_create_user(
                    session, message.from_user.id, settings.admin_id_set
                )
                report = await build_otchet_report(session, user.id, base)
            await message.answer(f"<pre>{report}</pre>", parse_mode="HTML")
        except Exception as e:  # noqa: BLE001
            logger.exception("otchet failed")
            await message.answer(f"Ошибка отчёта: {e}")
        await state.clear()

    def _alerts_keyboard(alerts) -> InlineKeyboardMarkup:
        rows = []
        for a in alerts:
            mark = "🔔 ON" if a.pushover else "🔕 OFF"
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"{a.base_symbol} {a.exchange} Pushover: {mark}",
                        callback_data=f"po:{a.id}",
                    )
                ]
            )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def _format_alerts_list(alerts) -> str:
        if not alerts:
            return "Твои ценовые алерты:\n\n(пусто)\n\nСоздать: /alert"
        lines = ["Твои ценовые алерты:", ""]
        for a in alerts:
            direction_ru = "Рост" if a.direction == "up" else "Падение"
            po = "🔔 ON" if a.pushover else "🔕 OFF"
            lines.append(f"• {a.base_symbol}/{a.quote} на {a.exchange.upper()} (фьючерс)")
            lines.append(f"  Цель: {a.target_price} | Тип: {direction_ru}")
            lines.append(f"  Pushover: {po}")
            lines.append("")
        return "\n".join(lines).rstrip()

    # —— /alert ——
    @router.message(Command("alert"))
    async def cmd_alert(message: Message, state: FSMContext, command: CommandObject) -> None:
        if command.args:
            ok = await _create_alert_from_text(message, command.args.strip())
            if ok:
                await state.clear()
            return
        await state.set_state(Form.waiting_alert)
        await message.answer(
            "Введите: <биржа> <тикер> <цена>\n"
            "Пример: binance ELSA 1.29\n"
            "или: bingx HYPE 77.5"
        )

    @router.message(Form.waiting_alert, F.text)
    async def on_alert_input(message: Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("Ожидание сброшено.")
            return
        ok = await _create_alert_from_text(message, text)
        if ok:
            await state.clear()

    async def _create_alert_from_text(message: Message, text: str) -> bool:
        cfg = config_store.config
        parts = text.replace(",", " ").split()
        if len(parts) < 3:
            await message.answer("Нужно: <биржа> <тикер> <цена>\nПример: binance BTC 95000")
            return False
        ex = _normalize_exchange(parts[0])
        if not ex:
            await message.answer(
                "Неизвестная биржа. Доступны: binance, bybit, bitget, aster, bingx, gate, mexc, hyperliquid"
            )
            return False
        base = normalize_user_input(parts[1], cfg.normalization)
        try:
            price = float(parts[2].replace(",", "."))
        except ValueError:
            await message.answer("Цена должна быть числом")
            return False
        if not base or price <= 0:
            await message.answer("Некорректный тикер или цена")
            return False

        # текущая цена на бирже
        rows = await lookup_base_on_exchanges(base, cfg)
        row = next((r for r in rows if r.exchange == ex and r.available), None)
        if not row or row.price is None:
            await message.answer(f"{base} не найден на {ex.capitalize()}")
            return False
        direction = "up" if price > row.price else "down"
        direction_ru = "Рост" if direction == "up" else "Падение"

        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
            alert = await repo.create_price_alert(
                session,
                user_id=user.id,
                base_symbol=base,
                exchange=ex,
                target_price=price,
                direction=direction,
                quote="USDT",
                pushover=False,
            )
            alerts = await repo.list_price_alerts(session, user.id)

        card = (
            f"• {base}/USDT на {ex.upper()} (фьючерс)\n"
            f"  Цель: {price} | Тип: {direction_ru}\n"
            f"  Сейчас: {row.price}\n"
            f"  Pushover: 🔕 OFF"
        )
        await message.answer(
            "Алерт создан:\n\n" + card,
            reply_markup=_alerts_keyboard([alert]),
        )
        if len(alerts) > 1:
            await message.answer(_format_alerts_list(alerts), reply_markup=_alerts_keyboard(alerts))
        return True

    @router.message(Command("alerts"))
    async def cmd_alerts(message: Message, state: FSMContext) -> None:
        await state.clear()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
            alerts = await repo.list_price_alerts(session, user.id)
        kb = _alerts_keyboard(alerts) if alerts else None
        await message.answer(_format_alerts_list(alerts), reply_markup=kb)

    @router.callback_query(F.data.startswith("po:"))
    async def on_pushover_toggle(query: CallbackQuery) -> None:
        try:
            alert_id = int((query.data or "").split(":")[1])
        except (IndexError, ValueError):
            await query.answer("Ошибка")
            return
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, query.from_user.id, settings.admin_id_set
            )
            alert = await repo.toggle_price_alert_pushover(session, alert_id, user.id)
            alerts = await repo.list_price_alerts(session, user.id)
        if not alert:
            await query.answer("Алерт не найден")
            return
        mark = "ON" if alert.pushover else "OFF"
        await query.answer(f"Pushover {mark}")
        try:
            await query.message.edit_text(
                _format_alerts_list(alerts),
                reply_markup=_alerts_keyboard(alerts) if alerts else None,
            )
        except Exception:  # noqa: BLE001
            await query.message.answer(
                _format_alerts_list(alerts),
                reply_markup=_alerts_keyboard(alerts) if alerts else None,
            )

    # —— /remove (кнопки) ——
    @router.message(Command("remove"))
    async def cmd_remove(message: Message, state: FSMContext) -> None:
        await state.clear()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
            items = await repo.list_watchlist(session, user.id)
        if not items:
            await message.answer("Список пуст — удалять нечего.")
            return
        await message.answer(
            "Выберите тикер для удаления из мониторинга:",
            reply_markup=_remove_keyboard(items),
        )

    @router.callback_query(F.data.startswith("rm:"))
    async def on_remove_callback(query: CallbackQuery) -> None:
        base = (query.data or "").split(":", 1)[-1].upper()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, query.from_user.id, settings.admin_id_set
            )
            removed = await repo.remove_watchlist_with_user_data(session, user.id, base)
            items = await repo.list_watchlist(session, user.id)
        if removed:
            text = f"Удалено: {base} (watchlist + ваши cooldown/delivery по тикеру)."
        else:
            text = f"{base} не найден в списке."
        if items:
            await query.message.edit_text(
                text + "\n\nВыберите следующий тикер или закройте сообщение:",
                reply_markup=_remove_keyboard(items),
            )
        else:
            await query.message.edit_text(text + "\nСписок мониторинга пуст.")
        await query.answer()

    # —— /list ——
    @router.message(Command("list"))
    async def cmd_list(message: Message, state: FSMContext) -> None:
        await state.clear()
        async with session_factory() as session:
            user = await repo.get_or_create_user(
                session, message.from_user.id, settings.admin_id_set
            )
            entries = await repo.list_watchlist_entries(session, user.id)
            bases = [e[0] for e in entries]
            exch_map = await repo.exchanges_map_for_bases(session, bases)
            for base, _ in entries:
                item = await repo.get_watchlist_item(session, user.id, base)
                if not item:
                    continue
                binds = (
                    await session.execute(
                        select(WatchlistBinding).where(
                            WatchlistBinding.watchlist_id == item.id
                        )
                    )
                ).scalars().all()
                if binds:
                    exch_map[base] = sorted(
                        {
                            f"{b.exchange.capitalize()}({b.base_symbol})"
                            for b in binds
                        }
                    )
        if not entries:
            await message.answer("Отслеживаются\n\n(пусто)\n\nДобавьте тикер: /add или /check")
            return
        await message.answer(format_list_report(entries, exch_map))

    # —— admin ——
    @router.message(Command("admin_stats"))
    async def cmd_admin_stats(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        async with session_factory() as session:
            st = await repo.stats(session)
        rss = psutil.Process().memory_info().rss / (1024 * 1024)
        await message.answer(
            "Stats:\n"
            f"users={st['users']}\n"
            f"watchlist_rows={st['watchlist_rows']}\n"
            f"unique_symbols={st['unique_symbols']}\n"
            f"raw_ticks={st['raw_ticks']}\n"
            f"signals={st['signals']}\n"
            f"rss_mb={rss:.1f}\n"
            f"config_hash={config_store.hash}"
        )

    @router.message(Command("admin_symbols"))
    async def cmd_admin_symbols(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        async with session_factory() as session:
            symbols = await repo.union_watchlist_symbols(session)
        await message.answer(
            "Active bases:\n" + (", ".join(symbols) if symbols else "(empty)")
        )

    @router.message(Command("admin_reload"))
    async def cmd_admin_reload(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        changed, msg = config_store.try_reload()
        if changed:
            async with session_factory() as session:
                await repo.save_config_meta(session, config_store.hash)
        await message.answer(msg)

    @router.message(Command("admin_status"))
    async def cmd_admin_status(message: Message) -> None:
        if not _is_admin(message, settings):
            return
        async with session_factory() as session:
            rows = (await session.execute(select(ExchangeHealth))).scalars().all()
        if not rows:
            await message.answer("No exchange health data yet")
            return
        lines = []
        for r in rows:
            lines.append(
                f"{r.exchange}: fails={r.consecutive_failures} "
                f"last_ok={r.last_success_at} err={r.last_error or '-'}"
            )
        await message.answer("Exchanges:\n" + "\n".join(lines))

    dp.include_router(router)
