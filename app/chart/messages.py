"""Текст точки сигнала — тот же, что уходит в Telegram."""

from __future__ import annotations

import json
from dataclasses import fields

from app.core.metrics import WindowMetrics
from app.core.signals import FiredSignal, SIGNAL_NAMES
from app.delta.confirm import SpotDeltaConfirm
from app.core.signal_manager import format_signal_message
from app.storage.models import Signal


def signal_text(row: Signal) -> str:
    payload = json.loads(row.payload_json or "{}")
    raw_metrics = payload.get("metrics") or {}
    names = {item.name for item in fields(WindowMetrics)}
    metrics = WindowMetrics(**{key: raw_metrics.get(key) for key in names})
    fired = [
        FiredSignal(
            code=item.get("code") or "",
            name=SIGNAL_NAMES.get(item.get("code") or "", item.get("code") or ""),
            window=item.get("window") or row.window,
            details=item.get("details") or {},
        )
        for item in payload.get("fired") or []
    ]
    if not fired:
        codes = json.loads(row.types_json or "[]")
        fired = [
            FiredSignal(code=code, name=SIGNAL_NAMES.get(code, code), window=row.window)
            for code in codes
        ]
    confirmed = row.s7_status == "confirmed"
    spot = SpotDeltaConfirm(True, None, None, None) if confirmed else None
    return format_signal_message(
        base=row.base_symbol,
        exchange=row.exchange,
        window=row.window,
        fired=fired,
        metrics=metrics,
        s7_status=row.s7_status,
        flags={},
        ts=row.ts_utc,
        spot_delta=spot,
    )
