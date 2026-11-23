"""Сборка маркет-дельты по свечам из минутных бакетов."""

from __future__ import annotations

from bisect import bisect_right

TF_MS = {
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


def align_market_delta(
    candle_opens_ms: list[int],
    rows: list[tuple[str, int, float]],
    tf_ms: int,
) -> dict[str, list[float | None]]:
    """Сумма (buy − sell) внутри каждой свечи, отдельно по бирже.

    Нет бакетов в свече — None, чтобы линия прерывалась, а не рисовала ноль.
    """
    opens = candle_opens_ms
    acc: dict[str, list[float]] = {}
    seen: dict[str, list[bool]] = {}
    for exchange, bucket_ms, delta in rows:
        if not opens:
            break
        idx = bisect_right(opens, bucket_ms) - 1
        if idx < 0 or bucket_ms >= opens[idx] + tf_ms:
            continue
        if exchange not in acc:
            acc[exchange] = [0.0] * len(opens)
            seen[exchange] = [False] * len(opens)
        acc[exchange][idx] += delta
        seen[exchange][idx] = True
    out: dict[str, list[float | None]] = {}
    for exchange, values in acc.items():
        flags = seen[exchange]
        out[exchange] = [value if flag else None for value, flag in zip(values, flags)]
    return out
