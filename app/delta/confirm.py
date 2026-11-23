"""Подтверждение сигнала спотовой дельтой на том же окне."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class SpotDeltaConfirm:
    confirmed: bool
    leader_exchange: str | None
    leader_usd: float | None
    total_usd: float | None


def spot_delta_confirm(
    by_exchange: dict[str, float],
    price_change_pct: float | None,
) -> SpotDeltaConfirm:
    """confirmed, если знак суммарной дельты совпадает со знаком цены.

    Биржа перевеса — та, у которой модуль дельты за окно больше остальных.
    """
    if not by_exchange or all(v == 0 for v in by_exchange.values()):
        return SpotDeltaConfirm(False, None, None, None)
    total = sum(by_exchange.values())
    leader, leader_usd = max(by_exchange.items(), key=lambda item: (abs(item[1]), item[0]))
    aligned = (
        price_change_pct is not None
        and price_change_pct != 0
        and total != 0
        and (total > 0) == (price_change_pct > 0)
    )
    return SpotDeltaConfirm(aligned, leader, leader_usd, total)
