"""Shared number formatting (USD and token quantity)."""

from __future__ import annotations


def fmt_compact(v: float | None, *, money: bool = False) -> str:
    if v is None:
        return "—"
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    prefix = "$" if money else ""
    n = abs_v
    if n >= 1_000_000_000:
        body = f" {n / 1_000_000_000:.2f}B"
    elif n >= 1_000_000:
        body = f"{n / 1_000_000:.2f}M"
    elif n >= 1_000:
        body = f"{n / 1_000:.1f}K"
    else:
        body = f"{n:.4g}"
    return f"{sign}{prefix}{body}"


def fmt_usd(v: float | None) -> str:
    return fmt_compact(v, money=True)


def fmt_qty(v: float | None) -> str:
    return fmt_compact(v, money=False)


def fmt_pct(v: float | None, signed: bool = True) -> str:
    if v is None:
        return "—"
    if signed:
        return f"{v:+.2f}%"
    return f"{v:.2f}%"


def fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"{v:,.2f}"
    if abs(v) >= 1:
        return f"{v:.4f}"
    return f"{v:.6g}"


def fmt_ls(long_pct: float | None, short_pct: float | None) -> str:
    if long_pct is None:
        return "—"
    short = short_pct if short_pct is not None else 100.0 - long_pct
    return f"{long_pct:.1f}/{short:.1f}"
