"""Shared formatting helpers for reports."""

from __future__ import annotations

from typing import Any


def fmt_money(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{int(value):,}".replace(",", " ")
    except Exception:
        return str(value)


def fmt_num(value: Any, digits: int = 0) -> str:
    if value is None:
        return "—"
    try:
        number = float(value)
        if digits == 0:
            return f"{int(round(number)):,}".replace(",", " ")
        return f"{number:.{digits}f}"
    except Exception:
        return str(value)
