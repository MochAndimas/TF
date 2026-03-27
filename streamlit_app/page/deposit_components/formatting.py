"""Formatting helpers for the deposit page."""

from __future__ import annotations

from decouple import config

USD_TO_IDR_RATE = config("USD_TO_IDR_RATE", default=16968, cast=float)


def currency_multiplier(currency_unit: str) -> float:
    return float(USD_TO_IDR_RATE) if currency_unit == "IDR" else 1.0


def currency_label(currency_unit: str) -> str:
    return "Rp" if currency_unit == "IDR" else "$"


def compact_currency_value(value: float, currency_unit: str) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000_000:
        scaled, unit = value / 1_000_000_000_000, "T"
    elif absolute >= 1_000_000_000:
        scaled, unit = value / 1_000_000_000, "B"
    elif absolute >= 1_000_000:
        scaled, unit = value / 1_000_000, "M"
    elif absolute >= 1_000:
        scaled, unit = value / 1_000, "K"
    else:
        scaled, unit = value, ""
    compact = f"{scaled:,.0f}" if not unit else f"{scaled:.{max(0, 3 - len(str(int(abs(scaled))) if scaled else '1'))}f}".rstrip("0").rstrip(".") + unit
    return f"{currency_label(currency_unit)}{compact}"


def format_amount(value: float | int, currency_unit: str = "USD") -> str:
    return compact_currency_value(float(value) * currency_multiplier(currency_unit), currency_unit)


def format_qty(value: float | int) -> str:
    return f"{int(float(value)):,}"


def format_aov(value: float | int, currency_unit: str = "USD") -> str:
    return format_amount(value, currency_unit=currency_unit)


def format_amount_full(value: float | int, currency_unit: str = "USD") -> str:
    converted_value = float(value) * currency_multiplier(currency_unit)
    if currency_unit == "IDR":
        return f"Rp{converted_value:,.0f}"
    return f"${converted_value:,.2f}"


def metric_formatter(metric_key: str, value: float | int, currency_unit: str = "USD") -> str:
    if metric_key == "depo_amount":
        return format_amount(value, currency_unit=currency_unit)
    if metric_key == "qty":
        return format_qty(value)
    return format_aov(value, currency_unit=currency_unit)
