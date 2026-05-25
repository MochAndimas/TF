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
    if not unit:
        compact = f"{scaled:,.0f}"
    else:
        abs_scaled = abs(scaled)
        if abs_scaled >= 100:
            decimals = 0
        elif abs_scaled >= 10:
            decimals = 1
        else:
            decimals = 2
        compact_base = f"{scaled:.{decimals}f}"
        if "." in compact_base:
            compact_base = compact_base.rstrip("0").rstrip(".")
        compact = compact_base + unit
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

