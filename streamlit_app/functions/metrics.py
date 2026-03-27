"""Metric-card formatting and rendering helpers."""

from __future__ import annotations

import streamlit as st
from decouple import config


def _campaign_format_number(value: float | int) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "0"

    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}"


def _campaign_format_compact_number(value: float | int, suffix: str = "") -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return f"0{suffix}"

    absolute = abs(number)
    if absolute >= 1_000_000_000_000:
        scaled = number / 1_000_000_000_000
        unit = "T"
    elif absolute >= 1_000_000_000:
        scaled = number / 1_000_000_000
        unit = "B"
    elif absolute >= 1_000_000:
        scaled = number / 1_000_000
        unit = "M"
    elif absolute >= 1_000:
        scaled = number / 1_000
        unit = "K"
    else:
        scaled = number
        unit = ""

    if not unit:
        return f"{_campaign_format_number(scaled)}{suffix}"

    integer_digits = len(str(int(abs(scaled)))) if scaled else 1
    decimal_places = max(0, 3 - integer_digits)
    compact_value = f"{scaled:.{decimal_places}f}".rstrip("0").rstrip(".")
    return f"{compact_value}{unit}{suffix}"


def _campaign_format_currency(value: float | int, compact: bool = False) -> str:
    if not compact:
        return f"Rp. {_campaign_format_number(value)}"
    return f"Rp. {_campaign_format_compact_number(value)}"


def _campaign_format_usd(value: float | int, compact: bool = False) -> str:
    if not compact:
        return f"$ {_campaign_format_number(value)}"
    return f"$ {_campaign_format_compact_number(value)}"


def _campaign_convert_idr_to_usd(value: float | int) -> float:
    rate = float(config("USD_TO_IDR_RATE", default=16968, cast=float))
    if rate == 0:
        return float(value)
    return float(value) / rate


def _campaign_format_growth(growth: float | None) -> str:
    if growth is None:
        return "N/A"
    sign = "+" if growth > 0 else ""
    return f"{sign}{growth:.2f}% from last period"


def _campaign_metric_value(metrics: dict[str, float], key: str) -> float:
    value = metrics.get(key, 0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _render_hover_metric_card(
    st_module,
    label: str,
    value: str,
    delta: str,
    growth_value: float | None = None,
    tooltip: str | None = None,
) -> None:
    delta_color = "off" if growth_value == 0 else "normal"
    st_module.metric(label=label, value=value, delta=delta, delta_color=delta_color, help=tooltip)


def _campaign_growth_from_periods(source_metrics: dict[str, object], key: str) -> float:
    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    previous_metrics = source_metrics.get("previous_period", {}).get("metrics", {})
    current_value = _campaign_metric_value(current_metrics, key)
    previous_value = _campaign_metric_value(previous_metrics, key)

    if previous_value == 0:
        return 0.0 if current_value == 0 else 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def render_campaign_metric_cards(st_module, source_metrics: dict[str, object], source_label: str) -> None:
    st_module.markdown(f'<div class="metric-section-title">{source_label} Performance</div>', unsafe_allow_html=True)

    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = source_metrics.get("growth_percentage", {})
    cards = [
        ("Cost Spend", "cost"),
        ("Impressions", "impressions"),
        ("Clicks", "clicks"),
        ("Leads", "leads"),
        ("Cost/Leads", "cost_leads"),
    ]

    for column, (label, key) in zip(st_module.columns(5, gap="small"), cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key in {"cost", "cost_leads"}:
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                    tooltip_value = _campaign_format_currency(raw_value, compact=False)
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                growth_text = _campaign_format_growth(growth_value)
                if key in {"cost", "cost_leads"}:
                    _render_hover_metric_card(
                        st_module,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
                    st_module.metric(label=label, value=metric_value, delta=growth_text)


def render_brand_awareness_metric_cards(st_module, source_metrics: dict[str, object], source_label: str) -> None:
    if source_label:
        st_module.markdown(f'<div class="metric-section-title">{source_label} - Brand Awareness</div>', unsafe_allow_html=True)
    st_module.markdown(
        """
        <style>
            div[data-testid="stMetricLabel"] > div {
                white-space: nowrap !important;
                overflow: hidden !important;
                text-overflow: ellipsis !important;
                font-size: 1.05rem !important;
            }
            div[data-testid="stMetricValue"] > div {
                font-size: 1.8rem !important;
                line-height: 1.15 !important;
            }
            div[data-testid="stMetricDelta"] > div {
                font-size: 0.84rem !important;
                white-space: normal !important;
                overflow: visible !important;
                text-overflow: unset !important;
                line-height: 1.2 !important;
                word-break: break-word !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = source_metrics.get("growth_percentage", {})
    cards = [("Spend", "cost"), ("Impr.", "impressions"), ("Clicks", "clicks"), ("CTR", "ctr"), ("CPM", "cpm"), ("CPC", "cpc")]

    for column, (label, key) in zip(st_module.columns(6, gap="small"), cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key == "cpc":
                    metric_value = f"Rp. {round(raw_value):,}"
                    tooltip_value = f"Rp. {round(raw_value):,}"
                elif key in {"cost", "cpm"}:
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                    tooltip_value = _campaign_format_currency(raw_value, compact=False)
                elif key == "ctr":
                    metric_value = f"{raw_value:.2f}%"
                    tooltip_value = None
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                growth_text = _campaign_format_growth(growth_value)
                if key in {"cost", "cpm", "cpc"}:
                    _render_hover_metric_card(
                        st_module,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
                    st_module.metric(label=label, value=metric_value, delta=growth_text)


def render_overview_metric_cards(st_module, summary_payload: dict[str, object]) -> None:
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    cards = [("Last Day Stickiness", "last_day_stickiness"), ("Average Stickiness", "average_stickiness"), ("Active User", "active_user")]

    for column, (label, key) in zip(st_module.columns(3, gap="small"), cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                metric_value = _campaign_format_number(raw_value) if key == "active_user" else f"{raw_value:.2f}%"
                growth_value = growth_metrics.get(key, 0.0)
                st_module.metric(label=label, value=metric_value, delta=_campaign_format_growth(growth_value))


def render_overview_cost_metric_cards(st_module, summary_payload: dict[str, object]) -> None:
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    cards = [
        ("Total Ad Cost", "total_ad_cost"),
        ("Google Ad Cost", "google_ad_cost"),
        ("Facebook Ad Cost", "facebook_ad_cost"),
        ("Tiktok Ad Cost", "tiktok_ad_cost"),
    ]

    for column, (label, key) in zip(st_module.columns(4, gap="small"), cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                _render_hover_metric_card(
                    st_module,
                    label=label,
                    value=_campaign_format_currency(raw_value, compact=True),
                    delta=_campaign_format_growth(growth_metrics.get(key, 0.0)),
                    growth_value=growth_metrics.get(key, 0.0),
                    tooltip=_campaign_format_currency(raw_value, compact=False),
                )


def render_overview_leads_metric_cards(st_module, summary_payload: dict[str, object], currency_unit: str = "IDR") -> None:
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    primary_cards = [("Cost", "cost"), ("Impressions", "impressions"), ("Clicks", "clicks"), ("Leads", "leads"), ("Cost/Leads", "cost_leads")]
    secondary_cards = [("First Deposit", "first_deposit"), ("Cost to First Deposit", "cost_to_first_deposit")]

    for column, (label, key) in zip(st_module.columns(5, gap="small"), primary_cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key in {"cost", "cost_leads"}:
                    if currency_unit == "USD":
                        converted_value = _campaign_convert_idr_to_usd(raw_value)
                        metric_value = _campaign_format_usd(converted_value, compact=True)
                        tooltip_value = _campaign_format_usd(converted_value, compact=False)
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                        tooltip_value = _campaign_format_currency(raw_value, compact=False)
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key, 0.0)
                if key in {"cost", "cost_leads"}:
                    _render_hover_metric_card(
                        st_module,
                        label=label,
                        value=metric_value,
                        delta=_campaign_format_growth(growth_value),
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
                    st_module.metric(label=label, value=metric_value, delta=_campaign_format_growth(growth_value))

    for column, (label, key) in zip(st_module.columns(2, gap="small"), secondary_cards):
        with column:
            with st_module.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key == "first_deposit":
                    if currency_unit == "USD":
                        converted_value = _campaign_convert_idr_to_usd(raw_value)
                        metric_value = _campaign_format_usd(converted_value, compact=True)
                        tooltip_value = _campaign_format_usd(converted_value, compact=False)
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                        tooltip_value = _campaign_format_currency(raw_value, compact=False)
                    _render_hover_metric_card(
                        st_module,
                        label=label,
                        value=metric_value,
                        delta=_campaign_format_growth(growth_metrics.get(key, 0.0)),
                        growth_value=growth_metrics.get(key, 0.0),
                        tooltip=tooltip_value,
                    )
                else:
                    st_module.metric(
                        label=label,
                        value=f"{raw_value:.2f}%",
                        delta=_campaign_format_growth(growth_metrics.get(key, 0.0)),
                    )
