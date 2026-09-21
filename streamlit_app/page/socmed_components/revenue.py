"""Shared revenue cards for social media pages."""

from datetime import date

import streamlit as st

from streamlit_app.functions.metrics import _render_metric_with_growth, _campaign_format_growth


def revenue_comparison_for_period(period: str | None) -> str:
    return {"This Month": "month_to_date", "Last Month": "previous_month"}.get(period, "previous_period")


def render_revenue(metrics: dict | None) -> None:
    st.markdown("## Revenue")
    if metrics is None:
        st.info("Revenue data is unavailable. Please reload the page.")
        return
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    previous = metrics.get("previous_period", {})
    comparison_label = "from last period"
    if previous.get("start_date") and previous.get("end_date"):
        start = date.fromisoformat(previous["start_date"])
        end = date.fromisoformat(previous["end_date"])
        comparison_label = f"vs {start:%d %b %Y} – {end:%d %b %Y}"
    specs = [
        ("Register", "register", f'{current.get("register", 0):,}'),
        ("First Deposit Qty", "first_deposit_qty", f'{current.get("first_deposit_qty", 0):,}'),
        ("First Deposit", "first_deposit", f'$ {current.get("first_deposit", 0):,.2f}'),
        ("Register to Deposit %", "register_to_deposit", f'{current.get("register_to_deposit", 0):.2f}%'),
    ]
    for column, (label, key, value) in zip(st.columns(4, gap="small"), specs):
        with column:
            with st.container(border=True):
                change = growth.get(key, 0.0)
                _render_metric_with_growth(st, label, value, delta=_campaign_format_growth(change).replace("from last period", comparison_label),
                          delta_color="off" if change == 0 else "normal")
