"""Streamlit page for the first-deposit report."""

import datetime as dt

import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.page.deposit_components.charts import (
    build_daily_deposit_amount_figure,
    build_daily_deposit_qty_aov_figure,
    build_top_campaign_deposit_figure,
)
from streamlit_app.page.deposit_components.rendering import render_metric_cards, render_report_table

PAGE_STYLE = """
<style>
.deposit-title {
    text-align: center;
    font-size: 2.8rem;
    font-weight: 800;
    margin-bottom: 1rem;
}
.deposit-table-wrap {
    width: 100%;
    overflow-x: auto;
    border: 1px solid rgba(120, 140, 170, 0.32);
    border-radius: 10px;
}
.deposit-table {
    border-collapse: collapse;
    width: max-content;
    min-width: 100%;
    font-size: 0.96rem;
}
.deposit-table th,
.deposit-table td {
    border: 1px solid rgba(120, 140, 170, 0.28);
    padding: 7px 10px;
    white-space: nowrap;
}
.deposit-table thead th {
    background: rgba(43, 63, 92, 0.32);
    text-align: center;
}
.sticky-col {
    position: sticky;
    left: 0;
    z-index: 2;
    background: rgb(15, 23, 42);
}
.deposit-table tbody td.sticky-col {
    background: #e8eefc;
    color: #22304a;
}
.deposit-table thead .sticky-col {
    background: rgba(43, 63, 92, 0.32);
    color: #2f3747;
}
.section-head td {
    background: rgba(37, 99, 235, 0.25);
    font-weight: 700;
    border-top: 2px solid rgba(96, 165, 250, 0.9);
}
.metric-name {
    font-weight: 600;
    color: #22304a;
}
.metric-section-title {
    text-align: center;
    font-size: 2.2rem;
    font-weight: 800;
    margin: 1.2rem 0 0.8rem 0;
}
.deposit-group-title {
    text-align: center;
    font-size: 1.9rem;
    font-weight: 700;
    margin: 1.0rem 0 0.5rem 0;
}
.currency-inline-label {
    font-size: 0.98rem;
    font-weight: 600;
    padding-top: 0.2rem;
    text-align: right;
}
div[data-testid="stMetricLabel"] > div {
    font-size: 1.05rem !important;
}
div[data-testid="stMetricValue"] > div {
    font-size: 2.1rem !important;
    line-height: 1.1 !important;
}
div[data-testid="stMetricDelta"] > div {
    font-size: 0.9rem !important;
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
    line-height: 1.2 !important;
    word-break: break-word !important;
}
</style>
"""


async def show_deposit_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="deposit-title">First Deposit Report</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    type_options = {"All": "all", "User Acquisition": "user_acquisition", "Brand Awareness": "brand_awareness"}
    date_range_key = "deposit_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        left_col, right_col = st.columns([2, 2], gap="small")
        with left_col:
            period_key = st.selectbox("Periods", options=list(presets.keys()), index=0, key="deposit_period")
            if period_key == "Custom Range":
                selected = st.date_input("Select Date Range", key=date_range_key)
                if not isinstance(selected, tuple) or len(selected) != 2:
                    st.warning("Please select a valid date range.")
                    return
                start_date, end_date = selected
            else:
                start_date, end_date = presets[period_key]
                if st.session_state.get(date_range_key) != (start_date, end_date):
                    st.session_state[date_range_key] = (start_date, end_date)
        with right_col:
            selected_type_label = st.selectbox("Campaign Type", options=list(type_options.keys()), index=0, key="deposit_campaign_type")

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_type = type_options[selected_type_label]
    selected_range = (start_date, end_date, selected_type)
    should_fetch = "deposit_daily_payload" not in st.session_state or st.session_state.get("deposit_daily_range") != selected_range

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching deposit report..."):
            response = await fetch_data(st=st, host=host, uri="deposit/daily-report", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "campaign_type": selected_type})
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch deposit report.")
            return
        st.session_state["deposit_daily_payload"] = response
        st.session_state["deposit_daily_range"] = selected_range

    report = st.session_state.get("deposit_daily_payload", {}).get("data", {}).get("report", {})
    st.markdown('<div class="metric-section-title">First Deposit Summary</div>', unsafe_allow_html=True)
    currency_wrap_left, currency_wrap_mid, currency_wrap_right = st.columns([2.2, 2.6, 2.2], gap="small")
    with currency_wrap_mid:
        label_col, control_col = st.columns([1.1, 2.2], gap="small")
        with label_col:
            st.markdown('<div class="currency-inline-label">Currency</div>', unsafe_allow_html=True)
        with control_col:
            currency_unit = st.radio("Currency", options=["USD", "IDR"], horizontal=True, key="deposit_currency_unit", label_visibility="collapsed")

    render_metric_cards(report, currency_unit=currency_unit)
    daily_amount_figure = build_daily_deposit_amount_figure(report, currency_unit=currency_unit)
    qty_aov_figure = build_daily_deposit_qty_aov_figure(report, currency_unit=currency_unit)
    top_campaign_figure = build_top_campaign_deposit_figure(report, currency_unit=currency_unit)
    for figure, height in [(daily_amount_figure, 420), (qty_aov_figure, 420), (top_campaign_figure, 480)]:
        figure.update_layout(height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")

    for column, figure in zip(st.columns(2, gap="small"), [daily_amount_figure, qty_aov_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")
    with st.container(border=True):
        st.plotly_chart(top_campaign_figure, width="stretch")
    render_report_table(report, currency_unit=currency_unit)
