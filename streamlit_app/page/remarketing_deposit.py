"""Streamlit page for the remarketing deposit report."""

import datetime as dt

import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.page.deposit import PAGE_STYLE
from streamlit_app.page.deposit_components.charts import (
    build_campaign_deposit_amount_heatmap_figure,
    build_daily_deposit_amount_figure,
    build_daily_deposit_qty_aov_figure,
    build_top_campaign_deposit_figure,
)
from streamlit_app.page.deposit_components.rendering import (
    render_campaign_deposit_table,
    render_metric_cards,
)


async def show_remarketing_deposit_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="deposit-title">Remarketing Deposit Report</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    type_options = {
        "All": "all",
        "Remarketing": "remarketing",
        "User Acquisition": "user_acquisition",
        "Brand Awareness": "brand_awareness",
    }
    date_range_key = "remarketing_deposit_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        left_col, right_col = st.columns([2, 2], gap="small")
        with left_col:
            period_key = st.selectbox(
                "Periods",
                options=list(presets.keys()),
                index=0,
                key="remarketing_deposit_period",
            )
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
            selected_type_label = st.selectbox(
                "Campaign Type",
                options=list(type_options.keys()),
                index=0,
                key="remarketing_deposit_campaign_type",
            )

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_type = type_options[selected_type_label]
    selected_range = (start_date, end_date, selected_type)
    should_fetch = (
        "remarketing_deposit_payload" not in st.session_state
        or st.session_state.get("remarketing_deposit_range") != selected_range
    )

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching remarketing deposit report..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="deposit/remarketing-report",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "campaign_type": selected_type,
                },
            )
        if not isinstance(response, dict) or not response.get("success", False):
            st.error(
                (response or {}).get("detail")
                or (response or {}).get("message")
                or "Failed to fetch remarketing deposit report."
            )
            return
        st.session_state["remarketing_deposit_payload"] = response
        st.session_state["remarketing_deposit_range"] = selected_range

    report = st.session_state.get("remarketing_deposit_payload", {}).get("data", {}).get("report", {})
    deposit_label = "Remarketing Deposit"
    st.markdown('<div class="metric-section-title">Remarketing Deposit Summary</div>', unsafe_allow_html=True)
    currency_wrap_left, currency_wrap_mid, currency_wrap_right = st.columns([2.2, 2.6, 2.2], gap="small")
    with currency_wrap_mid:
        label_col, control_col = st.columns([1.1, 2.2], gap="small")
        with label_col:
            st.markdown('<div class="currency-inline-label">Currency</div>', unsafe_allow_html=True)
        with control_col:
            currency_unit = st.radio(
                "Currency",
                options=["USD", "IDR"],
                horizontal=True,
                key="remarketing_deposit_currency_unit",
                label_visibility="collapsed",
            )

    render_metric_cards(report, currency_unit=currency_unit, deposit_label=deposit_label, split_by_user=False)
    daily_amount_figure = build_daily_deposit_amount_figure(
        report,
        currency_unit=currency_unit,
        deposit_label=deposit_label,
        split_by_user=False,
    )
    qty_aov_figure = build_daily_deposit_qty_aov_figure(
        report,
        currency_unit=currency_unit,
        deposit_label=deposit_label,
        split_by_user=False,
    )
    top_campaign_figure = build_top_campaign_deposit_figure(
        report,
        currency_unit=currency_unit,
        deposit_label=deposit_label,
        split_by_user=False,
    )
    campaign_heatmap_figure = build_campaign_deposit_amount_heatmap_figure(
        report,
        currency_unit=currency_unit,
        deposit_label=deposit_label,
    )
    for figure, height in [(daily_amount_figure, 420), (qty_aov_figure, 420), (top_campaign_figure, 480), (campaign_heatmap_figure, 560)]:
        figure.update_layout(height=height, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")

    for column, figure in zip(st.columns(2, gap="small"), [daily_amount_figure, qty_aov_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")
    with st.container(border=True):
        st.plotly_chart(top_campaign_figure, width="stretch")
    with st.container(border=True):
        st.plotly_chart(campaign_heatmap_figure, width="stretch")
    st.markdown('<div class="metric-section-title">Remarketing Deposit by Campaign</div>', unsafe_allow_html=True)
    with st.container(border=True):
        render_campaign_deposit_table(report, currency_unit=currency_unit, height=420)
