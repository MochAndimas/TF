"""Streamlit page for internal register analytics."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.page.campaign_components.common import PAGE_STYLE, set_transparent_chart_background


SOURCE_OPTIONS = {
    "All Sources": "all",
    "Google Ads": "google",
    "Facebook Ads": "facebook",
    "TikTok Ads": "tiktok",
}


def _render_filters() -> tuple[dt.date | None, dt.date | None, str | None]:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Internal Register</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    if "internal_register_date_range" not in st.session_state:
        st.session_state["internal_register_date_range"] = presets["Last 7 Day"]

    with st.container(border=True):
        period_col, source_col = st.columns([2, 2], gap="small")
        with period_col:
            selected_period = st.selectbox("Periods", options=list(presets.keys()), index=0, key="internal_register_period")
            if selected_period == "Custom Range":
                selected = st.date_input("Select Date Range", key="internal_register_date_range")
                if not isinstance(selected, tuple) or len(selected) != 2:
                    st.warning("Please select a valid date range.")
                    return None, None, None
                start_date, end_date = selected
            else:
                start_date, end_date = presets[selected_period]
                if st.session_state.get("internal_register_date_range") != (start_date, end_date):
                    st.session_state["internal_register_date_range"] = (start_date, end_date)
        with source_col:
            selected_source = st.selectbox("Register Source", options=list(SOURCE_OPTIONS.keys()), index=0, key="internal_register_source")

    return start_date, end_date, selected_source


def _fmt_int(value) -> str:
    return f"{int(float(value or 0)):,.0f}"


def _fmt_float(value) -> str:
    return f"{float(value or 0):,.2f}"


def _render_metrics(metrics: dict[str, object]) -> None:
    current_metrics = metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = metrics.get("growth_percentage", {})
    columns = st.columns(5, gap="small")
    metric_specs = [
        ("Total Register", "total_register", _fmt_int(current_metrics.get("total_register")), None),
        ("Avg Daily", "avg_daily_register", _fmt_float(current_metrics.get("avg_daily_register")), None),
        ("Active Campaigns", "active_campaigns", _fmt_int(current_metrics.get("active_campaigns")), None),
        ("Active Sources", "active_sources", _fmt_int(current_metrics.get("active_sources")), None),
        ("Peak Day Register", "peak_day_register", _fmt_int(current_metrics.get("peak_day_register")), f"Peak day: {current_metrics.get('peak_day') or '-'}"),
    ]
    for column, (label, key, value, tooltip) in zip(columns, metric_specs):
        with column:
            with st.container(border=True):
                growth_value = growth_metrics.get(key, 0.0)
                st.metric(
                    label,
                    value,
                    delta=_campaign_format_growth(growth_value),
                    delta_color="off" if growth_value == 0 else "normal",
                    help=tooltip,
                )


def _details_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    rename_map = {
        "campaign_id": "Campaign ID",
        "campaign_name": "Campaign Name",
        "ad_source": "Source",
        "ad_type": "Type",
        "total_register": "Register",
        "active_days": "Active Days",
        "avg_daily_register": "Avg Daily",
        "share_pct": "Share %",
        "first_date": "First Date",
        "last_date": "Last Date",
    }
    df = df.rename(columns=rename_map)
    columns = ["Campaign ID", "Campaign Name", "Source", "Type", "Register", "Share %", "Avg Daily"]
    return df[[column for column in columns if column in df.columns]]


async def show_internal_register_page(host: str) -> None:
    start_date, end_date, selected_source = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    source_key = SOURCE_OPTIONS[selected_source]
    selected_range = (start_date, end_date, source_key)
    cached_payload = st.session_state.get("internal_register_payload", {})
    should_fetch = "internal_register_payload" not in st.session_state or st.session_state.get("internal_register_range") != selected_range

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching data..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="campaign/internal-register",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "source": source_key,
                },
            )
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch internal register.")
            return
        st.session_state["internal_register_payload"] = response
        st.session_state["internal_register_range"] = selected_range

    data = st.session_state.get("internal_register_payload", {}).get("data", {})
    _render_metrics(data.get("metrics", {}))

    charts = data.get("charts", {})
    daily_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("daily_trend", {}).get("figure"), "Daily Internal Register"))
    cumulative_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("cumulative_trend", {}).get("figure"), "Cumulative Register by Campaign"))
    source_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("source_mix", {}).get("figure"), "Register by Source"))
    type_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("type_mix", {}).get("figure"), "Register by Campaign Type"))
    top_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("top_campaigns", {}).get("figure"), "Top Campaigns by Register"))
    heatmap_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("campaign_heatmap", {}).get("figure"), "Daily Register Heatmap"))

    daily_figure.update_layout(height=440)
    cumulative_figure.update_layout(height=440)
    source_figure.update_layout(height=440)
    type_figure.update_layout(height=440)
    top_figure.update_layout(height=520)
    heatmap_figure.update_layout(height=560)

    for column, figure in zip(st.columns(2, gap="small"), [daily_figure, cumulative_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    for column, figure in zip(st.columns(2, gap="small"), [source_figure, type_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    with st.container(border=True):
        st.plotly_chart(top_figure, width="stretch")

    with st.container(border=True):
        st.plotly_chart(heatmap_figure, width="stretch")

    st.markdown("### Campaign Register Details")
    details_df = _details_dataframe(data.get("details", []))
    if details_df.empty:
        st.info("No internal register data for selected date range.")
        return
    st.dataframe(
        details_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Register": st.column_config.NumberColumn("Register", format="%d"),
            "Share %": st.column_config.NumberColumn("Share %", format="%.2f"),
            "Avg Daily": st.column_config.NumberColumn("Avg Daily", format="%.2f"),
            "Active Days": st.column_config.NumberColumn("Active Days", format="%d"),
        },
    )
