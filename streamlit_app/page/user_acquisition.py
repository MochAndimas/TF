"""Streamlit page for User Acquisition analytics."""

import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.metrics import render_campaign_metric_cards
from streamlit_app.page.campaign_components.common import (
    render_campaign_page_filters,
    set_transparent_chart_background,
)
from streamlit_app.page.campaign_components.user_acquisition import (
    build_performance_table,
    render_performance_table,
)


async def show_user_acquisition_page(host: str) -> None:
    start_date, end_date, selected_source = render_campaign_page_filters(
        page_title="User Acquisition",
        period_key="campaign_ads_period",
        date_range_key="campaign_ads_date_range",
        source_key="campaign_ads_source",
    )
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    source_options = {"Google Ads": "google", "Facebook Ads": "facebook", "TikTok Ads": "tiktok"}
    selected_key = source_options[selected_source]
    selected_range = (start_date, end_date)
    cached_payload = st.session_state.get("campaign_ads_payload", {})
    cached_details = cached_payload.get("data", {}).get("ads_campaign_details", {})
    cached_selected_details = cached_details.get(selected_key, {}) if isinstance(cached_details, dict) else {}
    has_rows_schema = isinstance(cached_selected_details.get("rows"), list)
    should_fetch = "campaign_ads_payload" not in st.session_state or not has_rows_schema or st.session_state.get("campaign_ads_range") != selected_range

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching data..."):
            response = await fetch_data(st=st, host=host, uri="campaign/user-acquisition", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch campaign overview.")
            return
        st.session_state["campaign_ads_payload"] = response
        st.session_state["campaign_ads_range"] = selected_range

    overview_data = st.session_state.get("campaign_ads_payload", {}).get("data", {})
    render_campaign_metric_cards(st, overview_data.get("ads_metrics_with_growth", {}).get(selected_key, {}), selected_source)

    selected_charts = overview_data.get("leads_performance_charts", {}).get(selected_key, {})
    chart_specs = [
        ("cost_to_leads", f"Cost To Leads - {selected_source}", 430),
        ("clicks_to_leads", f"Clicks To Leads - {selected_source}", 430),
        ("leads_by_periods", f"Leads By Periods - {selected_source}", 430),
    ]
    figures = []
    for key, title, height in chart_specs:
        figure = set_transparent_chart_background(campaign_figure_from_payload(selected_charts.get(key, {}).get("figure"), title))
        figure.update_layout(height=height)
        figures.append(figure)
    for column, figure in zip(st.columns(3, gap="small"), figures):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    selected_details = overview_data.get("ads_campaign_details", {}).get(selected_key, {})
    detail_rows = selected_details.get("rows", [])
    selected_level, level_column, level_label, display_df = build_performance_table(detail_rows)
    if not level_column:
        st.markdown(f"### {selected_level}")
        st.info("No campaign data for selected date range.")
        return

    ua_insight_charts = overview_data.get("ua_insight_charts", {})
    ratio_trends_payload = ua_insight_charts.get("ratio_trends", {}).get(selected_key, {}).get(level_column, {})
    trend_specs = [
        ("cost_per_lead", f"{selected_source} Cost per Lead Trend"),
        ("click_per_lead", f"{selected_source} Click per Lead Trend"),
        ("click_through_lead", f"{selected_source} Click Through Lead Trend"),
    ]
    st.markdown("### Campaign Insights")
    trend_figures = []
    for key, title in trend_specs:
        figure = set_transparent_chart_background(campaign_figure_from_payload(ratio_trends_payload.get(key, {}).get("figure"), title))
        figure.update_layout(height=390)
        trend_figures.append(figure)
    for column, figure in zip(st.columns(3, gap="small"), trend_figures):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    top_n = int(ua_insight_charts.get("top_n", 10) or 10)
    scatter_figure = set_transparent_chart_background(
        campaign_figure_from_payload(
            ua_insight_charts.get("spend_vs_leads", {}).get(selected_key, {}).get(level_column, {}).get("figure"),
            f"{selected_source} Spend vs Leads",
        )
    )
    top_n_figure = set_transparent_chart_background(
        campaign_figure_from_payload(
            ua_insight_charts.get("top_leads", {}).get(selected_key, {}).get(level_column, {}).get("figure"),
            f"Top {top_n} {selected_source} by Leads",
        )
    )
    scatter_figure.update_layout(height=430)
    top_n_figure.update_layout(height=430)
    for column, figure in zip(st.columns(2, gap="small"), [scatter_figure, top_n_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    st.markdown("### Daily Pacing Insights")
    cumulative_figure = set_transparent_chart_background(
        campaign_figure_from_payload(
            ua_insight_charts.get("cumulative", {}).get(selected_key, {}).get(level_column, {}).get("figure"),
            f"{selected_source} Cumulative Leads vs Spend",
        )
    )
    daily_mix_figure = set_transparent_chart_background(
        campaign_figure_from_payload(
            ua_insight_charts.get("daily_mix", {}).get(selected_key, {}).get(level_column, {}).get("figure"),
            "Daily Mix (UA Leads by Source)",
        )
    )
    cumulative_figure.update_layout(height=430)
    daily_mix_figure.update_layout(height=430)
    for column, figure in zip(st.columns(2, gap="small"), [cumulative_figure, daily_mix_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    st.markdown(f"### {selected_level}")
    render_performance_table(level_label, display_df)
