"""Streamlit page for Remarketing analytics."""

import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.metrics import render_brand_awareness_metric_cards
from streamlit_app.page.campaign_components.common import (
    render_campaign_page_filters,
    set_transparent_chart_background,
)
from streamlit_app.page.campaign_components.brand_awareness import (
    build_performance_table,
    render_performance_table,
)


async def show_remarketing_page(host: str) -> None:
    start_date, end_date, selected_source = render_campaign_page_filters(
        page_title="Remarketing",
        period_key="remarketing_campaign_period",
        date_range_key="remarketing_campaign_date_range",
        source_key="remarketing_campaign_source",
        source_options={"Google Ads": "google", "Facebook Ads": "facebook"},
    )
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    source_options = {"Google Ads": "google", "Facebook Ads": "facebook"}
    selected_key = source_options[selected_source]
    selected_range = (start_date, end_date)
    cached_payload = st.session_state.get("remarketing_campaign_payload", {})
    cached_charts = cached_payload.get("data", {}).get("charts", {})
    cached_selected = cached_charts.get(selected_key, {}) if isinstance(cached_charts, dict) else {}
    has_rows_schema = isinstance(cached_selected.get("spend", {}).get("rows"), list)
    should_fetch = "remarketing_campaign_payload" not in st.session_state or not has_rows_schema or st.session_state.get("remarketing_campaign_range") != selected_range

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching data..."):
            response = await fetch_data(st=st, host=host, uri="campaign/remarketing", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch campaign overview.")
            return
        st.session_state["remarketing_campaign_payload"] = response
        st.session_state["remarketing_campaign_range"] = selected_range

    overview_data = st.session_state.get("remarketing_campaign_payload", {}).get("data", {})
    st.markdown(
        f'<div class="metric-section-title">{selected_source} - Remarketing</div>',
        unsafe_allow_html=True,
    )
    render_brand_awareness_metric_cards(
        st,
        overview_data.get("metrics_with_growth", {}).get(selected_key, {}),
        "",
    )

    selected_charts = overview_data.get("charts", {}).get(selected_key, {})
    spend_figure = set_transparent_chart_background(campaign_figure_from_payload(selected_charts.get("spend", {}).get("figure"), f"{selected_source} - Remarketing Spend"))
    performance_figure = set_transparent_chart_background(campaign_figure_from_payload(selected_charts.get("performance", {}).get("figure"), f"{selected_source} - Remarketing Performance"))
    spend_figure.update_layout(height=540)
    performance_figure.update_layout(height=540)
    for column, figure in zip(st.columns(2, gap="small"), [spend_figure, performance_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    detail_rows = overview_data.get("details", {}).get(selected_key, {}).get("rows", [])
    selected_level, level_column, level_label, display_df = build_performance_table(detail_rows)
    if not level_column:
        st.markdown(f"### {selected_level}")
        st.info("No remarketing data for selected date range.")
        return

    insight_charts = overview_data.get("insight_charts", {})
    ratio_trends_payload = insight_charts.get("ratio_trends", {}).get(selected_key, {}).get(level_column, {})
    trend_specs = [
        ("ctr", f"{selected_source} CTR Trend"),
        ("cpm", f"{selected_source} CPM Trend"),
        ("cpc", f"{selected_source} CPC Trend"),
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

    st.markdown(f"### {selected_level}")
    render_performance_table(level_label, display_df)
