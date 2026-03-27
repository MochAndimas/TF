"""Streamlit renderer for the main Overview page."""

import asyncio
import datetime as dt

import pandas as pd
import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import (
    render_brand_awareness_metric_cards,
    render_overview_cost_metric_cards,
    render_overview_leads_metric_cards,
    render_overview_metric_cards,
)
from streamlit_app.page.overview_components.charts import (
    build_cost_to_deposit_ratio_figure,
    build_cost_vs_deposit_figure,
)
from streamlit_app.page.overview_components.formatting import (
    apply_currency_to_ua_figure,
    apply_currency_to_ua_table,
    render_currency_toggle,
    set_transparent_chart_background,
)

PAGE_STYLE = """
<style>
.campaign-title {
    text-align: center;
    font-size: 3rem;
    font-weight: 800;
    margin-bottom: 1rem;
}
.metric-section-title {
    text-align: center;
    font-size: 2.6rem;
    font-weight: 800;
    margin: 1.6rem 0 1.1rem 0;
}
</style>
"""


def _has_metric(payload: dict, *path: str) -> bool:
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return False
        current = current.get(key, {})
    return bool(current)


async def _ensure_overview_payloads(host: str, start_date, end_date, selected_range) -> bool:
    cached_cost_payload = st.session_state.get("overview_campaign_cost_payload", {})
    cached_active_payload_map = st.session_state.get("overview_active_users_payload_by_source", {})
    cached_leads_payload = st.session_state.get("overview_leads_payload", {})
    cached_brand_payload = st.session_state.get("overview_brand_payload", {})

    should_fetch_cost = (
        "overview_campaign_cost_payload" not in st.session_state
        or not _has_metric(cached_cost_payload, "data", "cost_metrics_with_growth", "current_period", "metrics", "total_ad_cost")
        or st.session_state.get("overview_cost_range") != selected_range
    )
    should_fetch_active = (
        "overview_active_users_payload_by_source" not in st.session_state
        or not _has_metric(cached_active_payload_map.get("app", {}), "data", "stickiness_with_growth", "current_period", "metrics", "active_user")
        or not _has_metric(cached_active_payload_map.get("web", {}), "data", "stickiness_with_growth", "current_period", "metrics", "active_user")
        or not _has_metric(cached_active_payload_map.get("app_web", {}), "data", "stickiness_with_growth", "current_period", "metrics", "active_user")
        or st.session_state.get("overview_active_range") != selected_range
    )
    should_fetch_leads = (
        "overview_leads_payload" not in st.session_state
        or not _has_metric(cached_leads_payload, "data", "metrics_with_growth", "current_period", "metrics", "cost_leads")
        or st.session_state.get("overview_leads_range") != selected_range
    )
    should_fetch_brand = (
        "overview_brand_payload" not in st.session_state
        or not _has_metric(cached_brand_payload, "data", "metrics_with_growth", "current_period", "metrics", "ctr")
        or st.session_state.get("overview_brand_range") != selected_range
    )

    if not any([should_fetch_cost, should_fetch_active, should_fetch_leads, should_fetch_brand]):
        return True

    if not st.session_state.get("access_token"):
        st.error("Session invalid. Please log in again.")
        return False

    response_cost = response_leads = response_brand = None
    response_app = response_web = response_combined = None
    with st.spinner("Fetching data..."):
        if should_fetch_cost:
            response_cost = await fetch_data(st=st, host=host, uri="overview/campaign-cost", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
        if should_fetch_active:
            response_app, response_web, response_combined = await asyncio.gather(
                fetch_data(st=st, host=host, uri="overview/active-users", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "source": "app"}),
                fetch_data(st=st, host=host, uri="overview/active-users", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "source": "web"}),
                fetch_data(st=st, host=host, uri="overview/active-users", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "source": "app_web"}),
            )
        if should_fetch_leads:
            response_leads = await fetch_data(st=st, host=host, uri="overview/leads-acquisition", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
        if should_fetch_brand:
            response_brand = await fetch_data(st=st, host=host, uri="overview/brand-awareness", method="GET", params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})

    if should_fetch_cost:
        if not isinstance(response_cost, dict) or not response_cost.get("success", False):
            st.error((response_cost or {}).get("detail") or (response_cost or {}).get("message") or "Failed to fetch overview campaign cost.")
            return False
        st.session_state["overview_campaign_cost_payload"] = response_cost
        st.session_state["overview_cost_range"] = selected_range
    if should_fetch_leads:
        if not isinstance(response_leads, dict) or not response_leads.get("success", False):
            st.error((response_leads or {}).get("detail") or (response_leads or {}).get("message") or "Failed to fetch overview leads acquisition.")
            return False
        st.session_state["overview_leads_payload"] = response_leads
        st.session_state["overview_leads_range"] = selected_range
    if should_fetch_brand:
        if not isinstance(response_brand, dict) or not response_brand.get("success", False):
            st.error((response_brand or {}).get("detail") or (response_brand or {}).get("message") or "Failed to fetch overview brand awareness.")
            return False
        st.session_state["overview_brand_payload"] = response_brand
        st.session_state["overview_brand_range"] = selected_range
    if should_fetch_active:
        responses = [response_app, response_web, response_combined]
        if any(not isinstance(response, dict) or not response.get("success", False) for response in responses):
            st.error("Failed to fetch overview active users.")
            return False
        st.session_state["overview_active_users_payload_by_source"] = {"app": response_app, "web": response_web, "app_web": response_combined}
        st.session_state["overview_active_range"] = selected_range
    return True


async def show_overview_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Overview</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "overview_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        period_key = st.selectbox("Periods", options=list(presets.keys()), index=0, key="overview_period")
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

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_range = (start_date, end_date)
    if not await _ensure_overview_payloads(host, start_date, end_date, selected_range):
        return

    st.markdown('<div class="metric-section-title">FireBase Active User</div>', unsafe_allow_html=True)
    source_options = {"App + Web": "app_web", "App": "app", "Web": "web"}
    source_col_left, source_col_right = st.columns([1, 3], gap="small")
    with source_col_left:
        selected_source = st.selectbox("Source", options=list(source_options.keys()), index=0, key="overview_active_users_source")
    with source_col_right:
        st.empty()

    selected_source_key = source_options[selected_source]
    active_data = st.session_state.get("overview_active_users_payload_by_source", {}).get(selected_source_key, {}).get("data", {})
    stickiness_summary = active_data.get("stickiness_with_growth", {})
    active_chart_figure = set_transparent_chart_background(campaign_figure_from_payload(active_data.get("active_users_chart", {}).get("figure"), f"FireBase Active User {selected_source.upper()}"))
    active_chart_figure.update_layout(height=430)
    render_overview_metric_cards(st, stickiness_summary)
    with st.container(border=True):
        st.plotly_chart(active_chart_figure, width="stretch")

    overview_data = st.session_state.get("overview_campaign_cost_payload", {}).get("data", {})
    cost_summary = overview_data.get("cost_metrics_with_growth", {})
    cost_charts = overview_data.get("cost_breakdown_charts", {})
    st.markdown('<div class="metric-section-title">Ad Cost Spend</div>', unsafe_allow_html=True)
    render_overview_cost_metric_cards(st, cost_summary)
    pie_figures = [
        set_transparent_chart_background(campaign_figure_from_payload(cost_charts.get("cost_by_campaign_type", {}).get("figure"), "Cost by Campaign Type")),
        set_transparent_chart_background(campaign_figure_from_payload(cost_charts.get("ua_cost_by_platform", {}).get("figure"), "User Acquisition Cost by Platform")),
        set_transparent_chart_background(campaign_figure_from_payload(cost_charts.get("ba_cost_by_platform", {}).get("figure"), "Brand Awareness Cost by Platform")),
    ]
    for figure in pie_figures:
        figure.update_layout(height=420)
    for column, figure in zip(st.columns(3, gap="small"), pie_figures):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    brand_data = st.session_state.get("overview_brand_payload", {}).get("data", {})
    st.markdown('<div class="metric-section-title">Overall Performance Campaign Brand Awareness</div>', unsafe_allow_html=True)
    render_brand_awareness_metric_cards(st, brand_data.get("metrics_with_growth", {}), "")
    brand_spend_figure = set_transparent_chart_background(campaign_figure_from_payload(brand_data.get("spend_chart", {}).get("figure"), "Brand Awareness Spend"))
    brand_performance_figure = set_transparent_chart_background(campaign_figure_from_payload(brand_data.get("performance_chart", {}).get("figure"), "Brand Awareness Performance"))
    brand_spend_figure.update_layout(height=430)
    brand_performance_figure.update_layout(height=430)
    for column, figure in zip(st.columns(2, gap="small"), [brand_spend_figure, brand_performance_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    leads_data = st.session_state.get("overview_leads_payload", {}).get("data", {})
    st.markdown('<div class="metric-section-title">Overall Performance Campaign User Acquisition</div>', unsafe_allow_html=True)
    leads_currency_unit = render_currency_toggle("overview_leads_currency_unit")
    render_overview_leads_metric_cards(st, leads_data.get("metrics_with_growth", {}), currency_unit=leads_currency_unit)

    leads_by_source = leads_data.get("leads_by_source", {})
    leads_table_df = pd.DataFrame(leads_by_source.get("table_rows", []))
    if not leads_table_df.empty:
        leads_table_df = leads_table_df.rename(columns={"source": "Source", "cost": "Cost", "impressions": "Impressions", "clicks": "Clicks", "leads": "Leads", "cost_per_lead": "Cost/Lead"})
        desired_columns = ["Source", "Cost", "Impressions", "Clicks", "Leads", "Cost/Lead"]
        leads_table_df = leads_table_df[[column for column in desired_columns if column in leads_table_df.columns]]
        for column_name in ("Cost", "Impressions", "Clicks", "Leads", "Cost/Lead"):
            leads_table_df[column_name] = pd.to_numeric(leads_table_df[column_name], errors="coerce").fillna(0)
        leads_table_df = apply_currency_to_ua_table(leads_table_df, leads_currency_unit)

    leads_pie_figure = set_transparent_chart_background(campaign_figure_from_payload(leads_by_source.get("pie_chart", {}).get("figure"), "Leads by Source"))
    leads_pie_figure.update_layout(height=430)
    leads_source_left, leads_source_right = st.columns(2, gap="small")
    with leads_source_left:
        with st.container(border=True):
            st.markdown("### Leads by Source Table")
            if leads_table_df.empty:
                st.info("No leads source data for selected period.")
            else:
                st.dataframe(leads_table_df, width="stretch", hide_index=True, height=380)
    with leads_source_right:
        with st.container(border=True):
            st.plotly_chart(leads_pie_figure, width="stretch")

    cost_vs_leads_figure = set_transparent_chart_background(campaign_figure_from_payload(leads_data.get("cost_vs_leads_chart", {}).get("figure"), "Cost per Leads (Cost & Cost/Lead)"))
    cost_vs_leads_figure = apply_currency_to_ua_figure(cost_vs_leads_figure, chart_type="cost_vs_leads", currency_unit=leads_currency_unit)
    cost_vs_leads_figure.update_layout(height=430)
    leads_per_day_figure = set_transparent_chart_background(campaign_figure_from_payload(leads_data.get("leads_per_day_chart", {}).get("figure"), "Leads per Day"))
    leads_per_day_figure.update_layout(height=430)
    for column, figure in zip(st.columns(2, gap="small"), [cost_vs_leads_figure, leads_per_day_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    cost_to_revenue_rows = leads_data.get("cost_to_revenue_chart", {}).get("rows", [])
    cost_vs_deposit_figure = set_transparent_chart_background(build_cost_vs_deposit_figure(rows=cost_to_revenue_rows, currency_unit=leads_currency_unit))
    cost_to_deposit_ratio_figure = set_transparent_chart_background(build_cost_to_deposit_ratio_figure(rows=cost_to_revenue_rows))
    cost_vs_deposit_figure.update_layout(height=460)
    cost_to_deposit_ratio_figure.update_layout(height=460)
    for column, figure in zip(st.columns(2, gap="small"), [cost_vs_deposit_figure, cost_to_deposit_ratio_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")
