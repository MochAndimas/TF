"""Streamlit renderer for the main Overview page.

This module binds period selection, API fetching, session-state caching, and UI
composition for all overview sections shown on FE.
"""

import datetime as dt
import asyncio

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from decouple import config

from streamlit_app.functions.utils import (
    campaign_figure_from_payload,
    campaign_preset_ranges,
    fetch_data,
    render_brand_awareness_metric_cards,
    render_overview_cost_metric_cards,
    render_overview_leads_metric_cards,
    render_overview_metric_cards,
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

USD_TO_IDR_RATE = config("USD_TO_IDR_RATE", default=16968, cast=float)


def _set_transparent_chart_background(figure):
    """Apply the app's transparent chart canvas style to a Plotly figure.

    Args:
        figure: Plotly figure object parsed from backend payload.

    Returns:
        go.Figure: Same figure with transparent paper and plot backgrounds.
    """
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _render_currency_toggle(key: str) -> str:
    """Render a compact inline currency selector for overview subsections.

    Args:
        key (str): Stable Streamlit session-state key for the radio widget.

    Returns:
        str: Selected currency unit, currently ``"IDR"`` or ``"USD"``.
    """
    left, mid, right = st.columns([2.2, 2.6, 2.2], gap="small")
    with mid:
        label_col, control_col = st.columns([1.1, 2.2], gap="small")
        with label_col:
            st.markdown(
                '<div style="font-size:0.98rem;font-weight:600;padding-top:0.15rem;text-align:right;">Currency</div>',
                unsafe_allow_html=True,
            )
        with control_col:
            return st.radio(
                "Currency",
                options=["IDR", "USD"],
                horizontal=True,
                key=key,
                label_visibility="collapsed",
            )


def _convert_idr_to_usd(value: float | int) -> float:
    """Convert an IDR-denominated value into USD using the env FX rate.

    Args:
        value (float | int): Monetary value currently represented in IDR.

    Returns:
        float: Converted USD value. Returns the original numeric value when the
        configured exchange rate is invalid or zero.
    """
    rate = float(USD_TO_IDR_RATE or 0)
    if rate == 0:
        return float(value)
    return float(value) / rate


def _format_currency_value(value: float | int, currency_unit: str) -> str:
    """Format a table currency cell according to the selected display unit.

    Args:
        value (float | int): Numeric currency value already expressed in the
            target unit.
        currency_unit (str): Selected display currency, ``IDR`` or ``USD``.

    Returns:
        str: Human-readable formatted currency string for table rendering.
    """
    number = float(value)
    if currency_unit == "USD":
        return f"$ {number:,.2f}" if abs(number) < 1000 else f"$ {number:,.0f}"
    return f"Rp {number:,.0f}"


def _apply_currency_to_ua_table(leads_table_df: pd.DataFrame, currency_unit: str) -> pd.DataFrame:
    """Convert and format UA overview table currency columns for one unit.

    Args:
        leads_table_df (pd.DataFrame): Source table dataframe for the leads-by-
            source section.
        currency_unit (str): Selected display currency, ``IDR`` or ``USD``.

    Returns:
        pd.DataFrame: Display-ready dataframe with money columns converted and
        formatted while count columns keep their integer formatting.
    """
    if leads_table_df.empty:
        return leads_table_df

    formatted = leads_table_df.copy()
    if currency_unit == "USD":
        formatted["Cost"] = formatted["Cost"].apply(lambda value: _format_currency_value(_convert_idr_to_usd(value), "USD"))
        formatted["Cost/Lead"] = formatted["Cost/Lead"].apply(
            lambda value: _format_currency_value(_convert_idr_to_usd(value), "USD")
        )
    else:
        formatted["Cost"] = formatted["Cost"].apply(lambda value: _format_currency_value(value, "IDR"))
        formatted["Cost/Lead"] = formatted["Cost/Lead"].apply(lambda value: _format_currency_value(value, "IDR"))
    formatted["Impressions"] = formatted["Impressions"].apply(lambda value: f"{int(value):,}")
    formatted["Clicks"] = formatted["Clicks"].apply(lambda value: f"{int(value):,}")
    formatted["Leads"] = formatted["Leads"].apply(lambda value: f"{int(value):,}")
    return formatted


def _apply_currency_to_ua_figure(figure: go.Figure, chart_type: str, currency_unit: str) -> go.Figure:
    """Convert existing UA chart traces into the selected display currency.

    Args:
        figure (go.Figure): Plotly figure built from backend payload.
        chart_type (str): Supported chart type discriminator used to decide
            which traces and axis labels require currency conversion.
        currency_unit (str): Selected display currency, ``IDR`` or ``USD``.

    Returns:
        go.Figure: Same figure instance with converted values, updated labels,
        and currency-aware hover text when applicable.
    """
    if currency_unit == "IDR" or not figure.data:
        return figure

    if chart_type == "cost_vs_leads":
        cost_trace = figure.data[0]
        cpl_trace = figure.data[1]
        cost_trace.y = [_convert_idr_to_usd(value) for value in (cost_trace.y or [])]
        cpl_trace.y = [_convert_idr_to_usd(value) for value in (cpl_trace.y or [])]
        cost_trace.name = "Cost (USD)"
        cpl_trace.name = "Cost/Lead (USD)"
        cost_trace.hovertemplate = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        cpl_trace.hovertemplate = "<b>%{x}</b><br>Cost/Lead: $ %{y:,.2f}<extra></extra>"
        figure.update_layout(
            yaxis=dict(title="Cost (USD)"),
            yaxis2=dict(title="Cost/Lead (USD)"),
        )
        return figure

    if chart_type == "cost_to_revenue":
        cost_trace = figure.data[0]
        deposit_trace = figure.data[1]
        cost_trace.y = [_convert_idr_to_usd(value) for value in (cost_trace.y or [])]
        deposit_trace.y = [_convert_idr_to_usd(value) for value in (deposit_trace.y or [])]
        cost_trace.name = "Cost (USD)"
        deposit_trace.name = "First Deposit (USD)"
        cost_trace.hovertemplate = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        deposit_trace.hovertemplate = "<b>%{x}</b><br>First Deposit: $ %{y:,.2f}<extra></extra>"
        figure.update_layout(
            yaxis=dict(title="Cost (USD)"),
            yaxis2=dict(title="First Deposit (USD)"),
        )
        return figure

    return figure


def _build_cost_vs_deposit_figure(
    rows: list[dict],
    currency_unit: str,
) -> go.Figure:
    """Build the nominal daily chart for cost versus first deposit.

    Args:
        rows (list[dict]): Serialized daily rows from the backend chart payload.
        currency_unit (str): Selected display currency, ``IDR`` or ``USD``.

    Returns:
        go.Figure: Dual-axis grouped bar chart comparing cost and first-deposit
        values day by day in the selected currency.
    """
    daily_df = pd.DataFrame(rows or [])
    if daily_df.empty:
        figure = go.Figure()
        figure.update_layout(
            title="Cost vs First Deposit Per Hari",
            annotations=[
                {
                    "text": "No data available",
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                }
            ],
        )
        return figure

    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    daily_df["cost"] = pd.to_numeric(daily_df.get("cost", 0), errors="coerce").fillna(0.0)
    daily_df["first_deposit_idr"] = pd.to_numeric(
        daily_df.get("first_deposit_idr", 0),
        errors="coerce",
    ).fillna(0.0)
    date_labels = daily_df["date"].dt.strftime("%b %d\n%Y").tolist()
    cost_values = daily_df["cost"].tolist()
    deposit_values = daily_df["first_deposit_idr"].tolist()

    if currency_unit == "USD":
        cost_values = [_convert_idr_to_usd(value) for value in cost_values]
        deposit_values = [_convert_idr_to_usd(value) for value in deposit_values]
        cost_hover = "<b>%{x}</b><br>Cost: $ %{y:,.2f}<extra></extra>"
        deposit_hover = "<b>%{x}</b><br>First Deposit: $ %{y:,.2f}<extra></extra>"
        yaxis_title = "USD"
        cost_name = "Cost (USD)"
        deposit_name = "First Deposit (USD)"
    else:
        cost_hover = "<b>%{x}</b><br>Cost: Rp %{y:,.0f}<extra></extra>"
        deposit_hover = "<b>%{x}</b><br>First Deposit: Rp %{y:,.0f}<extra></extra>"
        yaxis_title = "IDR"
        cost_name = "Cost (IDR)"
        deposit_name = "First Deposit (IDR)"

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=cost_values,
            name=cost_name,
            marker_color="#6176ff",
            yaxis="y",
            offsetgroup="cost",
            hovertemplate=cost_hover,
        )
    )
    figure.add_trace(
        go.Bar(
            x=date_labels,
            y=deposit_values,
            name=deposit_name,
            marker_color="#13c39c",
            yaxis="y2",
            offsetgroup="deposit",
            hovertemplate=deposit_hover,
        )
    )
    figure.update_layout(
        title="Cost vs First Deposit Per Hari",
        barmode="group",
        xaxis=dict(type="category"),
        yaxis=dict(title=f"Cost ({currency_unit})"),
        yaxis2=dict(
            title=f"First Deposit ({currency_unit})",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        legend=dict(orientation="h", y=1.1, x=0),
    )
    return figure


def _build_cost_to_deposit_ratio_figure(rows: list[dict]) -> go.Figure:
    """Build the daily efficiency chart for cost-to-first-deposit percentage.

    Args:
        rows (list[dict]): Serialized daily rows containing
            ``cost_to_revenue_pct`` values from the backend payload.

    Returns:
        go.Figure: Line chart showing the daily percentage trend.
    """
    daily_df = pd.DataFrame(rows or [])
    if daily_df.empty:
        figure = go.Figure()
        figure.update_layout(
            title="Cost To First Deposit (%) Per Hari",
            annotations=[
                {
                    "text": "No data available",
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.5,
                    "y": 0.5,
                    "showarrow": False,
                }
            ],
        )
        return figure

    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    daily_df["cost_to_revenue_pct"] = pd.to_numeric(
        daily_df.get("cost_to_revenue_pct", 0),
        errors="coerce",
    ).fillna(0.0)
    date_labels = daily_df["date"].dt.strftime("%b %d\n%Y").tolist()
    ratio_values = daily_df["cost_to_revenue_pct"].tolist()

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=date_labels,
            y=ratio_values,
            mode="lines+markers",
            name="Cost To First Deposit",
            line=dict(color="#ff6248", width=2),
            hovertemplate="<b>%{x}</b><br>Cost To First Deposit: %{y:.2f}%<extra></extra>",
        )
    )
    figure.update_layout(
        title="Cost To First Deposit (%) Per Hari",
        xaxis=dict(type="category"),
        yaxis=dict(title="Percent", ticksuffix="%"),
        legend=dict(orientation="h", y=1.1, x=0),
    )
    return figure


async def show_overview_page(host: str) -> None:
    """Render full Overview page with all metric and chart sections.

    Args:
        host: Backend API base URL used by ``fetch_data`` calls.

    Returns:
        None: Produces Streamlit UI side effects and updates session caches.
    """
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Overview</div>', unsafe_allow_html=True)
    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "overview_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        period_key = st.selectbox(
            "Periods",
            options=list(presets.keys()),
            index=0,
            key="overview_period",
        )
        if period_key == "Custom Range":
            selected = st.date_input(
                "Select Date Range",
                key=date_range_key,
            )
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
    cached_cost_payload = st.session_state.get("overview_campaign_cost_payload", {})
    has_cost_metric = (
        "total_ad_cost" in
        cached_cost_payload.get("data", {}).get("cost_metrics_with_growth", {}).get("current_period", {}).get("metrics", {})
    )
    cached_active_payload_map = st.session_state.get("overview_active_users_payload_by_source", {})
    cached_app_payload = cached_active_payload_map.get("app", {})
    cached_web_payload = cached_active_payload_map.get("web", {})
    has_app_metric = (
        "active_user" in
        cached_app_payload.get("data", {}).get("stickiness_with_growth", {}).get("current_period", {}).get("metrics", {})
    )
    has_web_metric = (
        "active_user" in
        cached_web_payload.get("data", {}).get("stickiness_with_growth", {}).get("current_period", {}).get("metrics", {})
    )
    cached_leads_payload = st.session_state.get("overview_leads_payload", {})
    has_leads_metric = (
        "cost_leads" in
        cached_leads_payload.get("data", {}).get("metrics_with_growth", {}).get("current_period", {}).get("metrics", {})
    )
    cached_brand_payload = st.session_state.get("overview_brand_payload", {})
    has_brand_metric = (
        "ctr" in
        cached_brand_payload.get("data", {}).get("metrics_with_growth", {}).get("current_period", {}).get("metrics", {})
    )

    should_fetch_cost = (
        "overview_campaign_cost_payload" not in st.session_state
        or not has_cost_metric
        or st.session_state.get("overview_cost_range") != selected_range
    )
    should_fetch_active = (
        "overview_active_users_payload_by_source" not in st.session_state
        or not has_app_metric
        or not has_web_metric
        or st.session_state.get("overview_active_range") != selected_range
    )
    should_fetch_leads = (
        "overview_leads_payload" not in st.session_state
        or not has_leads_metric
        or st.session_state.get("overview_leads_range") != selected_range
    )
    should_fetch_brand = (
        "overview_brand_payload" not in st.session_state
        or not has_brand_metric
        or st.session_state.get("overview_brand_range") != selected_range
    )

    if should_fetch_cost or should_fetch_active or should_fetch_leads or should_fetch_brand:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return

        response_cost = None
        response_leads = None
        response_brand = None
        response_app = None
        response_web = None
        with st.spinner("Fetching data..."):
            if should_fetch_cost:
                response_cost = await fetch_data(
                    st=st,
                    host=host,
                    uri="overview/campaign-cost",
                    method="GET",
                    params={
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
            if should_fetch_active:
                response_app, response_web = await asyncio.gather(
                    fetch_data(
                        st=st,
                        host=host,
                        uri="overview/active-users",
                        method="GET",
                        params={
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                            "source": "app",
                        },
                    ),
                    fetch_data(
                        st=st,
                        host=host,
                        uri="overview/active-users",
                        method="GET",
                        params={
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                            "source": "web",
                        },
                    ),
                )
            if should_fetch_leads:
                response_leads = await fetch_data(
                    st=st,
                    host=host,
                    uri="overview/leads-acquisition",
                    method="GET",
                    params={
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )
            if should_fetch_brand:
                response_brand = await fetch_data(
                    st=st,
                    host=host,
                    uri="overview/brand-awareness",
                    method="GET",
                    params={
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )

        if should_fetch_cost:
            if not isinstance(response_cost, dict):
                st.error("Invalid response from overview campaign cost endpoint.")
                return
            if not response_cost.get("success", False):
                st.error(
                    response_cost.get("detail")
                    or response_cost.get("message")
                    or "Failed to fetch overview campaign cost."
                )
                return
            st.session_state["overview_campaign_cost_payload"] = response_cost
            st.session_state["overview_cost_range"] = selected_range

        if should_fetch_leads:
            if not isinstance(response_leads, dict):
                st.error("Invalid response from overview leads endpoint.")
                return
            if not response_leads.get("success", False):
                st.error(
                    response_leads.get("detail")
                    or response_leads.get("message")
                    or "Failed to fetch overview leads acquisition."
                )
                return
            st.session_state["overview_leads_payload"] = response_leads
            st.session_state["overview_leads_range"] = selected_range

        if should_fetch_brand:
            if not isinstance(response_brand, dict):
                st.error("Invalid response from overview brand awareness endpoint.")
                return
            if not response_brand.get("success", False):
                st.error(
                    response_brand.get("detail")
                    or response_brand.get("message")
                    or "Failed to fetch overview brand awareness."
                )
                return
            st.session_state["overview_brand_payload"] = response_brand
            st.session_state["overview_brand_range"] = selected_range

        if should_fetch_active:
            if not isinstance(response_app, dict) or not isinstance(response_web, dict):
                st.error("Invalid response from overview active users endpoint.")
                return
            if not response_app.get("success", False):
                st.error(
                    response_app.get("detail")
                    or response_app.get("message")
                    or "Failed to fetch overview active users app."
                )
                return
            if not response_web.get("success", False):
                st.error(
                    response_web.get("detail")
                    or response_web.get("message")
                    or "Failed to fetch overview active users web."
                )
                return
            st.session_state["overview_active_users_payload_by_source"] = {
                "app": response_app,
                "web": response_web,
            }
            st.session_state["overview_active_range"] = selected_range

    st.markdown('<div class="metric-section-title">FireBase Active User</div>', unsafe_allow_html=True)
    source_options = {"App": "app", "Web": "web"}
    source_col_left, source_col_right = st.columns([1, 3], gap="small")
    with source_col_left:
        selected_source = st.selectbox(
            "Source",
            options=list(source_options.keys()),
            index=0,
            key="overview_active_users_source",
        )
    with source_col_right:
        st.empty()

    selected_source_key = source_options[selected_source]
    active_payload = st.session_state.get("overview_active_users_payload_by_source", {}).get(selected_source_key, {})
    active_data = active_payload.get("data", {})
    stickiness_summary = active_data.get("stickiness_with_growth", {})
    active_chart_payload = active_data.get("active_users_chart", {}).get("figure")

    render_overview_metric_cards(st, stickiness_summary)

    active_chart_figure = campaign_figure_from_payload(
        active_chart_payload,
        f"FireBase Active User {selected_source.upper()}",
    )
    active_chart_figure = _set_transparent_chart_background(active_chart_figure)
    active_chart_figure.update_layout(height=430)

    with st.container(border=True):
        st.plotly_chart(active_chart_figure, width="stretch")

    payload = st.session_state.get("overview_campaign_cost_payload", {})
    overview_data = payload.get("data", {})
    cost_summary = overview_data.get("cost_metrics_with_growth", {})
    cost_charts = overview_data.get("cost_breakdown_charts", {})

    st.markdown(
        '<div class="metric-section-title">Ad Cost Spend</div>',
        unsafe_allow_html=True,
    )
    render_overview_cost_metric_cards(st, cost_summary)

    cost_by_type_payload = cost_charts.get("cost_by_campaign_type", {}).get("figure")
    ua_by_platform_payload = cost_charts.get("ua_cost_by_platform", {}).get("figure")
    ba_by_platform_payload = cost_charts.get("ba_cost_by_platform", {}).get("figure")

    cost_by_type_figure = campaign_figure_from_payload(
        cost_by_type_payload,
        "Cost by Campaign Type",
    )
    ua_by_platform_figure = campaign_figure_from_payload(
        ua_by_platform_payload,
        "User Acquisition Cost by Platform",
    )
    ba_by_platform_figure = campaign_figure_from_payload(
        ba_by_platform_payload,
        "Brand Awareness Cost by Platform",
    )
    cost_by_type_figure = _set_transparent_chart_background(cost_by_type_figure)
    ua_by_platform_figure = _set_transparent_chart_background(ua_by_platform_figure)
    ba_by_platform_figure = _set_transparent_chart_background(ba_by_platform_figure)
    cost_by_type_figure.update_layout(height=420)
    ua_by_platform_figure.update_layout(height=420)
    ba_by_platform_figure.update_layout(height=420)

    pie_left, pie_mid, pie_right = st.columns(3, gap="small")
    with pie_left:
        with st.container(border=True):
            st.plotly_chart(cost_by_type_figure, width="stretch")
    with pie_mid:
        with st.container(border=True):
            st.plotly_chart(ua_by_platform_figure, width="stretch")
    with pie_right:
        with st.container(border=True):
            st.plotly_chart(ba_by_platform_figure, width="stretch")

    brand_payload = st.session_state.get("overview_brand_payload", {})
    brand_data = brand_payload.get("data", {})
    brand_summary = brand_data.get("metrics_with_growth", {})
    brand_spend_payload = brand_data.get("spend_chart", {}).get("figure")
    brand_performance_payload = brand_data.get("performance_chart", {}).get("figure")

    st.markdown(
        '<div class="metric-section-title">Overall Performance Campaign Brand Awareness</div>',
        unsafe_allow_html=True,
    )
    render_brand_awareness_metric_cards(st, brand_summary, "")

    brand_spend_figure = campaign_figure_from_payload(
        brand_spend_payload,
        "Brand Awareness Spend",
    )
    brand_performance_figure = campaign_figure_from_payload(
        brand_performance_payload,
        "Brand Awareness Performance",
    )
    brand_spend_figure = _set_transparent_chart_background(brand_spend_figure)
    brand_performance_figure = _set_transparent_chart_background(brand_performance_figure)
    brand_spend_figure.update_layout(height=430)
    brand_performance_figure.update_layout(height=430)

    brand_left, brand_right = st.columns(2, gap="small")
    with brand_left:
        with st.container(border=True):
            st.plotly_chart(brand_spend_figure, width="stretch")
    with brand_right:
        with st.container(border=True):
            st.plotly_chart(brand_performance_figure, width="stretch")

    leads_payload = st.session_state.get("overview_leads_payload", {})
    leads_data = leads_payload.get("data", {})
    leads_summary = leads_data.get("metrics_with_growth", {})
    leads_by_source = leads_data.get("leads_by_source", {})
    cost_vs_leads_payload = leads_data.get("cost_vs_leads_chart", {}).get("figure")
    leads_per_day_payload = leads_data.get("leads_per_day_chart", {}).get("figure")
    cost_to_revenue_chart_payload = leads_data.get("cost_to_revenue_chart", {})

    st.markdown(
        '<div class="metric-section-title">Overall Performance Campaign User Acquisition</div>',
        unsafe_allow_html=True,
    )
    leads_currency_unit = _render_currency_toggle("overview_leads_currency_unit")
    render_overview_leads_metric_cards(st, leads_summary, currency_unit=leads_currency_unit)

    leads_table_rows = leads_by_source.get("table_rows", [])
    leads_table_df = pd.DataFrame(leads_table_rows)
    if not leads_table_df.empty:
        leads_table_df = leads_table_df.rename(
            columns={
                "source": "Source",
                "cost": "Cost",
                "impressions": "Impressions",
                "clicks": "Clicks",
                "leads": "Leads",
                "cost_per_lead": "Cost/Lead",
            }
        )
        desired_columns = ["Source", "Cost", "Impressions", "Clicks", "Leads", "Cost/Lead"]
        leads_table_df = leads_table_df[[column for column in desired_columns if column in leads_table_df.columns]]
        for column_name in ("Cost", "Impressions", "Clicks", "Leads", "Cost/Lead"):
            leads_table_df[column_name] = pd.to_numeric(leads_table_df[column_name], errors="coerce").fillna(0)
        leads_table_df = _apply_currency_to_ua_table(leads_table_df, leads_currency_unit)

    leads_pie_payload = leads_by_source.get("pie_chart", {}).get("figure")
    leads_pie_figure = campaign_figure_from_payload(leads_pie_payload, "Leads by Source")
    leads_pie_figure = _set_transparent_chart_background(leads_pie_figure)
    leads_pie_figure.update_layout(height=430)

    leads_source_left, leads_source_right = st.columns(2, gap="small")
    with leads_source_left:
        with st.container(border=True):
            st.markdown("### Leads by Source Table")
            if leads_table_df.empty:
                st.info("No leads source data for selected period.")
            else:
                st.dataframe(
                    leads_table_df,
                    width="stretch",
                    hide_index=True,
                    height=380,
                )
    with leads_source_right:
        with st.container(border=True):
            st.plotly_chart(leads_pie_figure, width="stretch")

    cost_vs_leads_figure = campaign_figure_from_payload(
        cost_vs_leads_payload,
        "Cost per Leads (Cost & Cost/Lead)",
    )
    cost_vs_leads_figure = _set_transparent_chart_background(cost_vs_leads_figure)
    cost_vs_leads_figure = _apply_currency_to_ua_figure(
        cost_vs_leads_figure,
        chart_type="cost_vs_leads",
        currency_unit=leads_currency_unit,
    )
    cost_vs_leads_figure.update_layout(height=430)

    leads_per_day_figure = campaign_figure_from_payload(
        leads_per_day_payload,
        "Leads per Day",
    )
    leads_per_day_figure = _set_transparent_chart_background(leads_per_day_figure)
    leads_per_day_figure.update_layout(height=430)

    charts_left, charts_right = st.columns(2, gap="small")
    with charts_left:
        with st.container(border=True):
            st.plotly_chart(cost_vs_leads_figure, width="stretch")
    with charts_right:
        with st.container(border=True):
            st.plotly_chart(leads_per_day_figure, width="stretch")

    cost_to_revenue_rows = cost_to_revenue_chart_payload.get("rows", [])
    cost_vs_deposit_figure = _build_cost_vs_deposit_figure(
        rows=cost_to_revenue_rows,
        currency_unit=leads_currency_unit,
    )
    cost_to_deposit_ratio_figure = _build_cost_to_deposit_ratio_figure(
        rows=cost_to_revenue_rows,
    )
    cost_vs_deposit_figure = _set_transparent_chart_background(cost_vs_deposit_figure)
    cost_to_deposit_ratio_figure = _set_transparent_chart_background(cost_to_deposit_ratio_figure)
    cost_vs_deposit_figure.update_layout(height=460)
    cost_to_deposit_ratio_figure.update_layout(height=460)
    revenue_left, revenue_right = st.columns(2, gap="small")
    with revenue_left:
        with st.container(border=True):
            st.plotly_chart(cost_vs_deposit_figure, width="stretch")
    with revenue_right:
        with st.container(border=True):
            st.plotly_chart(cost_to_deposit_ratio_figure, width="stretch")
