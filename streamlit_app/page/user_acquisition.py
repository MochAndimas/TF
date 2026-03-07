"""User Acquisition module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import datetime as dt
import textwrap

import pandas as pd
import streamlit as st

from streamlit_app.functions.utils import (
    campaign_figure_from_payload,
    campaign_preset_ranges,
    fetch_data,
    get_user,
    render_campaign_metric_cards,
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


def _set_transparent_chart_background(figure):
    """Apply transparent background for non-table Plotly figures.

    Args:
        figure: Plotly figure object generated from API payload.

    Returns:
        object: Same figure instance with transparent paper/plot background
        when it is not a table chart.
    """
    if not figure.data:
        return figure
    if any(getattr(trace, "type", "") == "table" for trace in figure.data):
        return figure
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_performance_dataframe(detail_rows: list[dict], level_column: str) -> pd.DataFrame:
    """Aggregate campaign detail rows for selected grouping dimension.

    Args:
        detail_rows (list[dict]): Raw detail rows from overview payload.
        level_column (str): Grouping key (`campaign_id`, `ad_group`, `ad_name`).

    Returns:
        pd.DataFrame: Aggregated performance dataframe with derived metrics
        (`click->lead`, `ctr`, `cpc`, `cpm`, `cost_per_leads`).
    """
    if not detail_rows:
        return pd.DataFrame()

    details_df = pd.DataFrame(detail_rows)
    if details_df.empty:
        return pd.DataFrame()

    for column in ("spend", "impressions", "clicks", "leads"):
        details_df[column] = pd.to_numeric(details_df.get(column, 0), errors="coerce").fillna(0)
    details_df[level_column] = details_df.get(level_column, "N/A").fillna("N/A").replace("", "N/A")
    details_df["campaign_source"] = details_df.get("campaign_source", "N/A").fillna("N/A").replace("", "N/A")

    grouped = (
        details_df.groupby(["campaign_source", level_column], as_index=False)[["spend", "impressions", "clicks", "leads"]]
        .sum()
        .sort_values("spend", ascending=False)
    )

    grouped["avg_click_to_leads_pct"] = grouped.apply(
        lambda row: round((float(row["leads"]) / float(row["clicks"])) * 100, 2) if float(row["clicks"]) else 0.0,
        axis=1,
    )
    grouped["avg_ctr_pct"] = grouped.apply(
        lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
        if float(row["impressions"])
        else 0.0,
        axis=1,
    )
    grouped["cpc"] = grouped.apply(
        lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
        axis=1,
    )
    grouped["cpm"] = grouped.apply(
        lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2)
        if float(row["impressions"])
        else 0.0,
        axis=1,
    )
    grouped["cost_per_leads"] = grouped.apply(
        lambda row: round(float(row["spend"]) / float(row["leads"])) if float(row["leads"]) else 0.0,
        axis=1,
    )
    return grouped


def _format_performance_display(df: pd.DataFrame, level_label: str) -> pd.DataFrame:
    """Format aggregated dataframe values for UI readability.

    Args:
        df (pd.DataFrame): Aggregated dataframe before display formatting.
        level_label (str): Active grouping label used as display column name.

    Returns:
        pd.DataFrame: Display-ready dataframe with currency, separator, and
        percentage formatting applied.
    """
    if df.empty:
        return df

    formatted = df.copy()
    if level_label in formatted.columns:
        formatted[level_label] = formatted[level_label].astype(str).apply(
            lambda value: textwrap.fill(value, width=46, break_long_words=False)
        )

    for col in ("Cost", "CPC", "CPM", "Cost/Leads", "Cost per Leads"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"Rp {float(v):,.0f}")
    for col in ("Impressions", "Clicks", "Leads"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"{int(float(v)):,}")
    for col in ("Click->Leads %", "Avg. Click to Leads", "Avg. CTR"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"{float(v):,.2f}%")

    return formatted


async def show_user_acquisition_page(host: str) -> None:
    """Render User Acquisition dashboard page and keep payload cache in sync.

    Args:
        host (str): API base URL used for protected backend requests.

    Returns:
        None: Renders Streamlit components as side effects.
    """
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">User Acquisition</div>', unsafe_allow_html=True)

    source_options = {
        "Google Ads": "google",
        "Facebook Ads": "facebook",
        "TikTok Ads": "tiktok",
    }
    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "campaign_ads_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]
    with st.container(border=True):
        filter_col, source_col = st.columns([2, 2], gap="small")
        with filter_col:
            period_key = st.selectbox(
                "Periods",
                options=list(presets.keys()),
                index=0,
                key="campaign_ads_period",
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

        with source_col:
            selected_source = st.selectbox(
                "Performance Source",
                options=list(source_options.keys()),
                index=0,
                key="campaign_ads_source",
            )

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_key = source_options[selected_source]
    cached_payload = st.session_state.get("campaign_ads_payload", {})
    cached_details = cached_payload.get("data", {}).get("ads_campaign_details", {})
    cached_selected_details = cached_details.get(selected_key, {}) if isinstance(cached_details, dict) else {}
    has_rows_schema = isinstance(cached_selected_details.get("rows"), list)
    selected_range = (start_date, end_date)

    should_fetch = (
        "campaign_ads_payload" not in st.session_state
        or not has_rows_schema
        or st.session_state.get("campaign_ads_range") != selected_range
    )

    if should_fetch:
        token_data = get_user(st.session_state._user_id)
        if token_data is None or not getattr(token_data, "access_token", None):
            st.error("Session invalid. Please log in again.")
            return

        with st.spinner("Fetching data..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="campaign/user-acquisition",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )

        if not isinstance(response, dict):
            st.error("Invalid response from campaign overview endpoint.")
            return
        if not response.get("success", False):
            st.error(response.get("detail") or response.get("message") or "Failed to fetch campaign overview.")
            return

        st.session_state["campaign_ads_payload"] = response
        st.session_state["campaign_ads_range"] = (start_date, end_date)

    payload = st.session_state.get("campaign_ads_payload", {})
    overview_data = payload.get("data", {})
    ads_metrics = overview_data.get("ads_metrics_with_growth", {})
    leads_performance_charts = overview_data.get("leads_performance_charts", {})
    ads_campaign_details = overview_data.get("ads_campaign_details", {})
    ua_insight_charts = overview_data.get("ua_insight_charts", {})

    selected_metrics = ads_metrics.get(selected_key, {})
    render_campaign_metric_cards(st, selected_metrics, selected_source)

    selected_charts = leads_performance_charts.get(selected_key, {})
    cost_to_leads_payload = selected_charts.get("cost_to_leads", {}).get("figure")
    leads_by_periods_payload = selected_charts.get("leads_by_periods", {}).get("figure")
    clicks_to_leads_payload = selected_charts.get("clicks_to_leads", {}).get("figure")

    cost_to_leads_figure = campaign_figure_from_payload(
        cost_to_leads_payload,
        f"Cost To Leads - {selected_source}",
    )
    leads_by_periods_figure = campaign_figure_from_payload(
        leads_by_periods_payload,
        f"Leads By Periods - {selected_source}",
    )
    clicks_to_leads_figure = campaign_figure_from_payload(
        clicks_to_leads_payload,
        f"Clicks To Leads - {selected_source}",
    )
    cost_to_leads_figure = _set_transparent_chart_background(cost_to_leads_figure)
    leads_by_periods_figure = _set_transparent_chart_background(leads_by_periods_figure)
    clicks_to_leads_figure = _set_transparent_chart_background(clicks_to_leads_figure)
    cost_to_leads_figure.update_layout(height=430)
    leads_by_periods_figure.update_layout(height=430)
    clicks_to_leads_figure.update_layout(height=430)

    chart_left, chart_mid, chart_right = st.columns(3, gap="small")
    with chart_left:
        with st.container(border=True):
            st.plotly_chart(cost_to_leads_figure, width="stretch")
    with chart_mid:
        with st.container(border=True):
            st.plotly_chart(clicks_to_leads_figure, width="stretch")
    with chart_right:
        with st.container(border=True):
            st.plotly_chart(leads_by_periods_figure, width="stretch")

    selected_details = ads_campaign_details.get(selected_key, {})
    detail_rows = selected_details.get("rows", [])
    level_options = {
        "Ad Campaign Performance": ("campaign_id", "Campaign ID"),
        "Ad group Performance": ("ad_group", "Ad Group"),
        "Ad Name Performance": ("ad_name", "Ad Name"),
    }
    selected_level = st.selectbox(
        "Performance Table",
        options=list(level_options.keys()),
        key="campaign_performance_level",
    )
    level_column, level_label = level_options[selected_level]
    performance_df = _build_performance_dataframe(detail_rows=detail_rows, level_column=level_column)

    st.markdown(f"### {selected_level}")
    if performance_df.empty:
        st.info("No campaign data for selected date range.")
        return

    selected_insight_source = ua_insight_charts.get("spend_vs_leads", {}).get(selected_key, {})
    scatter_payload = selected_insight_source.get(level_column, {}).get("figure")
    top_payload = (
        ua_insight_charts.get("top_leads", {})
        .get(selected_key, {})
        .get(level_column, {})
        .get("figure")
    )
    top_n = int(ua_insight_charts.get("top_n", 10) or 10)
    ratio_trends_payload = (
        ua_insight_charts.get("ratio_trends", {})
        .get(selected_key, {})
        .get(level_column, {})
    )
    cpl_payload = ratio_trends_payload.get("cost_per_lead", {}).get("figure")
    click_per_lead_payload = ratio_trends_payload.get("click_per_lead", {}).get("figure")
    ctl_payload = ratio_trends_payload.get("click_through_lead", {}).get("figure")
    cpl_figure = campaign_figure_from_payload(
        cpl_payload,
        f"{selected_source} Cost per Lead Trend",
    )
    click_per_lead_figure = campaign_figure_from_payload(
        click_per_lead_payload,
        f"{selected_source} Click per Lead Trend",
    )
    ctl_figure = campaign_figure_from_payload(
        ctl_payload,
        f"{selected_source} Click Through Lead Trend",
    )
    cpl_figure = _set_transparent_chart_background(cpl_figure)
    click_per_lead_figure = _set_transparent_chart_background(click_per_lead_figure)
    ctl_figure = _set_transparent_chart_background(ctl_figure)
    cpl_figure.update_layout(height=390)
    click_per_lead_figure.update_layout(height=390)
    ctl_figure.update_layout(height=390)

    scatter_figure = campaign_figure_from_payload(
        scatter_payload,
        f"{selected_source} Spend vs Leads",
    )
    top_n_figure = campaign_figure_from_payload(
        top_payload,
        f"Top {top_n} {selected_source} by Leads",
    )
    scatter_figure = _set_transparent_chart_background(scatter_figure)
    top_n_figure = _set_transparent_chart_background(top_n_figure)
    scatter_figure.update_layout(height=430)
    top_n_figure.update_layout(height=430)

    st.markdown("### Campaign Insights")
    trend_left, trend_mid, trend_right = st.columns(3, gap="small")
    with trend_left:
        with st.container(border=True):
            st.plotly_chart(cpl_figure, width="stretch")
    with trend_mid:
        with st.container(border=True):
            st.plotly_chart(click_per_lead_figure, width="stretch")
    with trend_right:
        with st.container(border=True):
            st.plotly_chart(ctl_figure, width="stretch")

    insight_left, insight_right = st.columns(2, gap="small")
    with insight_left:
        with st.container(border=True):
            st.plotly_chart(scatter_figure, width="stretch")
    with insight_right:
        with st.container(border=True):
            st.plotly_chart(top_n_figure, width="stretch")

    cumulative_payload = (
        ua_insight_charts.get("cumulative", {})
        .get(selected_key, {})
        .get(level_column, {})
        .get("figure")
    )
    daily_mix_payload = (
        ua_insight_charts.get("daily_mix", {})
        .get(selected_key, {})
        .get(level_column, {})
        .get("figure")
    )
    cumulative_figure = campaign_figure_from_payload(
        cumulative_payload,
        f"{selected_source} Cumulative Leads vs Spend",
    )
    daily_mix_figure = campaign_figure_from_payload(
        daily_mix_payload,
        "Daily Mix (UA Leads by Source)",
    )
    cumulative_figure = _set_transparent_chart_background(cumulative_figure)
    daily_mix_figure = _set_transparent_chart_background(daily_mix_figure)
    cumulative_figure.update_layout(height=430)
    daily_mix_figure.update_layout(height=430)

    st.markdown("### Daily Pacing Insights")
    pacing_left, pacing_right = st.columns(2, gap="small")
    with pacing_left:
        with st.container(border=True):
            st.plotly_chart(cumulative_figure, width="stretch")
    with pacing_right:
        with st.container(border=True):
            st.plotly_chart(daily_mix_figure, width="stretch")

    display_df = performance_df.rename(
        columns={
            "campaign_source": "Ads Source",
            level_column: level_label,
            "spend": "Cost",
            "impressions": "Impressions",
            "clicks": "Clicks",
            "leads": "Leads",
            "avg_click_to_leads_pct": "Click->Leads %",
            "avg_ctr_pct": "Avg. CTR",
            "cpc": "CPC",
            "cpm": "CPM",
            "cost_per_leads": "Cost/Leads",
        }
    )
    display_columns = [
        "Ads Source",
        level_label,
        "Cost",
        "Impressions",
        "Clicks",
        "Leads",
        "Click->Leads %",
        "Avg. CTR",
        "CPC",
        "CPM",
        "Cost/Leads",
    ]
    display_df = display_df[[column for column in display_columns if column in display_df.columns]]
    display_df = _format_performance_display(display_df, level_label)

    with st.container(border=True):
        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
            row_height=58,
            column_config={
                "Ads Source": st.column_config.TextColumn("Ads Source", width="small"),
                level_label: st.column_config.TextColumn(level_label, width="large"),
                "Cost": st.column_config.TextColumn("Cost", width="small"),
                "Impressions": st.column_config.TextColumn("Impressions", width="small"),
                "Clicks": st.column_config.TextColumn("Clicks", width="small"),
                "Leads": st.column_config.TextColumn("Leads", width="small"),
                "Click->Leads %": st.column_config.TextColumn("Click->Leads %", width="small"),
                "Avg. CTR": st.column_config.TextColumn("Avg. CTR", width="small"),
                "CPC": st.column_config.TextColumn("CPC", width="small"),
                "CPM": st.column_config.TextColumn("CPM", width="small"),
                "Cost/Leads": st.column_config.TextColumn("Cost/Leads", width="medium"),
            },
        )


async def show_campaign_ads_page(host: str) -> None:
    """Backward-compatible alias for legacy page handler name.

    Args:
        host (str): API base URL used by underlying page renderer.

    Returns:
        None: Delegates rendering to ``show_user_acquisition_page``.
    """
    await show_user_acquisition_page(host)
