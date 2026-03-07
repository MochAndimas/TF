"""Brand Awareness module.

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
    render_brand_awareness_metric_cards,
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
    """Apply transparent background style to Plotly chart.

    Args:
        figure: Plotly figure object.

    Returns:
        object: Styled figure with transparent paper and plot backgrounds.
    """
    figure.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_brand_awareness_performance_dataframe(detail_rows: list[dict], level_column: str) -> pd.DataFrame:
    """Aggregate Brand Awareness detail rows by selected performance level.

    Args:
        detail_rows (list[dict]): Raw detail rows from BA overview payload.
        level_column (str): Grouping key (`campaign_id`, `ad_group`, `ad_name`).

    Returns:
        pd.DataFrame: Aggregated dataframe with derived CTR/CPM/CPC metrics.
    """
    if not detail_rows:
        return pd.DataFrame()

    details_df = pd.DataFrame(detail_rows)
    if details_df.empty:
        return pd.DataFrame()

    for column in ("spend", "impressions", "clicks"):
        details_df[column] = pd.to_numeric(details_df.get(column, 0), errors="coerce").fillna(0)
    details_df[level_column] = details_df.get(level_column, "N/A").fillna("N/A").replace("", "N/A")
    details_df["campaign_source"] = details_df.get("campaign_source", "N/A").fillna("N/A").replace("", "N/A")

    grouped = (
        details_df.groupby(["campaign_source", level_column], as_index=False)[["spend", "impressions", "clicks"]]
        .sum()
        .sort_values("spend", ascending=False)
    )
    grouped["ctr"] = grouped.apply(
        lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
        if float(row["impressions"])
        else 0.0,
        axis=1,
    )
    grouped["cpm"] = grouped.apply(
        lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2)
        if float(row["impressions"])
        else 0.0,
        axis=1,
    )
    grouped["cpc"] = grouped.apply(
        lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
        axis=1,
    )
    return grouped


def _format_brand_awareness_display(df: pd.DataFrame, level_label: str) -> pd.DataFrame:
    """Format Brand Awareness dataframe values for table display.

    Args:
        df (pd.DataFrame): Aggregated dataframe before text formatting.
        level_label (str): Selected grouping label column name.

    Returns:
        pd.DataFrame: Display-ready dataframe with formatted currency,
        percentages, and thousand separators.
    """
    if df.empty:
        return df

    formatted = df.copy()
    if level_label in formatted.columns:
        formatted[level_label] = formatted[level_label].astype(str).apply(
            lambda value: textwrap.fill(value, width=46, break_long_words=False)
        )

    for col in ("Cost", "CPM", "CPC"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"Rp {float(v):,.0f}")
    for col in ("Impressions", "Clicks"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"{int(float(v)):,}")
    if "CTR" in formatted.columns:
        formatted["CTR"] = formatted["CTR"].apply(lambda v: f"{float(v):,.2f}%")
    return formatted


async def show_brand_awareness_page(host: str) -> None:
    """Render Brand Awareness dashboard page with cached API payload strategy.

    Args:
        host (str): API base URL used to request overview payload.

    Returns:
        None: Renders Streamlit components as side effects.
    """
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Brand Awareness</div>', unsafe_allow_html=True)

    source_options = {
        "Google Ads": "google",
        "Facebook Ads": "facebook",
        "TikTok Ads": "tiktok",
    }
    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "brand_awareness_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]
    with st.container(border=True):
        filter_col, source_col = st.columns([2, 2], gap="small")
        with filter_col:
            period_key = st.selectbox(
                "Periods",
                options=list(presets.keys()),
                index=0,
                key="brand_awareness_period",
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
                key="brand_awareness_source",
            )

    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_key = source_options[selected_source]
    cached_payload = st.session_state.get("brand_awareness_payload", {})
    cached_data = cached_payload.get("data", {})
    cached_charts = cached_data.get("charts", {})
    cached_selected = cached_charts.get(selected_key, {}) if isinstance(cached_charts, dict) else {}
    has_schema = isinstance(cached_selected.get("spend", {}).get("rows"), list)
    selected_range = (start_date, end_date)

    should_fetch = (
        "brand_awareness_payload" not in st.session_state
        or not has_schema
        or st.session_state.get("brand_awareness_range") != selected_range
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
                uri="campaign/brand-awareness",
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

        st.session_state["brand_awareness_payload"] = response
        st.session_state["brand_awareness_range"] = (start_date, end_date)

    payload = st.session_state.get("brand_awareness_payload", {})
    overview_data = payload.get("data", {})
    metrics_with_growth = overview_data.get("metrics_with_growth", {})
    chart_payloads = overview_data.get("charts", {})
    detail_payloads = overview_data.get("details", {})
    insight_charts = overview_data.get("insight_charts", {})

    selected_metrics = metrics_with_growth.get(selected_key, {})
    render_brand_awareness_metric_cards(st, selected_metrics, selected_source)

    selected_charts = chart_payloads.get(selected_key, {})
    spend_payload = selected_charts.get("spend", {}).get("figure")
    performance_payload = selected_charts.get("performance", {}).get("figure")

    spend_figure = campaign_figure_from_payload(spend_payload, f"{selected_source} - Brand Awareness Spend")
    performance_figure = campaign_figure_from_payload(
        performance_payload,
        f"{selected_source} - Brand Awareness Performance",
    )
    spend_figure = _set_transparent_chart_background(spend_figure)
    performance_figure = _set_transparent_chart_background(performance_figure)
    spend_figure.update_layout(height=540)
    performance_figure.update_layout(height=540)

    left_col, right_col = st.columns(2, gap="small")
    with left_col:
        with st.container(border=True):
            st.plotly_chart(spend_figure, width="stretch")
    with right_col:
        with st.container(border=True):
            st.plotly_chart(performance_figure, width="stretch")

    selected_details = detail_payloads.get(selected_key, {})
    detail_rows = selected_details.get("rows", [])
    level_options = {
        "Ad Campaign Performance": ("campaign_id", "Campaign ID"),
        "Ad Group Performance": ("ad_group", "Ad Group"),
        "Ad Name Performance": ("ad_name", "Ad Name"),
    }
    selected_level = st.selectbox(
        "Performance Table",
        options=list(level_options.keys()),
        key="brand_awareness_performance_level",
    )
    level_column, level_label = level_options[selected_level]
    performance_df = _build_brand_awareness_performance_dataframe(detail_rows=detail_rows, level_column=level_column)

    st.markdown(f"### {selected_level}")
    if performance_df.empty:
        st.info("No brand awareness data for selected date range.")
        return

    ratio_trends_payload = (
        insight_charts.get("ratio_trends", {})
        .get(selected_key, {})
        .get(level_column, {})
    )
    ctr_payload = ratio_trends_payload.get("ctr", {}).get("figure")
    cpm_payload = ratio_trends_payload.get("cpm", {}).get("figure")
    cpc_payload = ratio_trends_payload.get("cpc", {}).get("figure")

    ctr_figure = campaign_figure_from_payload(
        ctr_payload,
        f"{selected_source} CTR Trend",
    )
    cpm_figure = campaign_figure_from_payload(
        cpm_payload,
        f"{selected_source} CPM Trend",
    )
    cpc_figure = campaign_figure_from_payload(
        cpc_payload,
        f"{selected_source} CPC Trend",
    )
    ctr_figure = _set_transparent_chart_background(ctr_figure)
    cpm_figure = _set_transparent_chart_background(cpm_figure)
    cpc_figure = _set_transparent_chart_background(cpc_figure)
    ctr_figure.update_layout(height=390)
    cpm_figure.update_layout(height=390)
    cpc_figure.update_layout(height=390)

    st.markdown("### Campaign Insights")
    trend_left, trend_mid, trend_right = st.columns(3, gap="small")
    with trend_left:
        with st.container(border=True):
            st.plotly_chart(ctr_figure, width="stretch")
    with trend_mid:
        with st.container(border=True):
            st.plotly_chart(cpm_figure, width="stretch")
    with trend_right:
        with st.container(border=True):
            st.plotly_chart(cpc_figure, width="stretch")

    display_df = performance_df.rename(
        columns={
            "campaign_source": "Ads Source",
            level_column: level_label,
            "spend": "Cost",
            "impressions": "Impressions",
            "clicks": "Clicks",
            "ctr": "CTR",
            "cpm": "CPM",
            "cpc": "CPC",
        }
    )
    display_columns = [
        "Ads Source",
        level_label,
        "Cost",
        "Impressions",
        "Clicks",
        "CTR",
        "CPM",
        "CPC",
    ]
    display_df = display_df[[column for column in display_columns if column in display_df.columns]]
    display_df = _format_brand_awareness_display(display_df, level_label)

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
                "CTR": st.column_config.TextColumn("CTR", width="small"),
                "CPM": st.column_config.TextColumn("CPM", width="small"),
                "CPC": st.column_config.TextColumn("CPC", width="small"),
            },
        )
