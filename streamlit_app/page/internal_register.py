"""Streamlit page for internal register analytics."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.page.activity_components.api import fetch_legacy_activity_payload
from streamlit_app.page.campaign_components.common import PAGE_STYLE, set_transparent_chart_background


TAG_MIX_PANEL_HEIGHT = 400


def _render_filters() -> tuple[dt.date | None, dt.date | None, str | None]:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Register</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    if "internal_register_date_range" not in st.session_state:
        st.session_state["internal_register_date_range"] = presets["This Month"]
    if "internal_register_period" not in st.session_state:
        st.session_state["internal_register_period"] = "This Month"

    with st.container(border=True):
        selected_period = st.selectbox("Periods", options=list(presets.keys()), key="internal_register_period")
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
        st.segmented_control(
            "View",
            options=["All Source", "Campaign"],
            default="All Source",
            key="internal_register_view_v2",
        )

    selected_view = st.session_state.get("internal_register_view_v2", "All Source")
    st.markdown(f"## {selected_view}")
    return start_date, end_date, selected_view


async def _render_all_source(host: str, start_date, end_date) -> None:
    """Render All Source with the same card-and-panel layout as Campaign."""
    response = await fetch_legacy_activity_payload(
        host=host, uri="campaign/all-source-register", start_date=start_date, end_date=end_date,
        source="all", fallback_message="Failed to fetch all source register data.",
    )
    if response is None:
        return
    data = response.get("data", {})
    metrics = data.get("metrics", {})
    cols = st.columns(4, gap="small")
    metric_specs = [
        ("Total Register", "total_register", _fmt_int(metrics.get("total_register"))),
        ("Avg Daily", "avg_daily_register", _fmt_float(metrics.get("avg_daily_register"))),
        ("Active Sources", "active_sources", _fmt_int(metrics.get("active_sources"))),
        ("Peak Day Register", "peak_day_register", _fmt_int(metrics.get("peak_day_register"))),
    ]
    for column, (label, _key, value) in zip(cols, metric_specs):
        with column:
            with st.container(border=True):
                st.metric(label, value, help=f"Peak day: {metrics.get('peak_day') or '-'}" if label == "Peak Day Register" else None)
    daily = pd.DataFrame(data.get("daily_rows", []))
    sources = pd.DataFrame(data.get("source_rows", [])).head(10)
    details = pd.DataFrame(data.get("details", []))
    daily_figure = go.Figure()
    if not details.empty:
        top_sources = sources["source"].tolist()
        palette = ["#4C78FF", "#67A3FF", "#2DD4BF", "#A78BFA", "#FBBF24", "#FB7185", "#34D399", "#F97316", "#22D3EE", "#C084FC"]
        for index, source in enumerate(top_sources):
            subset = details[details["source"] == source].groupby("date", as_index=False)["value"].sum()
            daily_figure.add_trace(
                go.Scatter(
                    x=subset["date"], y=subset["value"], mode="lines+markers",
                    name=source, line=dict(color=palette[index], width=2),
                )
            )
    daily_figure.update_layout(title="Daily Register by Top 10 Sources", xaxis_title="Date", yaxis_title="Register")
    daily_figure = set_transparent_chart_background(daily_figure)
    daily_figure.update_layout(height=440)

    source_figure = go.Figure()
    if not sources.empty:
        source_figure.add_trace(
            go.Bar(x=sources["source"], y=sources["value"], name="Register", marker_color="#4C78FF"))
    source_figure.update_layout(title="Top 10 Register by Source", xaxis_title="Source", yaxis_title="Register")
    source_figure = set_transparent_chart_background(source_figure)
    source_figure.update_layout(height=440)
    left, right = st.columns(2, gap="small")
    with left:
        with st.container(border=True):
            if daily.empty:
                st.info("No All Source data for selected date range.")
            else:
                st.plotly_chart(daily_figure, width="stretch")
    with right:
        with st.container(border=True):
            if sources.empty:
                st.info("No source data for selected date range.")
            else:
                st.plotly_chart(source_figure, width="stretch")
    with st.container(border=True):
        if details.empty:
            st.info("No All Source data for selected date range.")
        else:
            available_sources = (
                details.groupby("source", as_index=False)["value"]
                .sum()
                .sort_values("value", ascending=False)["source"]
                .tolist()
            )
            top_sources = available_sources[:10]
            heatmap_view = st.selectbox(
                "Heatmap source",
                options=["Top 10 Sources", *available_sources],
                key="all_source_heatmap_source",
            )
            selected_sources = top_sources if heatmap_view == "Top 10 Sources" else [heatmap_view]
            heatmap_data = (
                details[details["source"].isin(selected_sources)]
                .pivot_table(index="source", columns="date", values="value", aggfunc="sum", fill_value=0)
                .reindex(selected_sources)
            )
            heatmap = go.Figure(
                data=go.Heatmap(
                    z=heatmap_data.values,
                    x=heatmap_data.columns,
                    y=heatmap_data.index,
                    colorscale="Blues",
                    colorbar=dict(title="Register"),
                )
            )
            heatmap.update_layout(
                title=f"Daily Register Heatmap — {heatmap_view}",
                xaxis_title="Date",
                yaxis_title="Source",
                height=360,
            )
            st.plotly_chart(set_transparent_chart_background(heatmap), width="stretch")
    with st.container(border=True):
        st.markdown("### Register by Source Details")
        source_details = pd.DataFrame(data.get("details", []))
        if source_details.empty:
            st.info("No source data for selected date range.")
        else:
            source_details = (
                source_details.groupby("source", as_index=False)["value"]
                .sum()
                .sort_values("value", ascending=False)
                .rename(columns={"source": "Source", "value": "Register"})
            )
            st.dataframe(
                source_details,
                width="stretch",
                hide_index=True,
                column_config={"Register": st.column_config.NumberColumn("Register", format="%d")},
            )


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


def _tag_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Tag Name", "Total Register"])
    df = pd.DataFrame(rows)
    df = df.rename(columns={"tag_name": "Tag Name", "total_regis": "Total Register"})
    if "Total Register" in df.columns:
        df["Total Register"] = pd.to_numeric(df["Total Register"], errors="coerce").fillna(0).astype(int)
    return df[[column for column in ["Tag Name", "Total Register"] if column in df.columns]]


def _render_tag_mix(tag_rows: list[dict[str, object]], tag_figure) -> None:
    tag_df = _tag_dataframe(tag_rows)
    table_col, chart_col = st.columns(2, gap="small")
    with table_col:
        with st.container(border=True, height=TAG_MIX_PANEL_HEIGHT):
            st.markdown("### Register by Tag")
            if tag_df.empty:
                st.info("No tag data for selected date range.")
            else:
                st.dataframe(
                    tag_df,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Tag Name": st.column_config.TextColumn("Tag Name", width="medium"),
                        "Total Register": st.column_config.NumberColumn("Total Register", format="%d"),
                    },
                )
    with chart_col:
        with st.container(border=True, height=TAG_MIX_PANEL_HEIGHT):
            st.plotly_chart(tag_figure, width="stretch")


async def show_internal_register_page(host: str) -> None:
    start_date, end_date, selected_view = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    if selected_view == "All Source":
        await _render_all_source(host, start_date, end_date)
        return

    source_key = "all"
    selected_range = (start_date, end_date, source_key)
    cached_payload = st.session_state.get("internal_register_payload", {})
    cached_charts = cached_payload.get("data", {}).get("charts", {}) if isinstance(cached_payload, dict) else {}
    should_fetch = (
        "internal_register_payload" not in st.session_state
        or st.session_state.get("internal_register_range") != selected_range
        or "tag_mix" not in cached_charts
    )

    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching data..."):
            response = await fetch_legacy_activity_payload(
                host=host,
                uri="campaign/internal-register",
                start_date=start_date,
                end_date=end_date,
                source=source_key,
                fallback_message="Failed to fetch internal register.",
            )
        if response is None:
            return
        st.session_state["internal_register_payload"] = response
        st.session_state["internal_register_range"] = selected_range

    data = st.session_state.get("internal_register_payload", {}).get("data", {})
    _render_metrics(data.get("metrics", {}))

    charts = data.get("charts", {})
    tag_mix = charts.get("tag_mix", {})
    tag_figure = set_transparent_chart_background(campaign_figure_from_payload(tag_mix.get("figure"), "Register by Tag"))
    daily_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("daily_trend", {}).get("figure"), "Daily Internal Register"))
    cumulative_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("cumulative_trend", {}).get("figure"), "Cumulative Register by Campaign"))
    source_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("source_mix", {}).get("figure"), "Register by Source"))
    type_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("type_mix", {}).get("figure"), "Register by Campaign Type"))
    top_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("top_campaigns", {}).get("figure"), "Top Campaigns by Register"))
    heatmap_figure = set_transparent_chart_background(campaign_figure_from_payload(charts.get("campaign_heatmap", {}).get("figure"), "Daily Register Heatmap"))

    tag_figure.update_layout(height=360)
    daily_figure.update_layout(height=440)
    cumulative_figure.update_layout(height=440)
    source_figure.update_layout(height=440)
    type_figure.update_layout(height=440)
    top_figure.update_layout(height=520)
    heatmap_figure.update_layout(height=560)

    _render_tag_mix(tag_mix.get("rows", []), tag_figure)

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
