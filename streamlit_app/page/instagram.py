"""Streamlit page for Instagram social media analytics."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_data
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.page.campaign_components.common import PAGE_STYLE


def _render_filters() -> tuple[dt.date | None, dt.date | None]:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Instagram</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "instagram_analytics_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        selected_period = st.selectbox(
            "Periods",
            options=list(presets.keys()),
            index=0,
            key="instagram_analytics_period",
        )
        if selected_period == "Custom Range":
            selected = st.date_input("Select Date Range", key=date_range_key)
            if not isinstance(selected, tuple) or len(selected) != 2:
                st.warning("Please select a valid date range.")
                return None, None
            start_date, end_date = selected
        else:
            start_date, end_date = presets[selected_period]
            if st.session_state.get(date_range_key) != (start_date, end_date):
                st.session_state[date_range_key] = (start_date, end_date)
    return start_date, end_date


def _fmt_int(value) -> str:
    return f"{int(float(value or 0)):,.0f}"


def _fmt_float(value) -> str:
    return f"{float(value or 0):,.2f}"


def _render_metrics(metrics: dict[str, object]) -> None:
    current_metrics = metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = metrics.get("growth_percentage", {})
    metric_specs = [
        ("Total Followers", "total_followers", _fmt_int(current_metrics.get("total_followers"))),
        ("New Followers", "new_followers", _fmt_int(current_metrics.get("new_followers"))),
        ("Total Engagement", "total_engagement", _fmt_int(current_metrics.get("total_engagement"))),
        ("Likes", "likes", _fmt_int(current_metrics.get("likes"))),
        ("Comments", "comments", _fmt_int(current_metrics.get("comments"))),
        ("Shares", "shares", _fmt_int(current_metrics.get("shares"))),
        ("Saves", "saves", _fmt_int(current_metrics.get("saves"))),
        ("Engagement / New Follower", "engagement_per_new_follower", _fmt_float(current_metrics.get("engagement_per_new_follower"))),
    ]
    for row_start in range(0, len(metric_specs), 4):
        columns = st.columns(4, gap="small")
        for column, (label, key, value) in zip(columns, metric_specs[row_start : row_start + 4]):
            with column:
                with st.container(border=True):
                    growth_value = growth_metrics.get(key, 0.0)
                    st.metric(
                        label,
                        value,
                        delta=_campaign_format_growth(growth_value),
                        delta_color="off" if growth_value == 0 else "normal",
                    )


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in [
        "total_followers",
        "new_followers",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


def _build_growth_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Followers Growth",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    labels = _date_labels(df["date"])
    figure.add_trace(
        go.Bar(
            x=labels,
            y=df["new_followers"],
            name="New Followers",
            text=[f"{value:,}" for value in df["new_followers"]],
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>New Followers: %{y:,}<extra></extra>",
        )
    )
    follower_snapshot = df.loc[df["total_followers"] > 0].copy()
    if not follower_snapshot.empty:
        figure.add_trace(
            go.Scatter(
                x=_date_labels(follower_snapshot["date"]),
                y=follower_snapshot["total_followers"],
                name="Total Followers Snapshot",
                mode="lines+markers",
                yaxis="y2",
                hovertemplate="<b>%{x}</b><br>Total Followers: %{y:,}<extra></extra>",
            )
        )
    figure.update_layout(
        title="Followers Growth",
        xaxis_title="Date",
        yaxis_title="New Followers",
        yaxis2=dict(title="Total Followers", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_engagement_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Daily Engagement",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    labels = _date_labels(df["date"])
    for column, label in [
        ("likes", "Likes"),
        ("comments", "Comments"),
        ("shares", "Shares"),
        ("saves", "Saves"),
    ]:
        figure.add_trace(
            go.Bar(
                x=labels,
                y=df[column],
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["total_engagement"],
            name="Total Engagement",
            mode="lines+markers",
            hovertemplate="<b>%{x}</b><br>Total Engagement: %{y:,}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Daily Engagement",
        xaxis_title="Date",
        yaxis_title="Engagement",
        barmode="stack",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_composition_figure(df: pd.DataFrame) -> go.Figure:
    totals = {
        "Likes": int(df["likes"].sum()) if not df.empty else 0,
        "Comments": int(df["comments"].sum()) if not df.empty else 0,
        "Shares": int(df["shares"].sum()) if not df.empty else 0,
        "Saves": int(df["saves"].sum()) if not df.empty else 0,
    }
    figure = go.Figure(
        data=[
            go.Pie(
                labels=list(totals.keys()),
                values=list(totals.values()),
                hole=0.48,
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Value: %{value:,}<extra></extra>",
            )
        ]
    )
    figure.update_layout(
        title="Engagement Mix",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.08, x=0),
    )
    return figure


def _render_daily_table(df: pd.DataFrame) -> None:
    st.markdown("### Daily Instagram Metrics")
    if df.empty:
        st.info("No Instagram data for selected date range.")
        return

    display_df = df.rename(
        columns={
            "date": "Date",
            "total_followers": "Total Followers",
            "new_followers": "New Followers",
            "total_engagement": "Total Engagement",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "saves": "Saves",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Total Followers": st.column_config.NumberColumn("Total Followers", format="%d"),
            "New Followers": st.column_config.NumberColumn("New Followers", format="%d"),
            "Total Engagement": st.column_config.NumberColumn("Total Engagement", format="%d"),
            "Likes": st.column_config.NumberColumn("Likes", format="%d"),
            "Comments": st.column_config.NumberColumn("Comments", format="%d"),
            "Shares": st.column_config.NumberColumn("Shares", format="%d"),
            "Saves": st.column_config.NumberColumn("Saves", format="%d"),
        },
    )


async def show_instagram_page(host: str) -> None:
    start_date, end_date = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_range = (start_date, end_date)
    should_fetch = (
        "instagram_analytics_payload" not in st.session_state
        or st.session_state.get("instagram_analytics_range") != selected_range
    )
    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching Instagram analytics..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="instagram/analytics",
                method="GET",
                params={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch Instagram analytics.")
            return
        st.session_state["instagram_analytics_payload"] = response
        st.session_state["instagram_analytics_range"] = selected_range

    payload = st.session_state.get("instagram_analytics_payload", {}).get("data", {})
    df = _daily_dataframe(payload.get("daily_rows", []))
    _render_metrics(payload.get("metrics", {}))

    growth_figure = _build_growth_figure(df)
    engagement_figure = _build_engagement_figure(df)
    growth_figure.update_layout(height=440)
    engagement_figure.update_layout(height=440)
    for column, figure in zip(st.columns(2, gap="small"), [growth_figure, engagement_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    composition_figure = _build_composition_figure(df)
    composition_figure.update_layout(height=380)
    with st.container(border=True):
        st.plotly_chart(composition_figure, width="stretch")

    with st.container(border=True):
        _render_daily_table(df)
