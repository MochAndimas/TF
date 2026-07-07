"""Streamlit page for TikTok social media analytics."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.page.campaign_components.common import PAGE_STYLE
from streamlit_app.page.socmed_components.api import fetch_legacy_socmed_payload


def _render_filters() -> tuple[dt.date | None, dt.date | None]:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">TikTok</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "tiktok_analytics_date_range"
    period_key = "tiktok_analytics_period"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["This Month"]
    if period_key not in st.session_state:
        st.session_state[period_key] = "This Month"

    with st.container(border=True):
        selected_period = st.selectbox(
            "Periods",
            options=list(presets.keys()),
            key=period_key,
        )
        if selected_period == "Custom Range":
            selected = st.date_input("Select Date Range", key=date_range_key)
            if not isinstance(selected, tuple) or len(selected) != 2:
                st.warning("Please select a valid date range.")
                return None, None
            return selected

        start_date, end_date = presets[selected_period]
        if st.session_state.get(date_range_key) != (start_date, end_date):
            st.session_state[date_range_key] = (start_date, end_date)
        return start_date, end_date


def _fmt_int(value) -> str:
    return f"{int(float(value or 0)):,.0f}"


def _fmt_float(value) -> str:
    return f"{float(value or 0):,.2f}"


def _fmt_pct(value) -> str:
    return f"{float(value or 0):,.2f}%"


def _fmt_duration(seconds) -> str:
    total_seconds = int(float(seconds or 0))
    minutes, remaining_seconds = divmod(total_seconds, 60)
    if minutes >= 60:
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m {remaining_seconds}s"
    return f"{minutes}m {remaining_seconds}s"


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in [
        "followers_snapshot",
        "total_likes",
        "video_count",
        "views",
        "likes",
        "comments",
        "shares",
        "engagement",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    return df.sort_values("date")


def _media_daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in ["video_count", "views", "likes", "comments", "shares", "engagement"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    return df.sort_values("date")


def _media_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    for column in ["duration", "views", "likes", "comments", "shares", "engagement"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    for column in ["video_id", "description", "permalink", "cover_image_url"]:
        df[column] = df[column].fillna("").astype(str)
    return df.sort_values(["views", "engagement", "date"], ascending=[False, False, False])


def _render_metrics(metrics: dict[str, object]) -> None:
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    specs = [
        ("Followers Snapshot", "followers_snapshot", _fmt_int(current.get("followers_snapshot"))),
        ("Total Likes", "total_likes", _fmt_int(current.get("total_likes"))),
        ("Video Count", "video_count", _fmt_int(current.get("video_count"))),
        ("Views / Plays", "views", _fmt_int(current.get("views"))),
        ("Engagement", "engagement", _fmt_int(current.get("engagement"))),
        ("Engagement Rate", "engagement_rate", _fmt_pct(current.get("engagement_rate"))),
        ("Likes", "likes", _fmt_int(current.get("likes"))),
        ("Comments", "comments", _fmt_int(current.get("comments"))),
        ("Shares", "shares", _fmt_int(current.get("shares"))),
    ]
    for row_start in range(0, len(specs), 3):
        row_specs = specs[row_start : row_start + 3]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, key, value) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    growth_value = growth.get(key, 0.0)
                    st.metric(
                        label,
                        value,
                        delta=_campaign_format_growth(growth_value),
                        delta_color="off" if growth_value == 0 else "normal",
                    )


def _build_followers_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Followers Snapshot")
        return figure
    labels = _date_labels(df["date"])
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["followers_snapshot"],
            name="Followers",
            mode="lines+markers",
            hovertemplate="<b>%{x}</b><br>Followers: %{y:,}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["video_count"],
            name="Video Count",
            mode="lines+markers",
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Video Count: %{y:,}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Account Snapshot",
        xaxis_title="Date",
        yaxis_title="Followers",
        yaxis2=dict(title="Video Count", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_account_performance_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Account Performance Snapshot")
        return figure
    labels = _date_labels(df["date"])
    for column, label in [("views", "Views / Plays"), ("engagement", "Engagement")]:
        figure.add_trace(
            go.Scatter(
                x=labels,
                y=df[column],
                name=label,
                mode="lines+markers",
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.update_layout(
        title="Account Performance Snapshot",
        xaxis_title="Date",
        yaxis_title="Lifetime Count",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_media_daily_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Media Views by Upload Date")
        return figure
    labels = _date_labels(df["date"])
    figure.add_trace(
        go.Bar(
            x=labels,
            y=df["views"],
            name="Views / Plays",
            customdata=df[["video_count", "engagement", "engagement_rate"]].values,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Views: %{y:,}<br>"
                "Videos: %{customdata[0]:,}<br>"
                "Engagement: %{customdata[1]:,}<br>"
                "Engagement Rate: %{customdata[2]:.2f}%<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title="Media Views by Upload Date",
        xaxis_title="Upload Date",
        yaxis_title="Views / Plays",
        xaxis=dict(type="category"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_top_media_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Top 5 TikTok Videos")
        return figure
    top_df = df.head(5).sort_values("views", ascending=True).copy()
    text = top_df["description"].str.replace(r"\s+", " ", regex=True).str.strip()
    text = text.where(text != "", top_df["video_id"].astype(str))
    top_df["label"] = pd.to_datetime(top_df["date"]).dt.strftime("%b %d") + " | " + text.str.slice(0, 58)
    for column, label in [("likes", "Likes"), ("comments", "Comments"), ("shares", "Shares")]:
        figure.add_trace(
            go.Bar(
                y=top_df["label"],
                x=top_df[column],
                name=label,
                orientation="h",
                customdata=top_df[["views", "engagement", "engagement_rate"]].values,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + f"{label}: %{{x:,}}<br>"
                    + "Views: %{customdata[0]:,}<br>"
                    + "Engagement: %{customdata[1]:,}<br>"
                    + "Engagement Rate: %{customdata[2]:.2f}%<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Top 5 TikTok Videos by Views",
        xaxis_title="Engagement",
        barmode="stack",
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=18, r=18, t=70, b=36),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _render_daily_table(df: pd.DataFrame) -> None:
    st.markdown("### Daily TikTok Account Snapshot")
    if df.empty:
        st.info("No TikTok account snapshot data for selected date range.")
        return
    display_df = df[
        [
            "date",
            "followers_snapshot",
            "total_likes",
            "video_count",
            "views",
            "likes",
            "comments",
            "shares",
            "engagement",
            "engagement_rate",
        ]
    ].rename(
        columns={
            "date": "Date",
            "followers_snapshot": "Followers Snapshot",
            "total_likes": "Total Likes",
            "video_count": "Video Count",
            "views": "Views / Plays",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "engagement": "Engagement",
            "engagement_rate": "Engagement Rate",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
        },
    )


def _render_media_metrics(summary: dict[str, object]) -> None:
    specs = [
        ("Videos", _fmt_int(summary.get("video_count"))),
        ("Views / Plays", _fmt_int(summary.get("views"))),
        ("Media Engagement", _fmt_int(summary.get("engagement"))),
        ("Engagement Rate", _fmt_pct(summary.get("engagement_rate"))),
        ("Likes", _fmt_int(summary.get("likes"))),
        ("Comments", _fmt_int(summary.get("comments"))),
        ("Shares", _fmt_int(summary.get("shares"))),
        ("Avg Views / Video", _fmt_float(summary.get("avg_views_per_video"))),
        ("Avg Engagement / Video", _fmt_float(summary.get("avg_engagement_per_video"))),
    ]
    for row_start in range(0, len(specs), 3):
        row_specs = specs[row_start : row_start + 3]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, value) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _render_media_table(df: pd.DataFrame) -> None:
    st.markdown("### Top TikTok Videos")
    if df.empty:
        st.info("No TikTok video data for selected date range.")
        return

    display_df = df[
        [
            "date",
            "description",
            "duration",
            "views",
            "engagement",
            "engagement_rate",
            "likes",
            "comments",
            "shares",
            "permalink",
        ]
    ].copy()
    display_df["description"] = display_df["description"].str.slice(0, 160)
    display_df["duration"] = display_df["duration"].map(_fmt_duration)
    display_df = display_df.rename(
        columns={
            "date": "Upload Date",
            "description": "Video",
            "duration": "Duration",
            "views": "Views / Plays",
            "engagement": "Engagement",
            "engagement_rate": "Engagement Rate",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "permalink": "Permalink",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Upload Date": st.column_config.DateColumn("Upload Date"),
            "Video": st.column_config.TextColumn("Video", width="large"),
            "Permalink": st.column_config.LinkColumn("Permalink"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
        },
    )


async def show_tiktok_page(host: str) -> None:
    start_date, end_date = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_range = (start_date, end_date)
    should_fetch = (
        "tiktok_analytics_payload" not in st.session_state
        or st.session_state.get("tiktok_analytics_range") != selected_range
    )
    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching TikTok analytics..."):
            response = await fetch_legacy_socmed_payload(
                host=host,
                uri="tiktok/analytics",
                start_date=start_date,
                end_date=end_date,
                fallback_message="Failed to fetch TikTok analytics.",
            )
        if response is None:
            return
        st.session_state["tiktok_analytics_payload"] = response
        st.session_state["tiktok_analytics_range"] = selected_range

    payload = st.session_state.get("tiktok_analytics_payload", {}).get("data", {})
    daily_df = _daily_dataframe(payload.get("daily_rows", []))
    media_daily_df = _media_daily_dataframe(payload.get("media_daily_rows", []))
    media_df = _media_dataframe(payload.get("media_rows", []))

    _render_metrics(payload.get("metrics", {}))
    followers_figure = _build_followers_figure(daily_df)
    performance_figure = _build_account_performance_figure(daily_df)
    for column, figure in zip(st.columns(2, gap="small"), [followers_figure, performance_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    with st.container(border=True):
        _render_daily_table(daily_df)

    st.markdown("## Media Insights")
    _render_media_metrics(payload.get("media_summary", {}))
    media_daily_figure = _build_media_daily_figure(media_daily_df)
    top_media_figure = _build_top_media_figure(media_df)
    for column, figure in zip(st.columns(2, gap="small"), [media_daily_figure, top_media_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    with st.container(border=True):
        _render_media_table(media_df)
