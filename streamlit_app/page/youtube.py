"""Streamlit page for YouTube social media analytics."""

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
    st.markdown('<div class="campaign-title">YouTube</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "youtube_analytics_date_range"
    if date_range_key not in st.session_state:
        st.session_state[date_range_key] = presets["Last 7 Day"]

    with st.container(border=True):
        selected_period = st.selectbox(
            "Periods",
            options=list(presets.keys()),
            index=0,
            key="youtube_analytics_period",
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
        "views",
        "subscribers_gained",
        "subscribers_lost",
        "net_subscribers",
        "likes",
        "comments",
        "shares",
        "total_engagement",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["watch_hours", "average_view_duration"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    return df.sort_values("date")


def _media_daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["content_type"] = df["content_type"].fillna("").astype(str)
    for column in [
        "video_count",
        "views",
        "likes",
        "comments",
        "shares",
        "subscribers_gained",
        "total_engagement",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["watch_hours"] = pd.to_numeric(df["watch_hours"], errors="coerce").fillna(0.0)
    return df.sort_values(["date", "content_type"])


def _media_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    for column in ["views", "likes", "comments", "shares", "subscribers_gained", "total_engagement"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["watch_hours", "average_view_percentage"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    for column in ["video_id", "title", "content_type", "thumbnail_url", "permalink"]:
        df[column] = df[column].fillna("").astype(str)
    return df.sort_values(["views", "total_engagement", "date"], ascending=[False, False, False])


def _render_metrics(metrics: dict[str, object]) -> None:
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    specs = [
        ("Views", "views", _fmt_int(current.get("views"))),
        ("Watch Hours", "watch_hours", _fmt_float(current.get("watch_hours"))),
        ("Subscribers Gained", "subscribers_gained", _fmt_int(current.get("subscribers_gained"))),
        ("Subscribers Lost", "subscribers_lost", _fmt_int(current.get("subscribers_lost"))),
        ("Net Subscribers", "net_subscribers", _fmt_int(current.get("net_subscribers"))),
        ("Total Engagement", "total_engagement", _fmt_int(current.get("total_engagement"))),
        ("Likes", "likes", _fmt_int(current.get("likes"))),
        ("Comments", "comments", _fmt_int(current.get("comments"))),
        ("Avg View Duration", "average_view_duration", _fmt_duration(current.get("average_view_duration"))),
    ]
    for row_start in range(0, len(specs), 3):
        columns = st.columns(3, gap="small")
        for column, (label, key, value) in zip(columns, specs[row_start : row_start + 3]):
            with column:
                with st.container(border=True):
                    growth_value = growth.get(key, 0.0)
                    st.metric(
                        label,
                        value,
                        delta=_campaign_format_growth(growth_value),
                        delta_color="off" if growth_value == 0 else ("inverse" if key == "subscribers_lost" else "normal"),
                    )


def _build_views_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Daily Views & Watch Hours")
        return figure

    labels = _date_labels(df["date"])
    figure.add_trace(
        go.Bar(
            x=labels,
            y=df["views"],
            name="Views",
            hovertemplate="<b>%{x}</b><br>Views: %{y:,}<extra></extra>",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["watch_hours"],
            name="Watch Hours",
            mode="lines+markers",
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Watch Hours: %{y:,.2f}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Daily Views & Watch Hours",
        xaxis_title="Date",
        yaxis_title="Views",
        yaxis2=dict(title="Watch Hours", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_subscribers_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Subscribers Growth")
        return figure

    labels = _date_labels(df["date"])
    for column, label in [
        ("subscribers_gained", "Subscribers Gained"),
        ("subscribers_lost", "Subscribers Lost"),
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
            y=df["net_subscribers"],
            name="Net Subscribers",
            mode="lines+markers",
            hovertemplate="<b>%{x}</b><br>Net Subscribers: %{y:,}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Subscribers Growth",
        xaxis_title="Date",
        yaxis_title="Subscribers",
        barmode="group",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_engagement_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Daily Engagement")
        return figure

    labels = _date_labels(df["date"])
    for column, label in [("likes", "Likes"), ("comments", "Comments"), ("shares", "Shares")]:
        figure.add_trace(
            go.Bar(
                x=labels,
                y=df[column],
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
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


def _render_daily_table(df: pd.DataFrame) -> None:
    st.markdown("### Daily YouTube Metrics")
    if df.empty:
        st.info("No YouTube daily data for selected date range.")
        return

    display_df = df[
        [
            "date",
            "views",
            "watch_hours",
            "subscribers_gained",
            "subscribers_lost",
            "net_subscribers",
            "likes",
            "comments",
            "shares",
            "average_view_duration",
        ]
    ].rename(
        columns={
            "date": "Date",
            "views": "Views",
            "watch_hours": "Watch Hours",
            "subscribers_gained": "Subscribers Gained",
            "subscribers_lost": "Subscribers Lost",
            "net_subscribers": "Net Subscribers",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "average_view_duration": "Avg View Duration (sec)",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Watch Hours": st.column_config.NumberColumn("Watch Hours", format="%.2f"),
            "Avg View Duration (sec)": st.column_config.NumberColumn("Avg View Duration (sec)", format="%.0f"),
        },
    )


def _render_media_metrics(summary: dict[str, object]) -> None:
    totals = summary.get("totals", {}) if isinstance(summary, dict) else {}
    specs = [
        ("Content", _fmt_int(totals.get("video_count"))),
        ("Views", _fmt_int(totals.get("views"))),
        ("Watch Hours", _fmt_float(totals.get("watch_hours"))),
        ("Avg % Viewed", f"{_fmt_float(totals.get('average_view_percentage'))}%"),
        ("Media Engagement", _fmt_int(totals.get("total_engagement"))),
        ("Subscribers Gained", _fmt_int(totals.get("subscribers_gained"))),
        ("Avg Engagement", _fmt_float(totals.get("avg_engagement_per_video"))),
    ]
    for row_specs, count in [(specs[:4], 4), (specs[4:], 3)]:
        columns = st.columns(count, gap="small")
        for column, (label, value) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _build_media_type_figure(summary: dict[str, object]) -> go.Figure:
    df = pd.DataFrame(summary.get("by_type", []) if isinstance(summary, dict) else [])
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Content Type Performance")
        return figure

    for column, label in [("views", "Views"), ("total_engagement", "Engagement"), ("subscribers_gained", "Subscribers Gained")]:
        figure.add_trace(
            go.Bar(
                x=df["content_type"],
                y=pd.to_numeric(df[column], errors="coerce").fillna(0),
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.update_layout(
        title="Content Type Performance",
        xaxis_title="Content Type",
        yaxis_title="Volume",
        barmode="group",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_top_media_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Top 5 YouTube Content")
        return figure

    top_df = df.head(5).sort_values("views", ascending=True).copy()
    top_df["label"] = (
        top_df["content_type"].str.title()
        + " | "
        + pd.to_datetime(top_df["date"]).dt.strftime("%b %d")
        + " | "
        + top_df["title"].str.replace(r"\s+", " ", regex=True).str.slice(0, 56)
    )
    figure.add_trace(
        go.Bar(
            y=top_df["label"],
            x=top_df["views"],
            name="Views",
            orientation="h",
            customdata=top_df[["watch_hours", "total_engagement", "average_view_percentage"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Views: %{x:,}<br>"
                "Watch Hours: %{customdata[0]:,.2f}<br>"
                "Engagement: %{customdata[1]:,}<br>"
                "Avg % Viewed: %{customdata[2]:,.2f}%<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title="Top 5 YouTube Content by Views",
        xaxis_title="Views",
        yaxis_title=None,
        margin=dict(l=18, r=18, t=70, b=36),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_media_activity_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Media Performance by Upload Date")
        return figure

    for content_type in sorted(df["content_type"].dropna().unique()):
        subset = df[df["content_type"] == content_type]
        figure.add_trace(
            go.Bar(
                x=_date_labels(subset["date"]),
                y=subset["views"],
                name=str(content_type).title(),
                customdata=subset[["video_count", "watch_hours", "total_engagement"]].values,
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    f"{str(content_type).title()} Views: %{{y:,}}<br>"
                    "Content: %{customdata[0]:,}<br>"
                    "Watch Hours: %{customdata[1]:,.2f}<br>"
                    "Engagement: %{customdata[2]:,}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Media Performance by Upload Date",
        xaxis_title="Upload Date",
        yaxis_title="Views",
        barmode="group",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _render_media_table(df: pd.DataFrame) -> None:
    st.markdown("### Top YouTube Content")
    if df.empty:
        st.info("No YouTube media data for selected date range.")
        return

    display_df = df[
        [
            "date",
            "content_type",
            "title",
            "views",
            "watch_hours",
            "average_view_percentage",
            "likes",
            "comments",
            "shares",
            "subscribers_gained",
            "permalink",
        ]
    ].copy()
    display_df["title"] = display_df["title"].str.slice(0, 160)
    display_df = display_df.rename(
        columns={
            "date": "Published Date",
            "content_type": "Content Type",
            "title": "Title",
            "views": "Views",
            "watch_hours": "Watch Hours",
            "average_view_percentage": "Avg % Viewed",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "subscribers_gained": "Subscribers Gained",
            "permalink": "Permalink",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Published Date": st.column_config.DateColumn("Published Date"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Permalink": st.column_config.LinkColumn("Permalink"),
            "Watch Hours": st.column_config.NumberColumn("Watch Hours", format="%.2f"),
            "Avg % Viewed": st.column_config.NumberColumn("Avg % Viewed", format="%.2f%%"),
        },
    )


async def show_youtube_page(host: str) -> None:
    start_date, end_date = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_range = (start_date, end_date)
    should_fetch = (
        "youtube_analytics_payload" not in st.session_state
        or st.session_state.get("youtube_analytics_range") != selected_range
    )
    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching YouTube analytics..."):
            response = await fetch_data(
                st=st,
                host=host,
                uri="youtube/analytics",
                method="GET",
                params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
        if not isinstance(response, dict) or not response.get("success", False):
            st.error((response or {}).get("detail") or (response or {}).get("message") or "Failed to fetch YouTube analytics.")
            return
        st.session_state["youtube_analytics_payload"] = response
        st.session_state["youtube_analytics_range"] = selected_range

    payload = st.session_state.get("youtube_analytics_payload", {}).get("data", {})
    daily_df = _daily_dataframe(payload.get("daily_rows", []))
    media_daily_df = _media_daily_dataframe(payload.get("media_daily_rows", []))
    media_df = _media_dataframe(payload.get("media_rows", []))

    _render_metrics(payload.get("metrics", {}))
    views_figure = _build_views_figure(daily_df)
    subscribers_figure = _build_subscribers_figure(daily_df)
    for column, figure in zip(st.columns(2, gap="small"), [views_figure, subscribers_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    engagement_figure = _build_engagement_figure(daily_df)
    engagement_figure.update_layout(height=380)
    with st.container(border=True):
        st.plotly_chart(engagement_figure, width="stretch")

    with st.container(border=True):
        _render_daily_table(daily_df)

    st.markdown("## Media Insights")
    _render_media_metrics(payload.get("media_summary", {}))
    media_activity_figure = _build_media_activity_figure(media_daily_df)
    media_type_figure = _build_media_type_figure(payload.get("media_summary", {}))
    for column, figure in zip(st.columns(2, gap="small"), [media_activity_figure, media_type_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    top_media_figure = _build_top_media_figure(media_df)
    top_media_figure.update_layout(height=430)
    with st.container(border=True):
        st.plotly_chart(top_media_figure, width="stretch")

    with st.container(border=True):
        _render_media_table(media_df)
