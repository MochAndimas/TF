"""Streamlit page for Facebook Page analytics."""

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
    st.markdown('<div class="campaign-title">Facebook</div>', unsafe_allow_html=True)
    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "facebook_analytics_date_range"
    period_key = "facebook_analytics_period"
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


def _fmt_ms_hours(value) -> str:
    return f"{float(value or 0) / 3_600_000:,.2f}h"


def _fmt_ms_duration(value) -> str:
    total_seconds = int(round(float(value or 0) / 1000))
    minutes, seconds = divmod(total_seconds, 60)
    if minutes >= 60:
        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


def _fmt_post_type(value) -> str:
    return str(value or "Unknown").replace("_", " ").title()


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "post_type" not in df.columns:
        df["post_type"] = "UNKNOWN"
    df["post_type"] = df["post_type"].fillna("UNKNOWN").astype(str).str.upper().replace("", "UNKNOWN")
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    if "video_view_time" not in df.columns:
        df["video_view_time"] = 0
    for column in [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "net_followers",
        "organic_impressions",
        "post_engagements",
        "engagement_rate",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "total_reactions",
        "video_views",
        "video_view_time",
        "page_views",
    ]:
        if column == "engagement_rate":
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
        else:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values(["date", "post_type"])


def _media_daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "post_type" not in df.columns:
        df["post_type"] = "UNKNOWN"
    df["post_type"] = df["post_type"].fillna("UNKNOWN").astype(str).str.upper().replace("", "UNKNOWN")
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    for column in ("post_video_view_time", "post_video_avg_time_watched"):
        if column not in df.columns:
            df[column] = 0
    for column in [
        "post_count",
        "total_engagement",
        "total_reactions",
        "comments",
        "shares",
        "post_clicks",
        "post_media_view",
        "post_video_views",
        "post_video_view_time",
        "engagement_rate",
    ]:
        if column == "engagement_rate":
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
        else:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["post_video_avg_time_watched"] = pd.to_numeric(df["post_video_avg_time_watched"], errors="coerce").fillna(0.0).astype(float)
    return df.sort_values("date")


def _media_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    for column in ("post_video_view_time", "post_video_avg_time_watched", "post_video_length"):
        if column not in df.columns:
            df[column] = 0
    for column in [
        "total_engagement",
        "total_reactions",
        "comments",
        "shares",
        "post_clicks",
        "post_media_view",
        "post_video_views",
        "post_video_view_time",
        "post_video_length",
        "engagement_rate",
    ]:
        if column == "engagement_rate":
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
        else:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["post_video_avg_time_watched"] = pd.to_numeric(df["post_video_avg_time_watched"], errors="coerce").fillna(0.0).astype(float)
    for column in ["post_id", "post_type", "message", "permalink_url"]:
        df[column] = df[column].fillna("").astype(str)
    return df.sort_values(["total_engagement", "post_media_view", "date"], ascending=[False, False, False])


def _render_metrics(metrics: dict[str, object]) -> None:
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    specs = [
        ("Total Page Fans", "page_fans"),
        ("New Followers", "page_fan_adds"),
        ("Unfollowers", "page_fan_removes"),
        ("Net Followers", "net_followers"),
        ("Organic Impressions", "organic_impressions"),
        ("Post Engagements", "post_engagements"),
        ("Engagement Rate", "engagement_rate"),
        ("Reactions", "total_reactions"),
        ("Video Views", "video_views"),
        ("Video Watch Hours", "video_view_time"),
        ("Page Views", "page_views"),
    ]
    for row_start in range(0, len(specs), 5):
        row_specs = specs[row_start : row_start + 5]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, key) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    growth_value = growth.get(key, 0.0)
                    if key == "engagement_rate":
                        value = _fmt_pct(current.get(key))
                    elif key == "video_view_time":
                        value = _fmt_ms_hours(current.get(key))
                    else:
                        value = _fmt_int(current.get(key))
                    st.metric(
                        label,
                        value,
                        delta=_campaign_format_growth(growth_value),
                        delta_color="off" if growth_value == 0 else ("inverse" if key == "page_fan_removes" else "normal"),
                    )


def _build_followers_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Followers Growth")
        return figure
    labels = _date_labels(df["date"])
    for column, label in [("page_fan_adds", "New Followers"), ("page_fan_removes", "Unfollowers")]:
        figure.add_trace(go.Bar(x=labels, y=df[column], name=label, hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>"))
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["page_fans"],
            name="Total Page Fans",
            mode="lines+markers",
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Total Page Fans: %{y:,}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Followers Growth",
        xaxis_title="Date",
        yaxis_title="Followers Activity",
        yaxis2=dict(title="Total Page Fans", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        barmode="group",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_daily_performance_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Daily Page Performance")
        return figure
    labels = _date_labels(df["date"])
    for column, label in [
        ("organic_impressions", "Organic Impressions"),
        ("video_views", "Video Views"),
        ("page_views", "Page Views"),
    ]:
        figure.add_trace(go.Scatter(x=labels, y=df[column], name=label, mode="lines+markers", hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>"))
    figure.update_layout(
        title="Daily Page Performance",
        xaxis_title="Date",
        yaxis_title="Volume",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_reaction_figure(df: pd.DataFrame) -> go.Figure:
    reaction_columns = [
        ("reaction_like", "Like"),
        ("reaction_love", "Love"),
        ("reaction_wow", "Wow"),
        ("reaction_haha", "Haha"),
        ("reaction_sorry", "Sorry"),
        ("reaction_anger", "Anger"),
    ]
    totals = [int(df[column].sum()) if not df.empty else 0 for column, _ in reaction_columns]
    figure = go.Figure(
        data=[go.Pie(labels=[label for _, label in reaction_columns], values=totals, hole=0.48, textinfo="label+percent")]
    )
    figure.update_layout(title="Reaction Mix", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return figure


def _render_daily_table(df: pd.DataFrame) -> None:
    st.markdown("### Daily Facebook Metrics")
    if df.empty:
        st.info("No Facebook Page data for selected date range.")
        return
    columns = {
        "date": "Date",
        "page_fans": "Total Fans",
        "page_fan_adds": "New Followers",
        "page_fan_removes": "Unfollowers",
        "net_followers": "Net Followers",
        "organic_impressions": "Organic Impressions",
        "post_engagements": "Post Engagements",
        "engagement_rate": "Engagement Rate",
        "total_reactions": "Reactions",
        "video_views": "Video Views",
        "video_view_time": "Video Watch Hours (h)",
        "page_views": "Page Views",
    }
    st.dataframe(
        df.assign(video_view_time=df["video_view_time"] / 3_600_000)[list(columns)].rename(columns=columns),
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Total Fans": st.column_config.NumberColumn("Total Fans", format="%d"),
            "New Followers": st.column_config.NumberColumn("New Followers", format="%d"),
            "Unfollowers": st.column_config.NumberColumn("Unfollowers", format="%d"),
            "Net Followers": st.column_config.NumberColumn("Net Followers", format="%d"),
            "Organic Impressions": st.column_config.NumberColumn("Organic Impressions", format="%d"),
            "Post Engagements": st.column_config.NumberColumn("Post Engagements", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Reactions": st.column_config.NumberColumn("Reactions", format="%d"),
            "Video Views": st.column_config.NumberColumn("Video Views", format="%d"),
            "Video Watch Hours (h)": st.column_config.NumberColumn("Video Watch Hours (h)", format="%.2f h"),
            "Page Views": st.column_config.NumberColumn("Page Views", format="%d"),
        },
    )


def _render_media_metrics(summary: dict[str, object]) -> None:
    specs = [
        ("Posts", _fmt_int(summary.get("post_count"))),
        ("Media Engagement", _fmt_int(summary.get("total_engagement"))),
        ("Engagement Rate", _fmt_pct(summary.get("engagement_rate"))),
        ("Reactions", _fmt_int(summary.get("total_reactions"))),
        ("Post Clicks", _fmt_int(summary.get("post_clicks"))),
        ("Media Views", _fmt_int(summary.get("post_media_view"))),
        ("Video Views", _fmt_int(summary.get("post_video_views"))),
        ("Video Watch Hours", _fmt_ms_hours(summary.get("post_video_view_time"))),
        ("Avg Watch Time", _fmt_ms_duration(summary.get("post_video_avg_time_watched"))),
        ("Avg Video Length", _fmt_ms_duration(summary.get("post_video_length"))),
        ("Avg Engagement", _fmt_float(summary.get("avg_engagement_per_post"))),
    ]
    for row_start in range(0, len(specs), 4):
        row_specs = specs[row_start : row_start + 4]
        for column, (label, value) in zip(st.columns(len(row_specs), gap="small"), row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _build_media_activity_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Media Engagement by Upload Date")
        return figure
    for post_type in sorted(df["post_type"].dropna().unique()):
        subset = df[df["post_type"] == post_type]
        if subset.empty:
            continue
        label = _fmt_post_type(post_type)
        figure.add_trace(
            go.Bar(
                x=_date_labels(subset["date"]),
                y=subset["total_engagement"],
                name=label,
                customdata=subset[["post_count", "post_media_view", "post_video_views"]].values,
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    + label
                    + " Engagement: %{y:,}<br>"
                    + "Posts: %{customdata[0]:,}<br>"
                    + "Media Views: %{customdata[1]:,}<br>"
                    + "Video Views: %{customdata[2]:,}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Media Engagement by Upload Date",
        xaxis_title="Upload Date",
        yaxis_title="Engagement",
        xaxis=dict(type="category"),
        barmode="group",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_media_type_figure(summary: dict[str, object]) -> go.Figure:
    rows = summary.get("by_type", []) if isinstance(summary, dict) else []
    df = pd.DataFrame(rows)
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Media Type Performance")
        return figure
    df["post_type_label"] = df["post_type"].map(_fmt_post_type)
    for column, label in [
        ("total_reactions", "Reactions"),
        ("comments", "Comments"),
        ("shares", "Shares"),
    ]:
        figure.add_trace(
            go.Bar(
                x=df["post_type_label"],
                y=pd.to_numeric(df[column], errors="coerce").fillna(0),
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.update_layout(
        title="Media Type Performance",
        xaxis_title="Post Type",
        yaxis_title="Engagement",
        barmode="stack",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_top_posts_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Top 5 Facebook Media by Engagement")
        return figure
    top_df = df.head(5).sort_values("total_engagement", ascending=True).copy()
    messages = top_df["message"].str.replace(r"\s+", " ", regex=True).str.strip()
    messages = messages.where(messages != "", top_df["post_id"].astype(str))
    top_df["label"] = (
        top_df["post_type"].map(_fmt_post_type)
        + " | "
        + pd.to_datetime(top_df["date"]).dt.strftime("%b %d")
        + " | "
        + messages.str.slice(0, 58)
    )
    for column, label in [
        ("total_reactions", "Reactions"),
        ("comments", "Comments"),
        ("shares", "Shares"),
    ]:
        figure.add_trace(
            go.Bar(
                y=top_df["label"],
                x=top_df[column],
                name=label,
                orientation="h",
                customdata=top_df[["total_engagement", "post_media_view", "post_type"]].values,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + f"{label}: %{{x:,}}<br>"
                    + "Total Engagement: %{customdata[0]:,}<br>"
                    + "Media Views: %{customdata[1]:,}<br>"
                    + "Type: %{customdata[2]}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Top 5 Facebook Media by Engagement",
        xaxis_title="Engagement",
        yaxis_title=None,
        barmode="stack",
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=18, r=18, t=70, b=36),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _render_media_table(df: pd.DataFrame) -> None:
    st.markdown("### Top Facebook Media")
    if df.empty:
        st.info("No Facebook media data for selected date range.")
        return
    display_df = df[
        [
            "date",
            "post_type",
            "message",
            "total_engagement",
            "engagement_rate",
            "total_reactions",
            "comments",
            "shares",
            "post_clicks",
            "post_media_view",
            "post_video_views",
            "post_video_view_time",
            "post_video_avg_time_watched",
            "post_video_length",
            "permalink_url",
        ]
    ].copy()
    display_df["message"] = display_df["message"].str.slice(0, 160)
    display_df["post_video_view_time"] = display_df["post_video_view_time"] / 3_600_000
    display_df["post_video_avg_time_watched"] = display_df["post_video_avg_time_watched"].map(_fmt_ms_duration)
    display_df["post_video_length"] = display_df["post_video_length"].map(_fmt_ms_duration)
    display_df = display_df.rename(
        columns={
            "date": "Date",
            "post_type": "Post Type",
            "message": "Post",
            "total_engagement": "Engagement",
            "engagement_rate": "Engagement Rate",
            "total_reactions": "Reactions",
            "comments": "Comments",
            "shares": "Shares",
            "post_clicks": "Clicks",
            "post_media_view": "Media Views",
            "post_video_views": "Video Views",
            "post_video_view_time": "Video Watch Hours (h)",
            "post_video_avg_time_watched": "Avg Watch Time",
            "post_video_length": "Video Length",
            "permalink_url": "Permalink",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Post Type": st.column_config.TextColumn("Post Type"),
            "Post": st.column_config.TextColumn("Post", width="large"),
            "Permalink": st.column_config.LinkColumn("Permalink"),
            "Engagement": st.column_config.NumberColumn("Engagement", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Reactions": st.column_config.NumberColumn("Reactions", format="%d"),
            "Comments": st.column_config.NumberColumn("Comments", format="%d"),
            "Shares": st.column_config.NumberColumn("Shares", format="%d"),
            "Clicks": st.column_config.NumberColumn("Clicks", format="%d"),
            "Media Views": st.column_config.NumberColumn("Media Views", format="%d"),
            "Video Views": st.column_config.NumberColumn("Video Views", format="%d"),
            "Video Watch Hours (h)": st.column_config.NumberColumn("Video Watch Hours (h)", format="%.2f h"),
            "Avg Watch Time": st.column_config.TextColumn("Avg Watch Time"),
            "Video Length": st.column_config.TextColumn("Video Length"),
        },
    )


async def show_facebook_page(host: str) -> None:
    start_date, end_date = _render_filters()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    selected_range = (start_date, end_date)
    should_fetch = (
        "facebook_analytics_payload" not in st.session_state
        or st.session_state.get("facebook_analytics_range") != selected_range
    )
    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching Facebook analytics..."):
            response = await fetch_legacy_socmed_payload(
                host=host,
                uri="facebook/analytics",
                start_date=start_date,
                end_date=end_date,
                fallback_message="Failed to fetch Facebook analytics.",
            )
        if response is None:
            return
        st.session_state["facebook_analytics_payload"] = response
        st.session_state["facebook_analytics_range"] = selected_range

    payload = st.session_state.get("facebook_analytics_payload", {}).get("data", {})
    daily_df = _daily_dataframe(payload.get("daily_rows", []))
    media_daily_df = _media_daily_dataframe(payload.get("media_daily_rows", []))
    media_df = _media_dataframe(payload.get("media_rows", []))

    _render_metrics(payload.get("metrics", {}))
    followers_figure = _build_followers_figure(daily_df)
    performance_figure = _build_daily_performance_figure(daily_df)
    for column, figure in zip(st.columns(2, gap="small"), [followers_figure, performance_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    reaction_figure = _build_reaction_figure(daily_df)
    reaction_figure.update_layout(height=380)
    with st.container(border=True):
        st.plotly_chart(reaction_figure, width="stretch")
    with st.container(border=True):
        _render_daily_table(daily_df)

    st.markdown("## Media Insights")
    media_summary = payload.get("media_summary", {})
    _render_media_metrics(media_summary)
    media_activity_figure = _build_media_activity_figure(media_daily_df)
    media_type_figure = _build_media_type_figure(media_summary)
    for column, figure in zip(st.columns(2, gap="small"), [media_activity_figure, media_type_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")
    top_posts_figure = _build_top_posts_figure(media_df)
    top_posts_figure.update_layout(height=430)
    with st.container(border=True):
        st.plotly_chart(top_posts_figure, width="stretch")
    with st.container(border=True):
        _render_media_table(media_df)
