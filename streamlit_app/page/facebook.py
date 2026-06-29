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


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "net_followers",
        "organic_impressions",
        "post_engagements",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "total_reactions",
        "video_views",
        "page_views",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")


def _media_daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in [
        "post_count",
        "total_engagement",
        "total_reactions",
        "comments",
        "shares",
        "post_clicks",
        "post_media_view",
        "post_video_views",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")


def _media_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in [
        "total_engagement",
        "total_reactions",
        "comments",
        "shares",
        "post_clicks",
        "post_media_view",
        "post_video_views",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["post_id", "message", "permalink_url"]:
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
        ("Reactions", "total_reactions"),
        ("Video Views", "video_views"),
        ("Page Views", "page_views"),
    ]
    for row_start in range(0, len(specs), 3):
        columns = st.columns(3, gap="small")
        for column, (label, key) in zip(columns, specs[row_start : row_start + 3]):
            with column:
                with st.container(border=True):
                    growth_value = growth.get(key, 0.0)
                    st.metric(
                        label,
                        _fmt_int(current.get(key)),
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
        "total_reactions": "Reactions",
        "video_views": "Video Views",
        "page_views": "Page Views",
    }
    st.dataframe(df[list(columns)].rename(columns=columns), width="stretch", hide_index=True)


def _render_media_metrics(summary: dict[str, object]) -> None:
    specs = [
        ("Posts", _fmt_int(summary.get("post_count"))),
        ("Media Engagement", _fmt_int(summary.get("total_engagement"))),
        ("Reactions", _fmt_int(summary.get("total_reactions"))),
        ("Post Clicks", _fmt_int(summary.get("post_clicks"))),
        ("Media Views", _fmt_int(summary.get("post_media_view"))),
        ("Video Views", _fmt_int(summary.get("post_video_views"))),
        ("Avg Engagement", _fmt_float(summary.get("avg_engagement_per_post"))),
    ]
    for row_specs, count in [(specs[:4], 4), (specs[4:], 3)]:
        for column, (label, value) in zip(st.columns(count, gap="small"), row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _build_media_activity_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Media Performance by Upload Date")
        return figure
    labels = _date_labels(df["date"])
    for column, label in [
        ("post_media_view", "Media Views"),
        ("post_video_views", "Video Views"),
        ("post_clicks", "Post Clicks"),
    ]:
        figure.add_trace(go.Bar(x=labels, y=df[column], name=label, hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>"))
    figure.update_layout(
        title="Media Performance by Upload Date",
        xaxis_title="Upload Date",
        yaxis_title="Volume",
        xaxis=dict(type="category"),
        barmode="group",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_top_posts_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(title="Top 5 Facebook Posts")
        return figure
    top_df = df.head(5).sort_values("total_engagement", ascending=True).copy()
    messages = top_df["message"].str.replace(r"\s+", " ", regex=True).str.strip()
    top_df["label"] = pd.to_datetime(top_df["date"]).dt.strftime("%b %d") + " | " + messages.str.slice(0, 54)
    for column, label in [("total_reactions", "Reactions"), ("comments", "Comments"), ("shares", "Shares")]:
        figure.add_trace(go.Bar(y=top_df["label"], x=top_df[column], name=label, orientation="h"))
    figure.update_layout(
        title="Top 5 Facebook Posts by Engagement",
        xaxis_title="Engagement",
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
            "message",
            "total_engagement",
            "total_reactions",
            "comments",
            "shares",
            "post_clicks",
            "post_media_view",
            "post_video_views",
            "permalink_url",
        ]
    ].copy()
    display_df["message"] = display_df["message"].str.slice(0, 160)
    display_df = display_df.rename(
        columns={
            "date": "Date",
            "message": "Post",
            "total_engagement": "Engagement",
            "total_reactions": "Reactions",
            "comments": "Comments",
            "shares": "Shares",
            "post_clicks": "Clicks",
            "post_media_view": "Media Views",
            "post_video_views": "Video Views",
            "permalink_url": "Permalink",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Post": st.column_config.TextColumn("Post", width="large"),
            "Permalink": st.column_config.LinkColumn("Permalink"),
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
    _render_media_metrics(payload.get("media_summary", {}))
    media_activity_figure = _build_media_activity_figure(media_daily_df)
    top_posts_figure = _build_top_posts_figure(media_df)
    for column, figure in zip(st.columns(2, gap="small"), [media_activity_figure, top_posts_figure]):
        figure.update_layout(height=440)
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")
    with st.container(border=True):
        _render_media_table(media_df)
