"""Streamlit page for Instagram social media analytics."""

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
    st.markdown('<div class="campaign-title">Instagram</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    date_range_key = "instagram_analytics_date_range"
    period_key = "instagram_analytics_period"
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


def _fmt_pct(value) -> str:
    return f"{float(value or 0):,.2f}%"


def _fmt_media_bucket(value) -> str:
    return str(value or "").replace("_", " ").title()


def _render_metrics(metrics: dict[str, object]) -> None:
    current_metrics = metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = metrics.get("growth_percentage", {})
    metric_specs = [
        ("Total Followers", "total_followers", _fmt_int(current_metrics.get("total_followers"))),
        ("New Followers", "new_followers", _fmt_int(current_metrics.get("new_followers"))),
        ("Unfollowers", "unfollowers", _fmt_int(current_metrics.get("unfollowers"))),
        ("Total Engagement", "total_engagement", _fmt_int(current_metrics.get("total_engagement"))),
        ("Engagement Rate", "engagement_rate", _fmt_pct(current_metrics.get("engagement_rate"))),
        ("Likes", "likes", _fmt_int(current_metrics.get("likes"))),
        ("Comments", "comments", _fmt_int(current_metrics.get("comments"))),
        ("Shares", "shares", _fmt_int(current_metrics.get("shares"))),
        ("Saves", "saves", _fmt_int(current_metrics.get("saves"))),
        ("Engagement / New Follower", "engagement_per_new_follower", _fmt_float(current_metrics.get("engagement_per_new_follower"))),
    ]
    for row_start in range(0, len(metric_specs), 5):
        row_specs = metric_specs[row_start : row_start + 5]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, key, value) in zip(columns, row_specs):
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
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    for column in [
        "total_followers",
        "new_followers",
        "unfollowers",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    return df.sort_values("date")


def _media_daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "media_bucket" not in df.columns:
        df["media_bucket"] = df.get("media_product_type", "").fillna("").astype(str)
    df["media_bucket"] = df["media_bucket"].fillna("").astype(str)
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    for column in [
        "media_count",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "views",
        "profile_visits",
        "follows",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    return df.sort_values(["date", "media_bucket"])


def _media_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "engagement_rate" not in df.columns:
        df["engagement_rate"] = 0.0
    for column in [
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "views",
        "profile_visits",
        "follows",
        "total_engagement",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    for column in ["media_id", "media_type", "media_product_type", "caption", "permalink"]:
        df[column] = df[column].fillna("").astype(str)
    return df.sort_values(["total_engagement", "reach", "date"], ascending=[False, False, False])


def _hashtag_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for column in [
        "post_count",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "views",
        "profile_visits",
        "follows",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["engagement_rate", "avg_engagement_per_post"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
    df["hashtag"] = df["hashtag"].fillna("").astype(str)
    return df.sort_values(["total_engagement", "reach", "post_count"], ascending=[False, False, False])


def _best_time_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for column in [
        "day_order",
        "hour",
        "post_count",
        "total_engagement",
        "reach",
        "views",
        "likes",
        "comments",
        "shares",
        "saves",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["avg_engagement", "avg_reach", "avg_views", "engagement_rate"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
    for column in ["day_of_week", "hour_label", "slot_label"]:
        df[column] = df[column].fillna("").astype(str)
    return df.sort_values(["day_order", "hour"])


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
    figure.add_trace(
        go.Bar(
            x=labels,
            y=df["unfollowers"],
            name="Unfollowers",
            text=[f"{value:,}" for value in df["unfollowers"]],
            textposition="auto",
            hovertemplate="<b>%{x}</b><br>Unfollowers: %{y:,}<extra></extra>",
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
        yaxis_title="Followers Activity",
        yaxis2=dict(title="Total Followers", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        barmode="group",
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


def _render_media_metrics(media_summary: dict[str, object]) -> None:
    totals = media_summary.get("totals", {}) if isinstance(media_summary, dict) else {}
    metric_specs = [
        ("Media", _fmt_int(totals.get("media_count"))),
        ("Feed Posts", _fmt_int(totals.get("feed_count"))),
        ("Reels", _fmt_int(totals.get("reels_count"))),
        ("Media Engagement", _fmt_int(totals.get("total_engagement"))),
        ("Engagement Rate", _fmt_pct(totals.get("engagement_rate"))),
        ("Media Reach", _fmt_int(totals.get("reach"))),
        ("Views", _fmt_int(totals.get("views"))),
        ("Profile Visits", _fmt_int(totals.get("profile_visits"))),
        ("Followers Gained", _fmt_int(totals.get("follows"))),
        ("Avg Engagement", _fmt_float(totals.get("avg_engagement_per_media"))),
    ]
    for row_start in range(0, len(metric_specs), 5):
        row_specs = metric_specs[row_start : row_start + 5]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, value) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _build_media_engagement_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Media Engagement by Upload Date",
            annotations=[{"text": "No media data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    for media_type in sorted(df["media_bucket"].dropna().unique()):
        subset = df[df["media_bucket"] == media_type]
        if subset.empty:
            continue
        label = _fmt_media_bucket(media_type)
        figure.add_trace(
            go.Bar(
                x=_date_labels(subset["date"]),
                y=subset["total_engagement"],
                name=label,
                customdata=subset[["media_count", "reach"]].values,
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    + label
                    + " Engagement: %{y:,}<br>"
                    + "Media: %{customdata[0]:,}<br>"
                    + "Reach: %{customdata[1]:,}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Media Engagement by Upload Date",
        xaxis_title="Upload Date",
        yaxis_title="Engagement",
        barmode="group",
        xaxis=dict(type="category"),
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_media_type_figure(media_summary: dict[str, object]) -> go.Figure:
    rows = media_summary.get("by_type", []) if isinstance(media_summary, dict) else []
    df = pd.DataFrame(rows)
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Media Type Performance",
            annotations=[{"text": "No media data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure
    if "media_bucket" not in df.columns and "media_product_type" in df.columns:
        df["media_bucket"] = df["media_product_type"]
    df["media_label"] = df["media_bucket"].map(_fmt_media_bucket)

    for column, label in [
        ("likes", "Likes"),
        ("comments", "Comments"),
        ("shares", "Shares"),
        ("saves", "Saves"),
    ]:
        figure.add_trace(
            go.Bar(
                x=df["media_label"],
                y=pd.to_numeric(df[column], errors="coerce").fillna(0),
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.update_layout(
        title="Media Type Performance",
        xaxis_title="Media Type",
        yaxis_title="Engagement",
        barmode="stack",
        legend=dict(orientation="h", y=1.14, x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_top_media_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Top 5 Post/Reels",
            annotations=[{"text": "No media data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    top_df = df.sort_values(["total_engagement", "reach", "date"], ascending=[False, False, False]).head(5).copy()
    top_df = top_df.sort_values("total_engagement", ascending=True)
    caption = top_df["caption"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    caption = caption.where(caption != "", top_df["media_id"].astype(str))
    top_df["label"] = (
        top_df["media_product_type"].str.title()
        + " | "
        + pd.to_datetime(top_df["date"]).dt.strftime("%b %d")
        + " | "
        + caption.str.slice(0, 48)
    )

    for column, label in [
        ("likes", "Likes"),
        ("comments", "Comments"),
        ("shares", "Shares"),
        ("saves", "Saves"),
    ]:
        figure.add_trace(
            go.Bar(
                y=top_df["label"],
                x=top_df[column],
                name=label,
                orientation="h",
                customdata=top_df[["total_engagement", "reach", "media_product_type"]].values,
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    + f"{label}: %{{x:,}}<br>"
                    + "Total Engagement: %{customdata[0]:,}<br>"
                    + "Reach: %{customdata[1]:,}<br>"
                    + "Type: %{customdata[2]}<extra></extra>"
                ),
            )
        )
    figure.update_layout(
        title="Top 5 Post/Reels by Engagement",
        xaxis_title="Engagement",
        yaxis_title=None,
        barmode="stack",
        legend=dict(orientation="h", y=1.12, x=0),
        margin=dict(l=18, r=18, t=70, b=36),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _build_hashtag_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Top Hashtags by Engagement",
            annotations=[{"text": "No hashtag data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    top_df = df.head(12).sort_values("total_engagement", ascending=True)
    figure.add_trace(
        go.Bar(
            y=top_df["hashtag"],
            x=top_df["total_engagement"],
            orientation="h",
            name="Engagement",
            customdata=top_df[["post_count", "reach", "views", "engagement_rate"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                + "Engagement: %{x:,}<br>"
                + "Posts: %{customdata[0]:,}<br>"
                + "Reach: %{customdata[1]:,}<br>"
                + "Views: %{customdata[2]:,}<br>"
                + "Engagement Rate: %{customdata[3]:.2f}%<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title="Top Hashtags by Engagement",
        xaxis_title="Engagement",
        yaxis_title=None,
        margin=dict(l=18, r=18, t=70, b=36),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return figure


def _render_best_time_metrics(best_time: dict[str, object]) -> None:
    summary = best_time.get("summary", {}) if isinstance(best_time, dict) else {}
    metric_specs = [
        ("Best Day", summary.get("best_day") or "-"),
        ("Best Hour", summary.get("best_hour") or "-"),
        ("Best Slot", summary.get("best_slot") or "-"),
        ("Slot Posts", _fmt_int(summary.get("best_slot_post_count"))),
        ("Slot Engagement Rate", _fmt_pct(summary.get("best_slot_engagement_rate"))),
        ("Slot Avg Engagement", _fmt_float(summary.get("best_slot_avg_engagement"))),
    ]
    for row_start in range(0, len(metric_specs), 3):
        row_specs = metric_specs[row_start : row_start + 3]
        columns = st.columns(len(row_specs), gap="small")
        for column, (label, value) in zip(columns, row_specs):
            with column:
                with st.container(border=True):
                    st.metric(label, value)


def _build_best_time_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Best Time Heatmap",
            annotations=[{"text": "No post timing data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot_rate = (
        df.pivot_table(index="day_of_week", columns="hour", values="engagement_rate", aggfunc="mean")
        .reindex(day_order)
        .reindex(columns=list(range(24)))
    )
    pivot_posts = (
        df.pivot_table(index="day_of_week", columns="hour", values="post_count", aggfunc="sum")
        .reindex(day_order)
        .reindex(columns=list(range(24)))
        .fillna(0)
    )
    pivot_engagement = (
        df.pivot_table(index="day_of_week", columns="hour", values="avg_engagement", aggfunc="mean")
        .reindex(day_order)
        .reindex(columns=list(range(24)))
        .fillna(0)
    )
    customdata = [
        [
            [
                int(pivot_posts.iloc[row_index, column_index]),
                float(pivot_engagement.iloc[row_index, column_index]),
            ]
            for column_index in range(len(pivot_rate.columns))
        ]
        for row_index in range(len(pivot_rate.index))
    ]
    figure.add_trace(
        go.Heatmap(
            x=[f"{hour:02d}:00" for hour in pivot_rate.columns],
            y=pivot_rate.index.tolist(),
            z=pivot_rate.values,
            customdata=customdata,
            colorscale="Blues",
            colorbar=dict(title="Engagement Rate"),
            hovertemplate=(
                "<b>%{y} %{x}</b><br>"
                + "Engagement Rate: %{z:.2f}%<br>"
                + "Posts: %{customdata[0]:,}<br>"
                + "Avg Engagement: %{customdata[1]:,.2f}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title="Best Time Heatmap",
        xaxis_title="Upload Hour",
        yaxis_title="Upload Day",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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
            "unfollowers": "Unfollowers",
            "total_engagement": "Total Engagement",
            "engagement_rate": "Engagement Rate",
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
            "Unfollowers": st.column_config.NumberColumn("Unfollowers", format="%d"),
            "Total Engagement": st.column_config.NumberColumn("Total Engagement", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Likes": st.column_config.NumberColumn("Likes", format="%d"),
            "Comments": st.column_config.NumberColumn("Comments", format="%d"),
            "Shares": st.column_config.NumberColumn("Shares", format="%d"),
            "Saves": st.column_config.NumberColumn("Saves", format="%d"),
        },
    )


def _render_best_time_table(df: pd.DataFrame) -> None:
    st.markdown("### Best Time Detail")
    if df.empty:
        st.info("No post timing data for selected date range.")
        return

    display_df = df[
        [
            "day_of_week",
            "hour_label",
            "post_count",
            "engagement_rate",
            "avg_engagement",
            "total_engagement",
            "avg_reach",
            "avg_views",
            "reach",
            "views",
        ]
    ].copy()
    display_df = display_df.rename(
        columns={
            "day_of_week": "Day",
            "hour_label": "Hour",
            "post_count": "Posts",
            "engagement_rate": "Engagement Rate",
            "avg_engagement": "Avg Engagement",
            "total_engagement": "Total Engagement",
            "avg_reach": "Avg Reach",
            "avg_views": "Avg Views",
            "reach": "Reach",
            "views": "Views",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Day": st.column_config.TextColumn("Day"),
            "Hour": st.column_config.TextColumn("Hour"),
            "Posts": st.column_config.NumberColumn("Posts", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Avg Engagement": st.column_config.NumberColumn("Avg Engagement", format="%.2f"),
            "Total Engagement": st.column_config.NumberColumn("Total Engagement", format="%d"),
            "Avg Reach": st.column_config.NumberColumn("Avg Reach", format="%.2f"),
            "Avg Views": st.column_config.NumberColumn("Avg Views", format="%.2f"),
            "Reach": st.column_config.NumberColumn("Reach", format="%d"),
            "Views": st.column_config.NumberColumn("Views", format="%d"),
        },
    )


def _render_hashtag_table(df: pd.DataFrame) -> None:
    st.markdown("### Hashtag Performance")
    if df.empty:
        st.info("No hashtags found in selected media captions.")
        return

    display_df = df[
        [
            "hashtag",
            "post_count",
            "total_engagement",
            "engagement_rate",
            "avg_engagement_per_post",
            "likes",
            "comments",
            "shares",
            "saves",
            "reach",
            "views",
            "profile_visits",
            "follows",
        ]
    ].copy()
    display_df = display_df.rename(
        columns={
            "hashtag": "Hashtag",
            "post_count": "Posts",
            "total_engagement": "Engagement",
            "engagement_rate": "Engagement Rate",
            "avg_engagement_per_post": "Avg Engagement/Post",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "saves": "Saves",
            "reach": "Reach",
            "views": "Views",
            "profile_visits": "Profile Visits",
            "follows": "Followers Gained",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Hashtag": st.column_config.TextColumn("Hashtag"),
            "Posts": st.column_config.NumberColumn("Posts", format="%d"),
            "Engagement": st.column_config.NumberColumn("Engagement", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Avg Engagement/Post": st.column_config.NumberColumn("Avg Engagement/Post", format="%.2f"),
            "Likes": st.column_config.NumberColumn("Likes", format="%d"),
            "Comments": st.column_config.NumberColumn("Comments", format="%d"),
            "Shares": st.column_config.NumberColumn("Shares", format="%d"),
            "Saves": st.column_config.NumberColumn("Saves", format="%d"),
            "Reach": st.column_config.NumberColumn("Reach", format="%d"),
            "Views": st.column_config.NumberColumn("Views", format="%d"),
            "Profile Visits": st.column_config.NumberColumn("Profile Visits", format="%d"),
            "Followers Gained": st.column_config.NumberColumn("Followers Gained", format="%d"),
        },
    )


def _render_media_table(df: pd.DataFrame) -> None:
    st.markdown("### Top Instagram Media")
    if df.empty:
        st.info("No post/reels media data for selected date range.")
        return

    display_df = df[
        [
            "date",
            "media_product_type",
            "media_type",
            "caption",
            "total_engagement",
            "engagement_rate",
            "likes",
            "comments",
            "shares",
            "saves",
            "reach",
            "views",
            "profile_visits",
            "follows",
            "permalink",
        ]
    ].copy()
    display_df["caption"] = display_df["caption"].str.slice(0, 140)
    display_df = display_df.rename(
        columns={
            "date": "Date",
            "media_product_type": "Product",
            "media_type": "Type",
            "caption": "Caption",
            "total_engagement": "Engagement",
            "engagement_rate": "Engagement Rate",
            "likes": "Likes",
            "comments": "Comments",
            "shares": "Shares",
            "saves": "Saves",
            "reach": "Reach",
            "views": "Views",
            "profile_visits": "Profile Visits",
            "follows": "Followers Gained",
            "permalink": "Permalink",
        }
    )
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Caption": st.column_config.TextColumn("Caption", width="large"),
            "Permalink": st.column_config.LinkColumn("Permalink"),
            "Engagement": st.column_config.NumberColumn("Engagement", format="%d"),
            "Engagement Rate": st.column_config.NumberColumn("Engagement Rate", format="%.2f%%"),
            "Likes": st.column_config.NumberColumn("Likes", format="%d"),
            "Comments": st.column_config.NumberColumn("Comments", format="%d"),
            "Shares": st.column_config.NumberColumn("Shares", format="%d"),
            "Saves": st.column_config.NumberColumn("Saves", format="%d"),
            "Reach": st.column_config.NumberColumn("Reach", format="%d"),
            "Views": st.column_config.NumberColumn("Views", format="%d"),
            "Profile Visits": st.column_config.NumberColumn("Profile Visits", format="%d"),
            "Followers Gained": st.column_config.NumberColumn("Followers Gained", format="%d"),
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
            response = await fetch_legacy_socmed_payload(
                host=host,
                uri="instagram/analytics",
                start_date=start_date,
                end_date=end_date,
                fallback_message="Failed to fetch Instagram analytics.",
            )
        if response is None:
            return
        st.session_state["instagram_analytics_payload"] = response
        st.session_state["instagram_analytics_range"] = selected_range

    payload = st.session_state.get("instagram_analytics_payload", {}).get("data", {})
    df = _daily_dataframe(payload.get("daily_rows", []))
    media_daily_df = _media_daily_dataframe(payload.get("media_daily_rows", []))
    media_df = _media_dataframe(payload.get("media_rows", []))
    hashtag_df = _hashtag_dataframe(payload.get("hashtag_rows", []))
    best_time = payload.get("best_time", {})
    best_time_df = _best_time_dataframe(best_time.get("rows", []) if isinstance(best_time, dict) else [])
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

    st.markdown("## Media Insights")
    _render_media_metrics(payload.get("media_summary", {}))

    media_engagement_figure = _build_media_engagement_figure(media_daily_df)
    media_type_figure = _build_media_type_figure(payload.get("media_summary", {}))
    media_engagement_figure.update_layout(height=430)
    media_type_figure.update_layout(height=430)
    for column, figure in zip(st.columns(2, gap="small"), [media_engagement_figure, media_type_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    top_media_figure = _build_top_media_figure(media_df)
    top_media_figure.update_layout(height=430)
    with st.container(border=True):
        st.plotly_chart(top_media_figure, width="stretch")

    with st.container(border=True):
        _render_media_table(media_df)

    st.markdown("## Best Time to Post")
    _render_best_time_metrics(best_time if isinstance(best_time, dict) else {})

    best_time_figure = _build_best_time_figure(best_time_df)
    best_time_figure.update_layout(height=430)
    with st.container(border=True):
        st.plotly_chart(best_time_figure, width="stretch")

    with st.container(border=True):
        _render_best_time_table(best_time_df)

    st.markdown("## Hashtag Insights")
    hashtag_figure = _build_hashtag_figure(hashtag_df)
    hashtag_figure.update_layout(height=430)
    with st.container(border=True):
        st.plotly_chart(hashtag_figure, width="stretch")

    with st.container(border=True):
        _render_hashtag_table(hashtag_df)
