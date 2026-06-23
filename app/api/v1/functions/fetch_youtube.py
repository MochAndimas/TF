"""YouTube analytics payload builders."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import YouTubeDailyInsight, YouTubeMediaInsight


def _growth_percentage(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 100.0 if current_value else 0.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


async def _read_daily_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            YouTubeDailyInsight.date.label("date"),
            YouTubeDailyInsight.views.label("views"),
            YouTubeDailyInsight.watch_hours.label("watch_hours"),
            YouTubeDailyInsight.subscribers_gained.label("subscribers_gained"),
            YouTubeDailyInsight.subscribers_lost.label("subscribers_lost"),
            YouTubeDailyInsight.net_subscribers.label("net_subscribers"),
            YouTubeDailyInsight.likes.label("likes"),
            YouTubeDailyInsight.comments.label("comments"),
            YouTubeDailyInsight.shares.label("shares"),
            YouTubeDailyInsight.average_view_duration.label("average_view_duration"),
        )
        .where(YouTubeDailyInsight.date.between(start_date, end_date))
        .order_by(YouTubeDailyInsight.date)
    )
    rows = (await session.execute(query)).fetchall()
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
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["watch_hours", "average_view_duration"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
    df["total_engagement"] = df[["likes", "comments", "shares"]].sum(axis=1)
    return df


async def _read_media_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            YouTubeMediaInsight.date.label("date"),
            YouTubeMediaInsight.video_id.label("video_id"),
            YouTubeMediaInsight.title.label("title"),
            YouTubeMediaInsight.published_at.label("published_at"),
            YouTubeMediaInsight.content_type.label("content_type"),
            YouTubeMediaInsight.thumbnail_url.label("thumbnail_url"),
            YouTubeMediaInsight.permalink.label("permalink"),
            YouTubeMediaInsight.views.label("views"),
            YouTubeMediaInsight.watch_hours.label("watch_hours"),
            YouTubeMediaInsight.average_view_percentage.label("average_view_percentage"),
            YouTubeMediaInsight.likes.label("likes"),
            YouTubeMediaInsight.comments.label("comments"),
            YouTubeMediaInsight.shares.label("shares"),
            YouTubeMediaInsight.subscribers_gained.label("subscribers_gained"),
        )
        .where(YouTubeMediaInsight.date.between(start_date, end_date))
        .order_by(YouTubeMediaInsight.date, YouTubeMediaInsight.video_id)
    )
    rows = (await session.execute(query)).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    for column in ["views", "likes", "comments", "shares", "subscribers_gained"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["watch_hours", "average_view_percentage"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).astype(float)
    for column in ["video_id", "title", "content_type", "thumbnail_url", "permalink"]:
        df[column] = df[column].fillna("").astype(str)
    df["total_engagement"] = df[["likes", "comments", "shares"]].sum(axis=1)
    return df


def _daily_summary(df: pd.DataFrame) -> dict[str, object]:
    metric_keys = [
        "views",
        "watch_hours",
        "subscribers_gained",
        "subscribers_lost",
        "net_subscribers",
        "likes",
        "comments",
        "shares",
        "total_engagement",
        "average_view_duration",
    ]
    if df.empty:
        current = {key: 0 for key in metric_keys}
        current["watch_hours"] = 0.0
        current["average_view_duration"] = 0.0
        return {"current_period": {"metrics": current}, "growth_percentage": {key: 0.0 for key in metric_keys}}

    current = {
        "views": int(df["views"].sum()),
        "watch_hours": round(float(df["watch_hours"].sum()), 2),
        "subscribers_gained": int(df["subscribers_gained"].sum()),
        "subscribers_lost": int(df["subscribers_lost"].sum()),
        "net_subscribers": int(df["net_subscribers"].sum()),
        "likes": int(df["likes"].sum()),
        "comments": int(df["comments"].sum()),
        "shares": int(df["shares"].sum()),
        "total_engagement": int(df["total_engagement"].sum()),
        "average_view_duration": round(float(df["average_view_duration"].mean()), 2),
    }
    midpoint = len(df) // 2
    previous = df.iloc[:midpoint]
    recent = df.iloc[midpoint:]
    growth = {}
    for metric in metric_keys:
        if metric == "average_view_duration":
            current_value = float(recent[metric].mean()) if not recent.empty else 0.0
            previous_value = float(previous[metric].mean()) if not previous.empty else 0.0
        else:
            current_value = float(recent[metric].sum()) if not recent.empty else 0.0
            previous_value = float(previous[metric].sum()) if not previous.empty else 0.0
        growth[metric] = _growth_percentage(current_value, previous_value)
    return {"current_period": {"metrics": current}, "growth_percentage": growth}


def _media_summary(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        totals = {
            "video_count": 0,
            "views": 0,
            "watch_hours": 0.0,
            "average_view_percentage": 0.0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "subscribers_gained": 0,
            "total_engagement": 0,
            "avg_engagement_per_video": 0.0,
        }
        return {"totals": totals, "by_type": []}

    video_count = int(df["video_id"].nunique())
    total_engagement = int(df["total_engagement"].sum())
    totals = {
        "video_count": video_count,
        "views": int(df["views"].sum()),
        "watch_hours": round(float(df["watch_hours"].sum()), 2),
        "average_view_percentage": round(float(df["average_view_percentage"].mean()), 2),
        "likes": int(df["likes"].sum()),
        "comments": int(df["comments"].sum()),
        "shares": int(df["shares"].sum()),
        "subscribers_gained": int(df["subscribers_gained"].sum()),
        "total_engagement": total_engagement,
        "avg_engagement_per_video": round(total_engagement / video_count, 2) if video_count else 0.0,
    }
    grouped = (
        df.groupby("content_type", as_index=False)
        .agg(
            video_count=("video_id", "nunique"),
            views=("views", "sum"),
            watch_hours=("watch_hours", "sum"),
            average_view_percentage=("average_view_percentage", "mean"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            subscribers_gained=("subscribers_gained", "sum"),
            total_engagement=("total_engagement", "sum"),
        )
        .sort_values("content_type")
    )
    grouped["watch_hours"] = grouped["watch_hours"].round(2)
    grouped["average_view_percentage"] = grouped["average_view_percentage"].round(2)
    grouped["avg_engagement_per_video"] = grouped.apply(
        lambda row: round(float(row["total_engagement"]) / float(row["video_count"]), 2)
        if float(row["video_count"]) else 0.0,
        axis=1,
    )
    return {"totals": totals, "by_type": grouped.to_dict(orient="records")}


def _rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.copy()
    rows["date"] = rows["date"].astype(str)
    return rows.to_dict(orient="records")


def _media_daily_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    grouped = (
        df.groupby(["date", "content_type"], as_index=False)
        .agg(
            video_count=("video_id", "nunique"),
            views=("views", "sum"),
            watch_hours=("watch_hours", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            subscribers_gained=("subscribers_gained", "sum"),
            total_engagement=("total_engagement", "sum"),
        )
        .sort_values(["date", "content_type"])
    )
    grouped["watch_hours"] = grouped["watch_hours"].round(2)
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _media_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values(["views", "total_engagement", "date"], ascending=[False, False, False]).copy()
    rows["date"] = rows["date"].astype(str)
    rows["published_at"] = rows["published_at"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return rows.to_dict(orient="records")


async def fetch_youtube_analytics_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build YouTube analytics payload for the dashboard page."""
    daily_df = await _read_daily_rows(session=session, start_date=start_date, end_date=end_date)
    media_df = await _read_media_rows(session=session, start_date=start_date, end_date=end_date)
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics": _daily_summary(daily_df),
        "daily_rows": _rows_payload(daily_df),
        "media_summary": _media_summary(media_df),
        "media_daily_rows": _media_daily_payload(media_df),
        "media_rows": _media_rows_payload(media_df),
    }
