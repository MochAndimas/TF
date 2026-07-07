"""TikTok analytics payload builders."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import TikTokInsights, TikTokMediaInsights


def _growth_percentage(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 100.0 if current_value else 0.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _safe_percentage(numerator: float, denominator: float) -> float:
    return round((numerator / denominator) * 100, 2) if denominator else 0.0


async def _read_daily_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            TikTokInsights.date.label("date"),
            TikTokInsights.followers_snapshot.label("followers_snapshot"),
            TikTokInsights.total_likes.label("total_likes"),
            TikTokInsights.video_count.label("video_count"),
            TikTokInsights.views.label("views"),
            TikTokInsights.likes.label("likes"),
            TikTokInsights.comments.label("comments"),
            TikTokInsights.shares.label("shares"),
            TikTokInsights.engagement.label("engagement"),
            TikTokInsights.engagement_rate.label("engagement_rate"),
        )
        .where(TikTokInsights.date.between(start_date, end_date))
        .order_by(TikTokInsights.date)
    )
    rows = (await session.execute(query)).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    integer_columns = [
        "followers_snapshot",
        "total_likes",
        "video_count",
        "views",
        "likes",
        "comments",
        "shares",
        "engagement",
    ]
    for column in integer_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["engagement_rate"] = pd.to_numeric(df["engagement_rate"], errors="coerce").fillna(0.0).astype(float)
    return df


async def _read_media_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            TikTokMediaInsights.date.label("date"),
            TikTokMediaInsights.video_id.label("video_id"),
            TikTokMediaInsights.created_at.label("created_at"),
            TikTokMediaInsights.description.label("description"),
            TikTokMediaInsights.permalink.label("permalink"),
            TikTokMediaInsights.cover_image_url.label("cover_image_url"),
            TikTokMediaInsights.duration.label("duration"),
            TikTokMediaInsights.views.label("views"),
            TikTokMediaInsights.likes.label("likes"),
            TikTokMediaInsights.comments.label("comments"),
            TikTokMediaInsights.shares.label("shares"),
            TikTokMediaInsights.engagement.label("engagement"),
            TikTokMediaInsights.engagement_rate.label("engagement_rate"),
        )
        .where(TikTokMediaInsights.date.between(start_date, end_date))
        .order_by(TikTokMediaInsights.date, TikTokMediaInsights.video_id)
    )
    rows = (await session.execute(query)).fetchall()
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
    return df


def _snapshot_summary(df: pd.DataFrame) -> dict[str, object]:
    metric_keys = [
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
    if df.empty:
        current = {key: 0 for key in metric_keys}
        current["engagement_rate"] = 0.0
        return {"current_period": {"metrics": current}, "growth_percentage": {key: 0.0 for key in metric_keys}}

    sorted_df = df.sort_values("date").copy()
    latest = sorted_df.iloc[-1]
    current = {
        "followers_snapshot": int(latest["followers_snapshot"]),
        "total_likes": int(latest["total_likes"]),
        "video_count": int(latest["video_count"]),
        "views": int(latest["views"]),
        "likes": int(latest["likes"]),
        "comments": int(latest["comments"]),
        "shares": int(latest["shares"]),
        "engagement": int(latest["engagement"]),
        "engagement_rate": round(float(latest["engagement_rate"]), 2),
    }

    midpoint = len(sorted_df) // 2
    previous = sorted_df.iloc[:midpoint]
    recent = sorted_df.iloc[midpoint:]
    previous_latest = previous.iloc[-1] if not previous.empty else None
    recent_latest = recent.iloc[-1] if not recent.empty else latest
    growth = {}
    for metric in metric_keys:
        previous_value = float(previous_latest[metric]) if previous_latest is not None else 0.0
        growth[metric] = _growth_percentage(float(recent_latest[metric]), previous_value)
    return {"current_period": {"metrics": current}, "growth_percentage": growth}


def _media_summary(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {
            "video_count": 0,
            "views": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "engagement": 0,
            "engagement_rate": 0.0,
            "avg_engagement_per_video": 0.0,
            "avg_views_per_video": 0.0,
        }
    video_count = int(df["video_id"].nunique())
    engagement = int(df["engagement"].sum())
    views = int(df["views"].sum())
    return {
        "video_count": video_count,
        "views": views,
        "likes": int(df["likes"].sum()),
        "comments": int(df["comments"].sum()),
        "shares": int(df["shares"].sum()),
        "engagement": engagement,
        "engagement_rate": _safe_percentage(float(engagement), float(views)),
        "avg_engagement_per_video": round(engagement / video_count, 2) if video_count else 0.0,
        "avg_views_per_video": round(views / video_count, 2) if video_count else 0.0,
    }


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
        df.groupby("date", as_index=False)
        .agg(
            video_count=("video_id", "nunique"),
            views=("views", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            engagement=("engagement", "sum"),
        )
        .sort_values("date")
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["engagement"]), float(row["views"])),
        axis=1,
    )
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _media_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values(["views", "engagement", "date"], ascending=[False, False, False]).copy()
    rows["date"] = rows["date"].astype(str)
    rows["created_at"] = rows["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return rows.to_dict(orient="records")


async def fetch_tiktok_analytics_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build TikTok analytics payload for the dashboard page."""
    daily_df = await _read_daily_rows(session=session, start_date=start_date, end_date=end_date)
    media_df = await _read_media_rows(session=session, start_date=start_date, end_date=end_date)
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics": _snapshot_summary(daily_df),
        "daily_rows": _rows_payload(daily_df),
        "media_summary": _media_summary(media_df),
        "media_daily_rows": _media_daily_payload(media_df),
        "media_rows": _media_rows_payload(media_df),
    }
