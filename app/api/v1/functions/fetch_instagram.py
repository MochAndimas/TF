"""Instagram analytics payload builders."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import InstagramInsights, InstagramMediaInsights


def _empty_instagram_frame() -> pd.DataFrame:
    """Return an empty Instagram dataframe with the expected analytics columns."""
    return pd.DataFrame(
        columns=[
            "date",
            "total_followers",
            "new_followers",
            "total_engagement",
            "likes",
            "comments",
            "shares",
            "saves",
        ]
    )


def _empty_instagram_media_frame() -> pd.DataFrame:
    """Return an empty Instagram media dataframe with analytics columns."""
    return pd.DataFrame(
        columns=[
            "date",
            "media_id",
            "media_type",
            "media_product_type",
            "timestamp",
            "caption",
            "permalink",
            "likes",
            "comments",
            "shares",
            "saves",
            "reach",
            "impressions",
            "plays",
            "total_engagement",
        ]
    )


async def _read_instagram_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            InstagramInsights.date.label("date"),
            InstagramInsights.total_followers.label("total_followers"),
            InstagramInsights.new_followers.label("new_followers"),
            InstagramInsights.total_engagement.label("total_engagement"),
            InstagramInsights.likes.label("likes"),
            InstagramInsights.comments.label("comments"),
            InstagramInsights.shares.label("shares"),
            InstagramInsights.saves.label("saves"),
        )
        .where(InstagramInsights.date.between(start_date, end_date))
        .order_by(InstagramInsights.date)
    )
    result = await session.execute(query)
    rows = result.fetchall()
    if not rows:
        return _empty_instagram_frame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    metric_columns = [
        "total_followers",
        "new_followers",
        "total_engagement",
        "likes",
        "comments",
        "shares",
        "saves",
    ]
    for column in metric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df


async def _read_instagram_media_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            InstagramMediaInsights.date.label("date"),
            InstagramMediaInsights.media_id.label("media_id"),
            InstagramMediaInsights.media_type.label("media_type"),
            InstagramMediaInsights.media_product_type.label("media_product_type"),
            InstagramMediaInsights.timestamp.label("timestamp"),
            InstagramMediaInsights.caption.label("caption"),
            InstagramMediaInsights.permalink.label("permalink"),
            InstagramMediaInsights.likes.label("likes"),
            InstagramMediaInsights.comments.label("comments"),
            InstagramMediaInsights.shares.label("shares"),
            InstagramMediaInsights.saves.label("saves"),
            InstagramMediaInsights.reach.label("reach"),
            InstagramMediaInsights.impressions.label("impressions"),
            InstagramMediaInsights.plays.label("plays"),
            InstagramMediaInsights.total_engagement.label("total_engagement"),
        )
        .where(InstagramMediaInsights.date.between(start_date, end_date))
        .order_by(InstagramMediaInsights.date, InstagramMediaInsights.media_product_type, InstagramMediaInsights.media_id)
    )
    result = await session.execute(query)
    rows = result.fetchall()
    if not rows:
        return _empty_instagram_media_frame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for column in [
        "likes",
        "comments",
        "shares",
        "saves",
        "reach",
        "impressions",
        "plays",
        "total_engagement",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["media_id", "media_type", "media_product_type", "caption", "permalink"]:
        df[column] = df[column].fillna("").astype(str)
    return df


def _growth_percentage(current_value: float, previous_value: float) -> float:
    if previous_value == 0:
        return 100.0 if current_value else 0.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def _latest_non_zero(series: pd.Series) -> int:
    non_zero = series[pd.to_numeric(series, errors="coerce").fillna(0) > 0]
    if non_zero.empty:
        return 0
    return int(non_zero.iloc[-1])


def _summary_payload(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        current = {
            "total_followers": 0,
            "new_followers": 0,
            "total_engagement": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "saves": 0,
            "engagement_per_new_follower": 0.0,
        }
        return {"current_period": {"metrics": current}, "growth_percentage": {key: 0.0 for key in current}}

    current = {
        "total_followers": _latest_non_zero(df["total_followers"]),
        "new_followers": int(df["new_followers"].sum()),
        "total_engagement": int(df["total_engagement"].sum()),
        "likes": int(df["likes"].sum()),
        "comments": int(df["comments"].sum()),
        "shares": int(df["shares"].sum()),
        "saves": int(df["saves"].sum()),
    }
    current["engagement_per_new_follower"] = round(
        current["total_engagement"] / current["new_followers"],
        2,
    ) if current["new_followers"] else 0.0

    midpoint = len(df) // 2
    previous = df.iloc[:midpoint].copy()
    recent = df.iloc[midpoint:].copy()
    growth = {}
    for metric in ("new_followers", "total_engagement", "likes", "comments", "shares", "saves"):
        growth[metric] = _growth_percentage(
            float(recent[metric].sum()) if not recent.empty else 0.0,
            float(previous[metric].sum()) if not previous.empty else 0.0,
        )
    growth["total_followers"] = 0.0
    growth["engagement_per_new_follower"] = _growth_percentage(
        current["engagement_per_new_follower"],
        round(float(previous["total_engagement"].sum()) / float(previous["new_followers"].sum()), 2)
        if not previous.empty and float(previous["new_followers"].sum()) else 0.0,
    )
    return {"current_period": {"metrics": current}, "growth_percentage": growth}


def _daily_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values("date").copy()
    rows["date"] = rows["date"].astype(str)
    return rows.to_dict(orient="records")


def _media_summary_payload(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        totals = {
            "media_count": 0,
            "feed_count": 0,
            "reels_count": 0,
            "total_engagement": 0,
            "reach": 0,
            "plays": 0,
            "avg_engagement_per_media": 0.0,
        }
        return {"totals": totals, "by_type": []}

    totals = {
        "media_count": int(len(df)),
        "feed_count": int((df["media_product_type"] == "FEED").sum()),
        "reels_count": int((df["media_product_type"] == "REELS").sum()),
        "total_engagement": int(df["total_engagement"].sum()),
        "reach": int(df["reach"].sum()),
        "plays": int(df["plays"].sum()),
    }
    totals["avg_engagement_per_media"] = round(totals["total_engagement"] / totals["media_count"], 2) if totals["media_count"] else 0.0

    grouped = (
        df.groupby("media_product_type", as_index=False)
        .agg(
            media_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
            reach=("reach", "sum"),
            impressions=("impressions", "sum"),
            plays=("plays", "sum"),
        )
        .sort_values("media_product_type")
    )
    grouped["avg_engagement_per_media"] = grouped.apply(
        lambda row: round(float(row["total_engagement"]) / float(row["media_count"]), 2)
        if float(row["media_count"]) else 0.0,
        axis=1,
    )
    return {"totals": totals, "by_type": grouped.to_dict(orient="records")}


def _media_daily_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    grouped = (
        df.groupby(["date", "media_product_type"], as_index=False)
        .agg(
            media_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
            reach=("reach", "sum"),
            impressions=("impressions", "sum"),
            plays=("plays", "sum"),
        )
        .sort_values(["date", "media_product_type"])
    )
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _media_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values(["total_engagement", "reach", "date"], ascending=[False, False, False]).copy()
    rows["date"] = rows["date"].astype(str)
    rows["timestamp"] = rows["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return rows.to_dict(orient="records")


async def fetch_instagram_analytics_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Instagram analytics payload for the dashboard page."""
    df = await _read_instagram_rows(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    media_df = await _read_instagram_media_rows(
        session=session,
        start_date=start_date,
        end_date=end_date,
    )
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "metrics": _summary_payload(df),
        "daily_rows": _daily_rows_payload(df),
        "media_summary": _media_summary_payload(media_df),
        "media_daily_rows": _media_daily_rows_payload(media_df),
        "media_rows": _media_rows_payload(media_df),
    }
