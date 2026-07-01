"""Facebook Page analytics payload builders."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import FacebookPageInsights, FacebookPageMediaInsights


DAILY_REACTION_COLUMNS = [
    "reaction_like",
    "reaction_love",
    "reaction_wow",
    "reaction_haha",
    "reaction_sorry",
    "reaction_anger",
]


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
            FacebookPageInsights.date.label("date"),
            FacebookPageInsights.page_fans.label("page_fans"),
            FacebookPageInsights.page_fan_adds.label("page_fan_adds"),
            FacebookPageInsights.page_fan_removes.label("page_fan_removes"),
            FacebookPageInsights.page_impressions_organic_v2.label("organic_impressions"),
            FacebookPageInsights.page_post_engagements.label("post_engagements"),
            FacebookPageInsights.reaction_like.label("reaction_like"),
            FacebookPageInsights.reaction_love.label("reaction_love"),
            FacebookPageInsights.reaction_wow.label("reaction_wow"),
            FacebookPageInsights.reaction_haha.label("reaction_haha"),
            FacebookPageInsights.reaction_sorry.label("reaction_sorry"),
            FacebookPageInsights.reaction_anger.label("reaction_anger"),
            FacebookPageInsights.page_video_views.label("video_views"),
            FacebookPageInsights.page_views_total.label("page_views"),
        )
        .where(FacebookPageInsights.date.between(start_date, end_date))
        .order_by(FacebookPageInsights.date)
    )
    rows = (await session.execute(query)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    metric_columns = [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "organic_impressions",
        "post_engagements",
        *DAILY_REACTION_COLUMNS,
        "video_views",
        "page_views",
    ]
    for column in metric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["net_followers"] = df["page_fan_adds"] - df["page_fan_removes"]
    df["total_reactions"] = df[DAILY_REACTION_COLUMNS].sum(axis=1)
    df["engagement_rate"] = df.apply(
        lambda row: _safe_percentage(float(row["post_engagements"]), float(row["page_fans"])),
        axis=1,
    )
    return df


async def _read_media_rows(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    query = (
        select(
            FacebookPageMediaInsights.date.label("date"),
            FacebookPageMediaInsights.post_id.label("post_id"),
            FacebookPageMediaInsights.created_time.label("created_time"),
            FacebookPageMediaInsights.message.label("message"),
            FacebookPageMediaInsights.permalink_url.label("permalink_url"),
            FacebookPageMediaInsights.comments.label("comments"),
            FacebookPageMediaInsights.shares.label("shares"),
            FacebookPageMediaInsights.reaction_like.label("reaction_like"),
            FacebookPageMediaInsights.reaction_love.label("reaction_love"),
            FacebookPageMediaInsights.reaction_wow.label("reaction_wow"),
            FacebookPageMediaInsights.reaction_haha.label("reaction_haha"),
            FacebookPageMediaInsights.reaction_sorry.label("reaction_sorry"),
            FacebookPageMediaInsights.reaction_anger.label("reaction_anger"),
            FacebookPageMediaInsights.post_clicks.label("post_clicks"),
            FacebookPageMediaInsights.post_media_view.label("post_media_view"),
            FacebookPageMediaInsights.post_video_views.label("post_video_views"),
            FacebookPageMediaInsights.total_engagement.label("total_engagement"),
        )
        .where(FacebookPageMediaInsights.date.between(start_date, end_date))
        .order_by(FacebookPageMediaInsights.date, FacebookPageMediaInsights.post_id)
    )
    rows = (await session.execute(query)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["created_time"] = pd.to_datetime(df["created_time"], errors="coerce")
    metric_columns = [
        "comments",
        "shares",
        *DAILY_REACTION_COLUMNS,
        "post_clicks",
        "post_media_view",
        "post_video_views",
        "total_engagement",
    ]
    for column in metric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["post_id", "message", "permalink_url"]:
        df[column] = df[column].fillna("").astype(str)
    df["total_reactions"] = df[DAILY_REACTION_COLUMNS].sum(axis=1)
    df["engagement_rate"] = df.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["post_media_view"])),
        axis=1,
    )
    return df


def _daily_summary(df: pd.DataFrame) -> dict[str, object]:
    metric_keys = [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "net_followers",
        "organic_impressions",
        "post_engagements",
        "total_reactions",
        "engagement_rate",
        "video_views",
        "page_views",
    ]
    if df.empty:
        current = {key: 0 for key in metric_keys}
        return {"current_period": {"metrics": current}, "growth_percentage": {key: 0.0 for key in metric_keys}}

    current = {
        "page_fans": int(df.loc[df["page_fans"] > 0, "page_fans"].iloc[-1]) if (df["page_fans"] > 0).any() else 0,
        "page_fan_adds": int(df["page_fan_adds"].sum()),
        "page_fan_removes": int(df["page_fan_removes"].sum()),
        "net_followers": int(df["net_followers"].sum()),
        "organic_impressions": int(df["organic_impressions"].sum()),
        "post_engagements": int(df["post_engagements"].sum()),
        "total_reactions": int(df["total_reactions"].sum()),
        "video_views": int(df["video_views"].sum()),
        "page_views": int(df["page_views"].sum()),
    }
    current["engagement_rate"] = _safe_percentage(
        float(current["post_engagements"]),
        float(current["page_fans"]),
    )
    midpoint = len(df) // 2
    previous = df.iloc[:midpoint]
    recent = df.iloc[midpoint:]
    growth = {"page_fans": 0.0}
    for metric in [key for key in metric_keys[1:] if key != "engagement_rate"]:
        growth[metric] = _growth_percentage(
            float(recent[metric].sum()) if not recent.empty else 0.0,
            float(previous[metric].sum()) if not previous.empty else 0.0,
        )
    growth["engagement_rate"] = _growth_percentage(
        _safe_percentage(
            float(recent["post_engagements"].sum()) if not recent.empty else 0.0,
            float(recent.loc[recent["page_fans"] > 0, "page_fans"].iloc[-1])
            if not recent.empty and (recent["page_fans"] > 0).any()
            else 0.0,
        ),
        _safe_percentage(
            float(previous["post_engagements"].sum()) if not previous.empty else 0.0,
            float(previous.loc[previous["page_fans"] > 0, "page_fans"].iloc[-1])
            if not previous.empty and (previous["page_fans"] > 0).any()
            else 0.0,
        ),
    )
    return {"current_period": {"metrics": current}, "growth_percentage": growth}


def _media_summary(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {
            "post_count": 0,
            "total_engagement": 0,
            "total_reactions": 0,
            "post_clicks": 0,
            "post_media_view": 0,
            "post_video_views": 0,
            "engagement_rate": 0.0,
            "avg_engagement_per_post": 0.0,
        }
    post_count = int(df["post_id"].nunique())
    total_engagement = int(df["total_engagement"].sum())
    return {
        "post_count": post_count,
        "total_engagement": total_engagement,
        "total_reactions": int(df["total_reactions"].sum()),
        "post_clicks": int(df["post_clicks"].sum()),
        "post_media_view": int(df["post_media_view"].sum()),
        "post_video_views": int(df["post_video_views"].sum()),
        "engagement_rate": _safe_percentage(float(total_engagement), float(df["post_media_view"].sum())),
        "avg_engagement_per_post": round(total_engagement / post_count, 2) if post_count else 0.0,
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
            post_count=("post_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            total_reactions=("total_reactions", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            post_clicks=("post_clicks", "sum"),
            post_media_view=("post_media_view", "sum"),
            post_video_views=("post_video_views", "sum"),
        )
        .sort_values("date")
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["post_media_view"])),
        axis=1,
    )
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _media_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values(["total_engagement", "post_media_view", "date"], ascending=[False, False, False]).copy()
    rows["engagement_rate"] = rows.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["post_media_view"])),
        axis=1,
    )
    rows["date"] = rows["date"].astype(str)
    rows["created_time"] = rows["created_time"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return rows.to_dict(orient="records")


async def fetch_facebook_analytics_payload(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, object]:
    """Build Facebook Page analytics payload for the dashboard page."""
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
