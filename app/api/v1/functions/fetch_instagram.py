"""Instagram analytics payload builders."""

from __future__ import annotations

from datetime import date
import re

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import InstagramInsights, InstagramMediaInsights

HASHTAG_PATTERN = re.compile(r"(?<![\w])#([\w]+)", re.UNICODE)


def _empty_instagram_frame() -> pd.DataFrame:
    """Return an empty Instagram dataframe with the expected analytics columns."""
    return pd.DataFrame(
        columns=[
            "date",
            "total_followers",
            "new_followers",
            "unfollowers",
            "total_engagement",
            "likes",
            "comments",
            "shares",
            "saves",
            "engagement_rate",
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
            "views",
            "profile_visits",
            "follows",
            "engagement_rate",
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
            InstagramInsights.unfollowers.label("unfollowers"),
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
        "unfollowers",
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
            InstagramMediaInsights.views.label("views"),
            InstagramMediaInsights.profile_visits.label("profile_visits"),
            InstagramMediaInsights.follows.label("follows"),
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
        "views",
        "profile_visits",
        "follows",
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


def _safe_percentage(numerator: float, denominator: float) -> float:
    return round((numerator / denominator) * 100, 2) if denominator else 0.0


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
            "unfollowers": 0,
            "total_engagement": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "saves": 0,
            "engagement_rate": 0.0,
            "engagement_per_new_follower": 0.0,
        }
        return {"current_period": {"metrics": current}, "growth_percentage": {key: 0.0 for key in current}}

    current = {
        "total_followers": _latest_non_zero(df["total_followers"]),
        "new_followers": int(df["new_followers"].sum()),
        "unfollowers": int(df["unfollowers"].sum()),
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
    current["engagement_rate"] = _safe_percentage(
        float(current["total_engagement"]),
        float(current["total_followers"]),
    )

    midpoint = len(df) // 2
    previous = df.iloc[:midpoint].copy()
    recent = df.iloc[midpoint:].copy()
    growth = {}
    for metric in ("new_followers", "unfollowers", "total_engagement", "likes", "comments", "shares", "saves"):
        growth[metric] = _growth_percentage(
            float(recent[metric].sum()) if not recent.empty else 0.0,
            float(previous[metric].sum()) if not previous.empty else 0.0,
        )
    growth["total_followers"] = 0.0
    recent_followers = _latest_non_zero(recent["total_followers"]) if not recent.empty else 0
    previous_followers = _latest_non_zero(previous["total_followers"]) if not previous.empty else 0
    growth["engagement_rate"] = _growth_percentage(
        _safe_percentage(float(recent["total_engagement"].sum()) if not recent.empty else 0.0, float(recent_followers)),
        _safe_percentage(float(previous["total_engagement"].sum()) if not previous.empty else 0.0, float(previous_followers)),
    )
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
    rows["engagement_rate"] = rows.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["total_followers"])),
        axis=1,
    )
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
            "views": 0,
            "profile_visits": 0,
            "follows": 0,
            "engagement_rate": 0.0,
            "avg_engagement_per_media": 0.0,
        }
        return {"totals": totals, "by_type": []}

    totals = {
        "media_count": int(len(df)),
        "feed_count": int((df["media_product_type"] == "FEED").sum()),
        "reels_count": int((df["media_product_type"] == "REELS").sum()),
        "total_engagement": int(df["total_engagement"].sum()),
        "reach": int(df["reach"].sum()),
        "views": int(df["views"].sum()),
        "profile_visits": int(df["profile_visits"].sum()),
        "follows": int(df["follows"].sum()),
    }
    totals["avg_engagement_per_media"] = round(totals["total_engagement"] / totals["media_count"], 2) if totals["media_count"] else 0.0
    totals["engagement_rate"] = _safe_percentage(float(totals["total_engagement"]), float(totals["reach"]))

    bucketed = df.copy()
    bucketed["media_bucket"] = bucketed.apply(
        lambda row: "REELS"
        if str(row["media_product_type"]).upper() == "REELS"
        else str(row["media_type"]).upper(),
        axis=1,
    )
    grouped = (
        bucketed.groupby("media_bucket", as_index=False)
        .agg(
            media_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
            reach=("reach", "sum"),
            views=("views", "sum"),
            profile_visits=("profile_visits", "sum"),
            follows=("follows", "sum"),
        )
        .sort_values("media_bucket")
    )
    grouped["avg_engagement_per_media"] = grouped.apply(
        lambda row: round(float(row["total_engagement"]) / float(row["media_count"]), 2)
        if float(row["media_count"]) else 0.0,
        axis=1,
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    return {"totals": totals, "by_type": grouped.to_dict(orient="records")}


def _media_daily_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    bucketed = df.copy()
    bucketed["media_bucket"] = bucketed.apply(
        lambda row: "REELS"
        if str(row["media_product_type"]).upper() == "REELS"
        else str(row["media_type"]).upper(),
        axis=1,
    )
    grouped = (
        bucketed.groupby(["date", "media_bucket"], as_index=False)
        .agg(
            media_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
            reach=("reach", "sum"),
            views=("views", "sum"),
            profile_visits=("profile_visits", "sum"),
            follows=("follows", "sum"),
        )
        .sort_values(["date", "media_bucket"])
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    grouped["date"] = grouped["date"].astype(str)
    return grouped.to_dict(orient="records")


def _media_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    rows = df.sort_values(["total_engagement", "reach", "date"], ascending=[False, False, False]).copy()
    rows["engagement_rate"] = rows.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    rows["date"] = rows["date"].astype(str)
    rows["timestamp"] = rows["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return rows.to_dict(orient="records")


def _extract_hashtags(caption: object) -> list[str]:
    tags = {
        f"#{match.group(1).lower()}"
        for match in HASHTAG_PATTERN.finditer(str(caption or ""))
    }
    return sorted(tags)


def _hashtag_rows_payload(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []

    rows: list[dict[str, object]] = []
    for _, media in df.iterrows():
        for hashtag in _extract_hashtags(media.get("caption")):
            rows.append(
                {
                    "hashtag": hashtag,
                    "media_id": media.get("media_id"),
                    "total_engagement": int(media.get("total_engagement") or 0),
                    "likes": int(media.get("likes") or 0),
                    "comments": int(media.get("comments") or 0),
                    "shares": int(media.get("shares") or 0),
                    "saves": int(media.get("saves") or 0),
                    "reach": int(media.get("reach") or 0),
                    "views": int(media.get("views") or 0),
                    "profile_visits": int(media.get("profile_visits") or 0),
                    "follows": int(media.get("follows") or 0),
                }
            )
    if not rows:
        return []

    hashtag_df = pd.DataFrame(rows)
    grouped = (
        hashtag_df.groupby("hashtag", as_index=False)
        .agg(
            post_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
            reach=("reach", "sum"),
            views=("views", "sum"),
            profile_visits=("profile_visits", "sum"),
            follows=("follows", "sum"),
        )
        .sort_values(["total_engagement", "reach", "post_count"], ascending=[False, False, False])
    )
    grouped["avg_engagement_per_post"] = grouped.apply(
        lambda row: round(float(row["total_engagement"]) / float(row["post_count"]), 2)
        if float(row["post_count"]) else 0.0,
        axis=1,
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    return grouped.to_dict(orient="records")


def _best_time_empty_payload() -> dict[str, object]:
    return {
        "summary": {
            "best_day": "",
            "best_hour": "",
            "best_slot": "",
            "best_slot_post_count": 0,
            "best_slot_engagement_rate": 0.0,
            "best_slot_avg_engagement": 0.0,
            "min_posts": 2,
        },
        "rows": [],
    }


def _best_time_rows_payload(df: pd.DataFrame) -> dict[str, object]:
    if df.empty or "timestamp" not in df.columns:
        return _best_time_empty_payload()

    rows = df.copy()
    rows["timestamp"] = pd.to_datetime(rows["timestamp"], errors="coerce")
    rows = rows.dropna(subset=["timestamp"])
    if rows.empty:
        return _best_time_empty_payload()

    rows["day_order"] = rows["timestamp"].dt.dayofweek
    rows["day_of_week"] = rows["timestamp"].dt.day_name()
    rows["hour"] = rows["timestamp"].dt.hour
    grouped = (
        rows.groupby(["day_order", "day_of_week", "hour"], as_index=False)
        .agg(
            post_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            reach=("reach", "sum"),
            views=("views", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
            saves=("saves", "sum"),
        )
        .sort_values(["day_order", "hour"])
    )
    grouped["avg_engagement"] = grouped.apply(
        lambda row: round(float(row["total_engagement"]) / float(row["post_count"]), 2)
        if float(row["post_count"]) else 0.0,
        axis=1,
    )
    grouped["avg_reach"] = grouped.apply(
        lambda row: round(float(row["reach"]) / float(row["post_count"]), 2)
        if float(row["post_count"]) else 0.0,
        axis=1,
    )
    grouped["avg_views"] = grouped.apply(
        lambda row: round(float(row["views"]) / float(row["post_count"]), 2)
        if float(row["post_count"]) else 0.0,
        axis=1,
    )
    grouped["engagement_rate"] = grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    grouped["hour_label"] = grouped["hour"].map(lambda value: f"{int(value):02d}:00")
    grouped["slot_label"] = grouped["day_of_week"] + " " + grouped["hour_label"]

    min_posts = 2
    ranked = grouped[grouped["post_count"] >= min_posts].copy()
    if ranked.empty:
        ranked = grouped.copy()
    ranked = ranked.sort_values(
        ["engagement_rate", "avg_engagement", "post_count", "reach"],
        ascending=[False, False, False, False],
    )
    best_slot = ranked.iloc[0] if not ranked.empty else None

    best_day_grouped = (
        rows.groupby(["day_order", "day_of_week"], as_index=False)
        .agg(
            post_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            reach=("reach", "sum"),
        )
    )
    best_day_grouped["engagement_rate"] = best_day_grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    best_day = best_day_grouped.sort_values(
        ["engagement_rate", "total_engagement", "post_count"],
        ascending=[False, False, False],
    ).iloc[0]

    best_hour_grouped = (
        rows.groupby("hour", as_index=False)
        .agg(
            post_count=("media_id", "nunique"),
            total_engagement=("total_engagement", "sum"),
            reach=("reach", "sum"),
        )
    )
    best_hour_grouped["engagement_rate"] = best_hour_grouped.apply(
        lambda row: _safe_percentage(float(row["total_engagement"]), float(row["reach"])),
        axis=1,
    )
    best_hour = best_hour_grouped.sort_values(
        ["engagement_rate", "total_engagement", "post_count"],
        ascending=[False, False, False],
    ).iloc[0]

    summary = {
        "best_day": str(best_day["day_of_week"]),
        "best_hour": f"{int(best_hour['hour']):02d}:00",
        "best_slot": str(best_slot["slot_label"]) if best_slot is not None else "",
        "best_slot_post_count": int(best_slot["post_count"]) if best_slot is not None else 0,
        "best_slot_engagement_rate": float(best_slot["engagement_rate"]) if best_slot is not None else 0.0,
        "best_slot_avg_engagement": float(best_slot["avg_engagement"]) if best_slot is not None else 0.0,
        "min_posts": min_posts,
    }
    return {"summary": summary, "rows": grouped.to_dict(orient="records")}


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
        "hashtag_rows": _hashtag_rows_payload(media_df),
        "best_time": _best_time_rows_payload(media_df),
    }
