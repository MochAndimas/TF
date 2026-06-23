"""Load utilities for external API pipelines."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import case, delete, literal, select, union_all
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import (
    Campaign,
    DataDepo,
    DataMsDeposit,
    DailyRegister,
    FacebookAds,
    FacebookPageInsights,
    FacebookPageMediaInsights,
    Ga4DailyMetrics,
    GoogleAds,
    InstagramInsights,
    InstagramMediaInsights,
    TikTokAds,
    YouTubeDailyInsight,
    YouTubeMediaInsight,
)


SQLITE_MAX_VARIABLES = 999
SQLITE_VARIABLE_HEADROOM = 50


def _optional_int(value):
    """Convert optional numeric value to int while preserving null-like inputs."""
    if value is None or pd.isna(value):
        return None
    return int(value)


def _optional_float(value):
    """Convert optional numeric value to float while preserving null-like inputs."""
    if value is None or pd.isna(value):
        return None
    return float(value)


def _normalize_sql_value(value):
    """Convert pandas/numpy null-like scalars into DB-safe Python values."""
    if value is None:
        return None
    if pd.isna(value):
        return None
    return value


def _iter_row_chunks(rows: list[dict], columns_per_row: int):
    """Yield insert chunks sized to stay below SQLite bind-variable limit.

    Args:
        rows (list[dict]): Insert/upsert payload rows.
        columns_per_row (int): Number of columns per row payload.

    Yields:
        list[dict]: Chunked row slices sized for safe SQLite statements.
    """
    if not rows:
        return
    safe_limit = max(SQLITE_MAX_VARIABLES - SQLITE_VARIABLE_HEADROOM, columns_per_row)
    chunk_size = max(1, safe_limit // columns_per_row)
    for idx in range(0, len(rows), chunk_size):
        yield rows[idx : idx + chunk_size]


async def rebuild_unique_campaign(session: AsyncSession) -> str:
    """Rebuild campaign dimension table from all ads fact tables.

    Args:
        session (AsyncSession): Active database session.

    Returns:
        str: Human-readable ETL status message.
    """
    campaign_union = union_all(
        select(GoogleAds.campaign_id, GoogleAds.campaign_name),
        select(FacebookAds.campaign_id, FacebookAds.campaign_name),
        select(TikTokAds.campaign_id, TikTokAds.campaign_name),
    ).subquery()

    query = select(
        campaign_union.c.campaign_id.distinct().label("campaign_id"),
        campaign_union.c.campaign_name,
        case(
            (campaign_union.c.campaign_name.like("GG%"), "google_ads"),
            (campaign_union.c.campaign_name.like("FB%"), "facebook_ads"),
            (campaign_union.c.campaign_name.like("TT%"), "tiktok_ads"),
            else_="unknown",
        ).label("ad_source"),
        case(
            (campaign_union.c.campaign_name.like("%- UA -%"), "user_acquisition"),
            (campaign_union.c.campaign_name.like("%- BA -%"), "brand_awareness"),
            (campaign_union.c.campaign_name.like("%- RM -%"), "remarketing"),
            else_="unknown",
        ).label("ad_type"),
        literal(datetime.now()).label("created_at"),
    )

    result = await session.execute(query)
    rows = [dict(row._mapping) for row in result.fetchall()]
    if not rows:
        return "No data found from source."

    insert_stmt = sqlite_insert(Campaign).values(rows)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["campaign_id"],
        set_={
            "campaign_name": insert_stmt.excluded.campaign_name,
            "ad_source": insert_stmt.excluded.ad_source,
            "ad_type": insert_stmt.excluded.ad_type,
            "created_at": insert_stmt.excluded.created_at,
        },
    )
    await session.execute(upsert_stmt)
    await session.commit()

    return "Data is being updated!"


def build_ads_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized ads dataframe into insert dictionaries.

    Args:
        df (pd.DataFrame): Validated ads dataframe.
        pull_date (date): ETL pull date stamped on loaded rows.

    Returns:
        list[dict]: Ads-table-compatible payload rows.
    """
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "date": row["date"],
                "campaign_id": row["campaign_id"],
                "campaign_name": row["campaign_name"],
                "ad_group": row["ad_group"],
                "ad_name": row["ad_name"],
                "cost": row["cost"],
                "impressions": row["impressions"],
                "clicks": row["clicks"],
                "leads": row["leads"],
                "pull_date": pull_date,
            }
        )
    return rows


def build_ga4_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized GA4 dataframe into insert dictionaries.

    Args:
        df (pd.DataFrame): Validated GA4 dataframe at ``date + source`` grain.
        pull_date (date): ETL pull date stamped on loaded rows.

    Returns:
        list[dict]: ``ga4_daily_metrics``-compatible payload rows.
    """
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "date": row["date"],
                "source": row["source"],
                "daily_active_users": row["daily_active_users"],
                "monthly_active_users": row["monthly_active_users"],
                "active_users": row["active_users"],
                "pull_date": pull_date,
            }
        )
    return rows


def build_daily_register_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized daily register rows into insert dictionaries."""
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "date": row["date"],
                "campaign_id": row["campaign_id"],
                "total_regis": int(row["total_regis"]),
                "pull_date": pull_date,
            }
        )
    return rows


def build_instagram_insights_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized Instagram insights rows into insert dictionaries."""
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "date": row["date"],
                "total_followers": int(row["total_followers"]),
                "new_followers": int(row["new_followers"]),
                "unfollowers": int(row["unfollowers"]),
                "total_engagement": int(row["total_engagement"]),
                "likes": int(row["likes"]),
                "comments": int(row["comments"]),
                "shares": int(row["shares"]),
                "saves": int(row["saves"]),
                "pull_date": pull_date,
            }
        )
    return rows


def build_youtube_daily_insight_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized YouTube daily metrics into insert dictionaries."""
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "date": row["date"],
                "views": int(row["views"]),
                "watch_hours": float(row["watch_hours"]),
                "subscribers_gained": int(row["subscribers_gained"]),
                "subscribers_lost": int(row["subscribers_lost"]),
                "net_subscribers": int(row["net_subscribers"]),
                "likes": int(row["likes"]),
                "comments": int(row["comments"]),
                "shares": int(row["shares"]),
                "average_view_duration": float(row["average_view_duration"]),
                "pull_date": pull_date,
            }
        )
    return rows


def build_youtube_media_insight_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized YouTube media snapshots into insert dictionaries."""
    rows = []
    for _, row in df.iterrows():
        published_at = row["published_at"]
        if hasattr(published_at, "to_pydatetime"):
            published_at = published_at.to_pydatetime()
        if published_at.tzinfo is not None:
            published_at = published_at.replace(tzinfo=None)
        rows.append(
            {
                "date": row["date"],
                "video_id": row["video_id"],
                "title": row["title"],
                "published_at": published_at,
                "content_type": row["content_type"],
                "thumbnail_url": None if pd.isna(row["thumbnail_url"]) else row["thumbnail_url"],
                "permalink": row["permalink"],
                "views": int(row["views"]),
                "watch_hours": float(row["watch_hours"]),
                "average_view_percentage": float(row["average_view_percentage"]),
                "likes": int(row["likes"]),
                "comments": int(row["comments"]),
                "shares": int(row["shares"]),
                "subscribers_gained": int(row["subscribers_gained"]),
                "pull_date": pull_date,
            }
        )
    return rows


def build_instagram_media_insights_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized Instagram media insights rows into insert dictionaries."""
    rows = []
    for _, row in df.iterrows():
        payload = {
            "date": row["date"],
            "media_id": row["media_id"],
            "media_type": row["media_type"],
            "media_product_type": row["media_product_type"],
            "timestamp": row.get("timestamp"),
            "caption": row.get("caption") or None,
            "permalink": row.get("permalink") or None,
            "media_url": row.get("media_url") or None,
            "thumbnail_url": row.get("thumbnail_url") or None,
            "likes": int(row["likes"]),
            "comments": int(row["comments"]),
            "shares": int(row["shares"]),
            "saves": int(row["saves"]),
            "reach": int(row["reach"]),
            "impressions": int(row["impressions"]),
            "plays": int(row["plays"]),
            "total_engagement": int(row["total_engagement"]),
            "pull_date": pull_date,
        }
        rows.append({key: _normalize_sql_value(value) for key, value in payload.items()})
    return rows


def build_facebook_page_insights_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized Facebook Page insights rows into insert dictionaries."""
    metric_columns = [
        "page_fans",
        "page_fan_adds",
        "page_fan_removes",
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_organic_v2",
        "page_post_engagements",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "page_video_views",
        "page_views_total",
    ]
    rows = []
    for _, row in df.iterrows():
        payload = {
            "page_id": row["page_id"],
            "date": row["date"],
            "pull_date": pull_date,
        }
        for column in metric_columns:
            payload[column] = int(row[column])
        rows.append(payload)
    return rows


def build_facebook_page_media_insights_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized Facebook Page post/media insight rows into insert dictionaries."""
    metric_columns = [
        "likes",
        "comments",
        "shares",
        "reaction_like",
        "reaction_love",
        "reaction_wow",
        "reaction_haha",
        "reaction_sorry",
        "reaction_anger",
        "post_media_view",
        "post_clicks",
        "post_video_views",
        "total_engagement",
    ]
    rows = []
    for _, row in df.iterrows():
        payload = {
            "page_id": row["page_id"],
            "date": row["date"],
            "post_id": row["post_id"],
            "post_type": row["post_type"],
            "status_type": row.get("status_type") or None,
            "created_time": row.get("created_time"),
            "message": row.get("message") or None,
            "permalink_url": row.get("permalink_url") or None,
            "full_picture": row.get("full_picture") or None,
            "pull_date": pull_date,
        }
        for column in metric_columns:
            payload[column] = int(row[column])
        rows.append({key: _normalize_sql_value(value) for key, value in payload.items()})
    return rows


def _infer_ad_type_from_campaign_name(campaign_name: str) -> str:
    """Infer campaign objective label from naming convention when available."""
    normalized_name = str(campaign_name or "").upper()
    if "- UA -" in normalized_name:
        return "user_acquisition"
    if "- BA -" in normalized_name:
        return "brand_awareness"
    if "- RM -" in normalized_name:
        return "remarketing"
    return "unknown"


async def _ensure_campaign_rows_for_ads(
    session: AsyncSession,
    model_cls,
    rows: list[dict],
) -> None:
    """Create missing campaign dimension rows before ads facts are inserted."""
    if not rows:
        return

    campaign_payloads: dict[str, dict] = {}
    source_name = getattr(model_cls, "__tablename__", "unknown")
    now = datetime.now()

    for row in rows:
        campaign_id = str(row.get("campaign_id") or "").strip()
        if not campaign_id:
            continue
        campaign_name = str(row.get("campaign_name") or "").strip() or f"Unknown Campaign {campaign_id}"
        campaign_payloads[campaign_id] = {
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "ad_source": source_name,
            "ad_type": _infer_ad_type_from_campaign_name(campaign_name),
            "created_at": now,
        }

    if not campaign_payloads:
        return

    campaign_ids = set(campaign_payloads)
    existing = await session.execute(
        select(Campaign.campaign_id).where(Campaign.campaign_id.in_(campaign_ids))
    )
    existing_ids = set(existing.scalars().all())
    missing_rows = [
        campaign_payloads[campaign_id]
        for campaign_id in sorted(campaign_ids - existing_ids)
    ]
    if not missing_rows:
        return

    insert_stmt = sqlite_insert(Campaign).values(missing_rows)
    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["campaign_id"])
    await session.execute(upsert_stmt)


async def upsert_ads_rows(session: AsyncSession, model_cls, rows: list[dict]) -> None:
    """Upsert rows into ads fact table using ads business key.

    Args:
        session (AsyncSession): Active database session.
        model_cls: Target ads SQLAlchemy model (`GoogleAds`/`FacebookAds`/`TikTokAds`).
        rows (list[dict]): Prepared ads payload rows.

    Returns:
        None: Writes data as side effect.
    """
    if not rows:
        return
    await _ensure_campaign_rows_for_ads(session=session, model_cls=model_cls, rows=rows)
    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(model_cls).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["date", "campaign_id", "ad_group", "ad_name"],
            set_={
                "campaign_name": insert_stmt.excluded.campaign_name,
                "cost": insert_stmt.excluded.cost,
                "impressions": insert_stmt.excluded.impressions,
                "clicks": insert_stmt.excluded.clicks,
                "leads": insert_stmt.excluded.leads,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def delete_rows_in_date_window(
    session: AsyncSession,
    model_cls,
    *,
    window_start: date,
    window_end: date,
) -> int:
    """Delete rows from a date-grained fact table inside a selected window."""
    result = await session.execute(
        delete(model_cls).where(model_cls.date.between(window_start, window_end))
    )
    return int(result.rowcount or 0)


async def upsert_ga4_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``ga4_daily_metrics`` using GA4 business key.

    Args:
        session (AsyncSession): Active database session.
        rows (list[dict]): Prepared GA4 payload rows.

    Returns:
        None: Writes data as side effect.
    """
    if not rows:
        return
    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(Ga4DailyMetrics).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["date", "source"],
            set_={
                "daily_active_users": insert_stmt.excluded.daily_active_users,
                "monthly_active_users": insert_stmt.excluded.monthly_active_users,
                "active_users": insert_stmt.excluded.active_users,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_daily_register_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``daily_register`` using date and campaign key."""
    if not rows:
        return

    await _ensure_campaign_rows_for_deposits(
        session=session,
        campaign_ids={str(row["campaign_id"]).strip() or "-" for row in rows},
    )
    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(DailyRegister).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["date", "campaign_id"],
            set_={
                "total_regis": insert_stmt.excluded.total_regis,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_instagram_insights_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``instagram_insights`` using date as business key."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(InstagramInsights).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={
                "total_followers": insert_stmt.excluded.total_followers,
                "new_followers": insert_stmt.excluded.new_followers,
                "unfollowers": insert_stmt.excluded.unfollowers,
                "total_engagement": insert_stmt.excluded.total_engagement,
                "likes": insert_stmt.excluded.likes,
                "comments": insert_stmt.excluded.comments,
                "shares": insert_stmt.excluded.shares,
                "saves": insert_stmt.excluded.saves,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_youtube_daily_insight_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``youtube_daily_insight`` using date as business key."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(YouTubeDailyInsight).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["date"],
            set_={
                "views": insert_stmt.excluded.views,
                "watch_hours": insert_stmt.excluded.watch_hours,
                "subscribers_gained": insert_stmt.excluded.subscribers_gained,
                "subscribers_lost": insert_stmt.excluded.subscribers_lost,
                "net_subscribers": insert_stmt.excluded.net_subscribers,
                "likes": insert_stmt.excluded.likes,
                "comments": insert_stmt.excluded.comments,
                "shares": insert_stmt.excluded.shares,
                "average_view_duration": insert_stmt.excluded.average_view_duration,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_youtube_media_insight_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``youtube_media_insight`` using video ID as business key."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(YouTubeMediaInsight).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["video_id"],
            set_={
                "date": insert_stmt.excluded.date,
                "title": insert_stmt.excluded.title,
                "published_at": insert_stmt.excluded.published_at,
                "content_type": insert_stmt.excluded.content_type,
                "thumbnail_url": insert_stmt.excluded.thumbnail_url,
                "permalink": insert_stmt.excluded.permalink,
                "views": insert_stmt.excluded.views,
                "watch_hours": insert_stmt.excluded.watch_hours,
                "average_view_percentage": insert_stmt.excluded.average_view_percentage,
                "likes": insert_stmt.excluded.likes,
                "comments": insert_stmt.excluded.comments,
                "shares": insert_stmt.excluded.shares,
                "subscribers_gained": insert_stmt.excluded.subscribers_gained,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_instagram_media_insights_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``instagram_media_insights`` using media_id as business key."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(InstagramMediaInsights).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["media_id"],
            set_={
                "date": insert_stmt.excluded.date,
                "media_type": insert_stmt.excluded.media_type,
                "media_product_type": insert_stmt.excluded.media_product_type,
                "timestamp": insert_stmt.excluded.timestamp,
                "caption": insert_stmt.excluded.caption,
                "permalink": insert_stmt.excluded.permalink,
                "media_url": insert_stmt.excluded.media_url,
                "thumbnail_url": insert_stmt.excluded.thumbnail_url,
                "likes": insert_stmt.excluded.likes,
                "comments": insert_stmt.excluded.comments,
                "shares": insert_stmt.excluded.shares,
                "saves": insert_stmt.excluded.saves,
                "reach": insert_stmt.excluded.reach,
                "impressions": insert_stmt.excluded.impressions,
                "plays": insert_stmt.excluded.plays,
                "total_engagement": insert_stmt.excluded.total_engagement,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_facebook_page_insights_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``facebook_page_insights`` using page_id and date."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(FacebookPageInsights).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["page_id", "date"],
            set_={
                "page_fans": insert_stmt.excluded.page_fans,
                "page_fan_adds": insert_stmt.excluded.page_fan_adds,
                "page_fan_removes": insert_stmt.excluded.page_fan_removes,
                "page_impressions": insert_stmt.excluded.page_impressions,
                "page_impressions_unique": insert_stmt.excluded.page_impressions_unique,
                "page_impressions_paid": insert_stmt.excluded.page_impressions_paid,
                "page_impressions_organic_v2": insert_stmt.excluded.page_impressions_organic_v2,
                "page_post_engagements": insert_stmt.excluded.page_post_engagements,
                "reaction_like": insert_stmt.excluded.reaction_like,
                "reaction_love": insert_stmt.excluded.reaction_love,
                "reaction_wow": insert_stmt.excluded.reaction_wow,
                "reaction_haha": insert_stmt.excluded.reaction_haha,
                "reaction_sorry": insert_stmt.excluded.reaction_sorry,
                "reaction_anger": insert_stmt.excluded.reaction_anger,
                "page_video_views": insert_stmt.excluded.page_video_views,
                "page_views_total": insert_stmt.excluded.page_views_total,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def upsert_facebook_page_media_insights_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``facebook_page_media_insights`` using post_id as business key."""
    if not rows:
        return

    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(FacebookPageMediaInsights).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["post_id"],
            set_={
                "page_id": insert_stmt.excluded.page_id,
                "date": insert_stmt.excluded.date,
                "post_type": insert_stmt.excluded.post_type,
                "status_type": insert_stmt.excluded.status_type,
                "created_time": insert_stmt.excluded.created_time,
                "message": insert_stmt.excluded.message,
                "permalink_url": insert_stmt.excluded.permalink_url,
                "full_picture": insert_stmt.excluded.full_picture,
                "likes": insert_stmt.excluded.likes,
                "comments": insert_stmt.excluded.comments,
                "shares": insert_stmt.excluded.shares,
                "reaction_like": insert_stmt.excluded.reaction_like,
                "reaction_love": insert_stmt.excluded.reaction_love,
                "reaction_wow": insert_stmt.excluded.reaction_wow,
                "reaction_haha": insert_stmt.excluded.reaction_haha,
                "reaction_sorry": insert_stmt.excluded.reaction_sorry,
                "reaction_anger": insert_stmt.excluded.reaction_anger,
                "post_media_view": insert_stmt.excluded.post_media_view,
                "post_clicks": insert_stmt.excluded.post_clicks,
                "post_video_views": insert_stmt.excluded.post_video_views,
                "total_engagement": insert_stmt.excluded.total_engagement,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


def build_first_deposit_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized first-deposit rows into ``data_depo`` payload dicts.

    Args:
        df (pd.DataFrame): Normalized and validated first-deposit dataframe.
        pull_date (date): Date stamp recorded on each loaded row.

    Returns:
        list[dict]: Insert/upsert payload rows compatible with the
        ``data_depo`` SQLAlchemy model.
    """
    rows = []
    for _, row in df.iterrows():
        payload = {
            "user_id": int(row["user_id"]),
            "tanggal_regis": row["tanggal_regis"],
            "fullname": row.get("fullname") or None,
            "email": row["email"] or None,
            "phone": row.get("phone") or None,
            "user_status": row["user_status"] or None,
            "campaign_id": row["campaign_id"],
            "tag": row.get("tag") or None,
            "protection": _optional_int(row.get("protection")),
            "assign_date": row.get("assign_date") or None,
            "analyst": _optional_int(row.get("analyst")),
            "first_depo_date": row.get("first_depo_date") or None,
            "first_depo": float(row["first_depo"]),
            "time_to_closing": row.get("time_to_closing") or None,
            "nmi": _optional_float(row.get("nmi")),
            "lot": _optional_float(row.get("lot")),
            "cabang": row.get("cabang") or None,
            "pool": bool(row["pool"]) if pd.notna(row.get("pool")) else None,
            "pull_date": pull_date,
        }
        rows.append({key: _normalize_sql_value(value) for key, value in payload.items()})
    return rows


async def _ensure_campaign_rows_for_deposits(session: AsyncSession, campaign_ids: set[str]) -> None:
    """Create placeholder campaign dimension rows for unknown deposit IDs.

    ``DataDepo.campaign_id`` has a foreign-key dependency on ``campaign``.
    First-deposit payloads may contain blank or previously unseen campaign IDs,
    so this helper ensures ETL loads do not fail just because the campaign
    dimension has not been rebuilt yet.

    Args:
        session (AsyncSession): Active database session.
        campaign_ids (set[str]): Unique campaign identifiers referenced by the
            first-deposit payload prepared for upsert.

    Returns:
        None: Inserts missing placeholder campaign rows as a side effect.
    """
    normalized_ids = {str(campaign_id).strip() or "-" for campaign_id in campaign_ids}
    if not normalized_ids:
        return

    existing = await session.execute(
        select(Campaign.campaign_id).where(Campaign.campaign_id.in_(normalized_ids))
    )
    existing_ids = set(existing.scalars().all())
    missing_ids = normalized_ids - existing_ids
    if not missing_ids:
        return

    now = datetime.now()
    placeholder_rows = [
        {
            "campaign_id": campaign_id,
            "campaign_name": "Unknown Campaign" if campaign_id == "-" else f"Unknown Campaign {campaign_id}",
            "ad_source": "unknown",
            "ad_type": "unknown",
            "created_at": now,
        }
        for campaign_id in sorted(missing_ids)
    ]
    insert_stmt = sqlite_insert(Campaign).values(placeholder_rows)
    upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=["campaign_id"])
    await session.execute(upsert_stmt)


async def upsert_first_deposit_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert first-deposit rows into ``data_depo`` using an idempotent key.

    The business key for this ETL is ``user_id + tanggal_regis + campaign_id``.
    Re-running the same window therefore updates existing records instead of
    creating duplicates, while still allowing revised values such as
    ``first_depo`` or ``user_status`` to overwrite older data.

    Args:
        session (AsyncSession): Active database session.
        rows (list[dict]): Prepared load payload generated from the normalized
            first-deposit dataframe.

    Returns:
        None: Writes rows into ``data_depo`` as a database side effect.
    """
    if not rows:
        return

    await _ensure_campaign_rows_for_deposits(
        session=session,
        campaign_ids={str(row["campaign_id"]).strip() or "-" for row in rows},
    )
    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(DataDepo).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["user_id", "tanggal_regis", "campaign_id"],
            set_={
                "fullname": insert_stmt.excluded.fullname,
                "email": insert_stmt.excluded.email,
                "phone": insert_stmt.excluded.phone,
                "user_status": insert_stmt.excluded.user_status,
                "tag": insert_stmt.excluded.tag,
                "protection": insert_stmt.excluded.protection,
                "assign_date": insert_stmt.excluded.assign_date,
                "analyst": insert_stmt.excluded.analyst,
                "first_depo_date": insert_stmt.excluded.first_depo_date,
                "first_depo": insert_stmt.excluded.first_depo,
                "time_to_closing": insert_stmt.excluded.time_to_closing,
                "nmi": insert_stmt.excluded.nmi,
                "lot": insert_stmt.excluded.lot,
                "cabang": insert_stmt.excluded.cabang,
                "pool": insert_stmt.excluded.pool,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def delete_first_deposit_rows_in_window(
    session: AsyncSession,
    *,
    window_start: date,
    window_end: date,
) -> int:
    """Delete first-deposit rows whose registration date falls in a window."""
    result = await session.execute(
        delete(DataDepo).where(DataDepo.tanggal_regis.between(window_start, window_end))
    )
    return int(result.rowcount or 0)


def build_ms_deposit_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized MS1 deposit/activity rows into load payload dicts."""
    rows = []
    for _, row in df.iterrows():
        payload = {
            "email": row["email"],
            "tag": row.get("tag") or None,
            "campaign_id": row["campaign_id"],
            "user_status": row["user_status"] or None,
            "first_depo": float(row["first_depo"]),
            "time_to_closing": row.get("time_to_closing") or None,
            "last_depo": row.get("last_depo") or None,
            "last_depo_amount": _optional_float(row.get("last_depo_amount")),
            "last_activity": row["last_activity"],
            "pull_date": pull_date,
        }
        rows.append({key: _normalize_sql_value(value) for key, value in payload.items()})
    return rows


async def upsert_ms_deposit_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert MS1 deposit/activity rows into ``data_ms_deposit``."""
    if not rows:
        return

    await _ensure_campaign_rows_for_deposits(
        session=session,
        campaign_ids={str(row["campaign_id"]).strip() or "-" for row in rows},
    )
    columns_per_row = len(rows[0])
    for chunk in _iter_row_chunks(rows, columns_per_row):
        insert_stmt = sqlite_insert(DataMsDeposit).values(chunk)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["email", "last_activity", "campaign_id", "tag"],
            set_={
                "user_status": insert_stmt.excluded.user_status,
                "first_depo": insert_stmt.excluded.first_depo,
                "time_to_closing": insert_stmt.excluded.time_to_closing,
                "last_depo": insert_stmt.excluded.last_depo,
                "last_depo_amount": insert_stmt.excluded.last_depo_amount,
                "pull_date": insert_stmt.excluded.pull_date,
            },
        )
        await session.execute(upsert_stmt)


async def delete_ms_deposit_rows_in_window(
    session: AsyncSession,
    *,
    window_start: date,
    window_end: date,
) -> int:
    """Delete MS1 deposit rows whose last activity date falls in a window."""
    result = await session.execute(
        delete(DataMsDeposit).where(DataMsDeposit.last_activity.between(window_start, window_end))
    )
    return int(result.rowcount or 0)
