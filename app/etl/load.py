"""Load utilities for external API pipelines."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import case, literal, select, union_all
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import (
    Campaign,
    DataDepo,
    FacebookAds,
    Ga4DailyMetrics,
    GoogleAds,
    TikTokAds,
)


SQLITE_MAX_VARIABLES = 999
SQLITE_VARIABLE_HEADROOM = 50


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


def build_depo_rows(df: pd.DataFrame, pull_date: date) -> list[dict]:
    """Convert normalized deposit dataframe into insert dictionaries.

    Args:
        df (pd.DataFrame): Validated deposit dataframe.
        pull_date (date): ETL pull date stamped on loaded rows.

    Returns:
        list[dict]: ``DataDepo``-compatible payload rows.
    """
    rows = []
    for _, row in df.iterrows():
        rows.append(
            {
                "user_id": row["id"],
                "tanggal_regis": row["tgl_regis"],
                "fullname": row["fullname"],
                "email": row["email"],
                "phone": row["phone"],
                "user_status": row["Status\nNew / Existing"],
                "campaign_id": row["campaignid"],
                "tag": row["tag"],
                "protection": row["protection"],
                "assign_date": row["Assign Date"],
                "analyst": row["Analyst"],
                "first_depo_date": row["First Depo Date"],
                "first_depo": row["First Depo $"],
                "time_to_closing": row["Time To Closing"],
                "nmi": row["NMI"],
                "lot": row["Lot"],
                "cabang": row["Cabang"],
                "pool": row["Pool"],
                "pull_date": pull_date,
            }
        )
    return rows


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


async def upsert_depo_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``data_depo`` table using deposit business key.

    Args:
        session (AsyncSession): Active database session.
        rows (list[dict]): Prepared ``DataDepo`` payload rows.

    Returns:
        None: Writes data as side effect.
    """
    if not rows:
        return
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
