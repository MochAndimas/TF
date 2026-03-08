"""Load utilities for external API pipelines."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from sqlalchemy import case, literal, select, union_all
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataDepo, FacebookAds, GoogleAds, TikTokAds


async def rebuild_unique_campaign(session: AsyncSession) -> str:
    """Upsert campaign dimension rows from all ad-source fact tables."""
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
    """Convert normalized deposit DataFrame into DataDepo insert dictionaries."""
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
    """Convert normalized ads DataFrame into ads insert dictionaries."""
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


async def upsert_depo_rows(session: AsyncSession, rows: list[dict]) -> None:
    """Upsert rows into ``data_depo`` using business key."""
    if not rows:
        return
    insert_stmt = sqlite_insert(DataDepo).values(rows)
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
    """Upsert rows into ads table using business key."""
    if not rows:
        return
    insert_stmt = sqlite_insert(model_cls).values(rows)
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
