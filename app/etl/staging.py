"""Staging-layer utilities for raw payload persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta

from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.external_api import StgAdsRaw
from app.etl.transform import normalize_columns


def _payload_hash(payload) -> str:
    """Build stable payload hash for raw staging rows.

    Args:
        payload: Raw JSON-serializable payload object.

    Returns:
        str: SHA-256 hex digest for dedupe/audit use.
    """
    serialized = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


async def cleanup_old_staging_rows(session: AsyncSession) -> int:
    """Delete raw staging rows older than the configured retention window."""
    retention_days = settings.STG_ADS_RAW_RETENTION_DAYS
    if retention_days <= 0:
        return 0
    cutoff = datetime.now() - timedelta(days=retention_days)
    result = await session.execute(delete(StgAdsRaw).where(StgAdsRaw.ingested_at < cutoff))
    return int(result.rowcount or 0)


async def _insert_staging_rows(session: AsyncSession, rows: list[dict]) -> int:
    if not rows:
        return 0
    await session.execute(insert(StgAdsRaw), rows)
    await cleanup_old_staging_rows(session)
    return len(rows)


async def stage_ads_raw(
    session: AsyncSession,
    raw_rows: list,
    *,
    run_id: str | None,
    source: str,
    range_name: str,
) -> int:
    """Persist raw ads sheet rows into staging table.

    Args:
        session (AsyncSession): Active database session.
        raw_rows (list): Raw Sheets payload rows (header + data rows).
        run_id (str | None): ETL run identifier for traceability.
        source (str): Logical source label stored in staging.
        range_name (str): Original Sheets range used by extraction.

    Returns:
        int: Number of staged rows inserted.
    """
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    payloads = []
    if isinstance(raw_rows[0], dict):
        payloads = raw_rows
    else:
        headers = normalize_columns(raw_rows[0]) if raw_rows else []
        if len(raw_rows) < 2:
            payloads = [{"_raw": row} for row in raw_rows]
        else:
            payloads = [dict(zip(headers, row)) for row in raw_rows[1:]]

    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": range_name,
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in payloads
    ]
    return await _insert_staging_rows(session, rows)


async def stage_ga4_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw GA4 API rows into shared ads staging table.

    Args:
        session (AsyncSession): Active database session.
        raw_rows (list[dict]): Raw GA4 API rows after extractor normalization.
        run_id (str | None): ETL run identifier for traceability.
        source (str): Logical source label stored in staging.

    Returns:
        int: Number of staged rows inserted.
    """
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "ga4_daily_metrics",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_instagram_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw Instagram insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "instagram_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_tiktok_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw TikTok insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "tiktok_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_tiktok_media_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw TikTok media insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "tiktok_media_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_youtube_daily_insight_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw YouTube daily insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "youtube_daily_insight_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_youtube_media_insight_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw YouTube media insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "youtube_media_insight_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_instagram_media_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw Instagram media insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "instagram_media_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_facebook_page_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw Facebook Page insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "facebook_page_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_facebook_page_media_insights_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw Facebook Page post/media insight rows into shared staging storage."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "facebook_page_media_insights_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_first_deposit_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw first-deposit API rows into the shared staging table.

    This function mirrors the staging behavior used by ads and GA4 pipelines so
    the project has one audit trail pattern for all external-source ETL jobs.
    Each raw JSON object is stored as immutable payload plus a stable payload
    hash and ETL run metadata.

    Args:
        session (AsyncSession): Active database session used for the insert.
        raw_rows (list[dict]): Raw JSON rows returned by the first-deposit API.
        run_id (str | None): ETL run identifier used to correlate staging rows
            with the async run tracker.
        source (str): Logical source label stored in the staging record.

    Returns:
        int: Number of raw rows written into staging for this ETL run.
    """
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "first_deposit_api",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_ms_deposit_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw MS1 deposit/activity rows into the shared staging table."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "ms_deposit_sheet",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)


async def stage_play_console_install_raw(
    session: AsyncSession,
    raw_rows: list[dict],
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist Google Play Console install/acquisition report rows."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    rows = [
        {
            "run_id": run_id,
            "source": source,
            "range_name": "play_console_gcs_export",
            "payload": item,
            "payload_hash": _payload_hash(item),
            "ingested_at": ingested_at,
        }
        for item in raw_rows
    ]
    return await _insert_staging_rows(session, rows)
