"""Staging-layer utilities for raw payload persistence."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import StgAdsRaw, StgDepoRaw
from app.etl.transform import normalize_columns


def _payload_hash(payload) -> str:
    serialized = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


async def stage_depo_raw(
    session: AsyncSession,
    raw_data: list,
    *,
    run_id: str | None,
    source: str,
) -> int:
    """Persist raw deposit payload into staging table."""
    if not raw_data:
        return 0

    ingested_at = datetime.now()
    rows = [
        StgDepoRaw(
            run_id=run_id,
            source=source,
            payload=item,
            payload_hash=_payload_hash(item),
            ingested_at=ingested_at,
        )
        for item in raw_data
    ]
    session.add_all(rows)
    await session.flush()
    return len(rows)


async def stage_ads_raw(
    session: AsyncSession,
    raw_rows: list,
    *,
    run_id: str | None,
    source: str,
    range_name: str,
) -> int:
    """Persist raw ads sheet rows into staging table."""
    if not raw_rows:
        return 0

    ingested_at = datetime.now()
    headers = normalize_columns(raw_rows[0]) if raw_rows else []
    payloads = []
    if len(raw_rows) < 2:
        payloads = [{"_raw": row} for row in raw_rows]
    else:
        payloads = [dict(zip(headers, row)) for row in raw_rows[1:]]

    rows = [
        StgAdsRaw(
            run_id=run_id,
            source=source,
            range_name=range_name,
            payload=item,
            payload_hash=_payload_hash(item),
            ingested_at=ingested_at,
        )
        for item in payloads
    ]
    if not rows:
        return 0

    session.add_all(rows)
    await session.flush()
    return len(rows)

