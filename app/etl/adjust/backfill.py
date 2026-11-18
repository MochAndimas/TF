"""Backfill missing campaign identifiers using the shared Adjust normalizer."""
import logging

from sqlalchemy import func, or_, select, update

from app.db.base import SqliteBase
from app.db.models.adjust import AdjustCallbackRaw
from app.etl.adjust.common import normalize_context

logger = logging.getLogger(__name__)


async def backfill_campaign_ids(connection) -> dict[str, int]:
    """Fill missing IDs across Adjust fact tables without replaying events."""
    totals = {}
    raw = AdjustCallbackRaw.__table__
    for table in SqliteBase.metadata.tables.values():
        required = {"campaign_id", "campaign_name", "source_name", "raw_id"}
        if not table.name.startswith("adjust_") or not required <= set(table.c.keys()):
            continue
        primary_key = next(iter(table.primary_key.columns))
        missing = or_(
            table.c.campaign_id.is_(None),
            func.trim(table.c.campaign_id) == "",
            table.c.campaign_id.contains("{"),
            table.c.campaign_id.contains("}"),
        )
        cursor = None
        changed = 0
        while True:
            query = (
                select(primary_key, table.c.campaign_name, table.c.source_name, raw.c.payload)
                .outerjoin(raw, table.c.raw_id == raw.c.id)
                .where(missing)
                .order_by(primary_key)
                .limit(500)
            )
            if cursor is not None:
                query = query.where(primary_key > cursor)
            rows = (await connection.execute(query)).mappings().all()
            if not rows:
                break
            for row in rows:
                payload = row["payload"] or {}
                normalized = normalize_context({
                    "campaign_id": payload.get("campaign_id"),
                    "campaign_name": row["campaign_name"] or payload.get("campaign_name"),
                    "network_name": row["source_name"] or payload.get("network_name"),
                })
                if normalized["campaign_id"] is None:
                    continue
                result = await connection.execute(
                    update(table)
                    .where(primary_key == row[primary_key.name], missing)
                    .values(
                        campaign_id=normalized["campaign_id"],
                        campaign_name=normalized["campaign_name"],
                    )
                )
                changed += result.rowcount
            cursor = rows[-1][primary_key.name]
        totals[table.name] = changed
        logger.info("Adjust campaign ID backfill: %s, %s updated", table.name, changed)
    return totals
