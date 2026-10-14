"""Bounded-batch processing of the durable inbox with explicit manual replay."""
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from sqlalchemy import func, select
from app.db.models.adjust import AdjustCallbackRaw
from app.etl.adjust.registry import CALLBACK_HANDLERS
from app.etl.transform import resolve_date_window


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def process_callbacks(session, types="auto", start_date=None, end_date=None, run_id=None):
    if types not in {"auto", "manual"}:
        raise HTTPException(400, "Invalid Adjust processing mode")
    high_water = await session.scalar(select(func.max(AdjustCallbackRaw.id))) or 0
    filters = [AdjustCallbackRaw.id <= high_water]
    if types == "auto":
        filters.append(AdjustCallbackRaw.status == "pending")
    else:
        start, end = resolve_date_window(types, start_date, end_date)
        zone = ZoneInfo("Asia/Jakarta")
        lower = datetime.combine(start, time.min, zone).astimezone(timezone.utc).replace(tzinfo=None)
        upper = datetime.combine(end + timedelta(days=1), time.min, zone).astimezone(timezone.utc).replace(tzinfo=None)
        filters.extend([AdjustCallbackRaw.received_at >= lower, AdjustCallbackRaw.received_at < upper])
    cursor = processed = failures = 0
    while True:
        rows = (await session.scalars(select(AdjustCallbackRaw).where(*filters, AdjustCallbackRaw.id > cursor).order_by(AdjustCallbackRaw.id).limit(200))).all()
        if not rows:
            break
        for raw in rows:
            raw.attempts += 1
            raw.run_id = run_id
            try:
                kind = str(raw.payload.get("activity_kind", "")).strip().lower()
                handler = CALLBACK_HANDLERS.get(kind)
                if handler is None:
                    raise ValueError("Unsupported Adjust activity_kind")
                fact = handler.normalize(raw.payload, raw.id)
            except ValueError as error:
                raw.status, raw.error_detail = "failed", str(error)[:500]
                failures += 1
            else:
                await handler.load(session, fact)
                raw.status, raw.error_detail = "processed", None
                processed += 1
            raw.processed_at = utc_now()
            cursor = raw.id
        await session.commit()
    if failures:
        raise HTTPException(422, f"DQ failed: Adjust processed {processed} callbacks; {failures} invalid callbacks retained for manual replay.")
    return f"Adjust processed {processed} callbacks."
