"""Extract/stage all levels, validate, then replace their window atomically."""
import json
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from sqlalchemy import delete, insert
from app.core.config import settings
from app.db.models.gpt_ads import GptAdsDaily
from app.etl.gpt_ads.client import GptAdsClient
from app.etl.gpt_ads.config import REPORT_LEVELS, resolve_gpt_ads_window
from app.etl.gpt_ads.transform import normalize_row, restore_row
from app.etl.staging import stage_ads_raw


async def run_gpt_ads(session, types="auto", start_date=None, end_date=None, run_id=None, *, client_factory=GptAdsClient):
    start, end = resolve_gpt_ads_window(types, start_date, end_date)
    today = datetime.now(ZoneInfo(settings.GPT_ADS_REPORTING_TIMEZONE)).date()
    if end >= today or start < today - timedelta(days=365) or (end - start).days >= 365:
        raise HTTPException(400, "GPT Ads requires completed days and a range of at most 365 days")
    counts = {level: 0 for level in REPORT_LEVELS}
    seen = set()
    # Keep large reports off heap; no fact-table writes until extraction is complete.
    with SpooledTemporaryFile(max_size=8*1024*1024, mode="w+", encoding="utf-8") as spool:
        async with client_factory() as client:
            account = await client.account()
            for level in REPORT_LEVELS:
                async for raw_rows, chunk_start, chunk_end in client.report(level, start, end, account["timezone"]):
                    await stage_ads_raw(session, [dict(level=level, account_id=account["id"], row=row) for row in raw_rows],
                                        run_id=run_id, source="gpt_ads", range_name=f"gpt_ads_{level}_daily")
                    await session.commit()
                    try:
                        for raw in raw_rows:
                            row = normalize_row(raw, level, account, chunk_start, chunk_end, run_id)
                            key = (level, row["date"], row["entity_id"])
                            if key in seen:
                                raise ValueError("DQ failed: GPT Ads duplicate daily entity across report pages")
                            seen.add(key)
                            spool.write(json.dumps(row, default=str) + '\n')
                            counts[level] += 1
                    except ValueError as error:
                        raise HTTPException(422, str(error)) from None
        spool.seek(0)
        try:
            await session.execute(delete(GptAdsDaily).where(
                GptAdsDaily.account_id == account["id"], GptAdsDaily.date.between(start, end)))
            batch = []
            for line in spool:
                batch.append(restore_row(json.loads(line)))
                if len(batch) >= 500:
                    await session.execute(insert(GptAdsDaily), batch)
                    batch.clear()
            if batch:
                await session.execute(insert(GptAdsDaily), batch)
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
    return f"GPT Ads updated {start} to {end}: " + ', '.join(f"{level}={count}" for level, count in counts.items()) + "."
