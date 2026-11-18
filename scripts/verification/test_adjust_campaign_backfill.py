"""Regression checks for Adjust campaign ID extraction and historical migration."""
import asyncio
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from app.db.base import SqliteBase
from app.db.models.adjust import AdjustCallbackRaw
from app.etl.adjust.backfill import backfill_campaign_ids
from app.etl.adjust.common import normalize_context


def test_paid_campaign_suffix_and_explicit_id_precedence():
    for source in ("Facebook Installs", "Meta Ads", "TikTok Installs", "Google Ads"):
        row = normalize_context({
            "network_name": source,
            "campaign_name": "Campaign+name+%28001234%29",
            "campaign_id": "{campaign_id}",
        })
        assert row["campaign_id"] == "001234"
        assert row["campaign_name"] == "Campaign name"
        assert normalize_context({
            "network_name": source, "campaign_name": "Campaign (001234)", "campaign_id": "keep",
        })["campaign_id"] == "keep"
    assert normalize_context({"network_name": "Organic", "campaign_name": "Campaign (2026)"})["campaign_id"] is None
    assert normalize_context({"network_name": "Facebook", "campaign_name": "Campaign (not-an-id)"})["campaign_id"] is None


def test_backfill_covers_all_facts_and_is_retry_safe():
    async def run():
        engine = create_async_engine("sqlite+aiosqlite://")
        facts = [
            table for table in SqliteBase.metadata.tables.values()
            if table.name.startswith("adjust_") and "campaign_id" in table.c
        ]
        async with engine.begin() as conn:
            await conn.run_sync(AdjustCallbackRaw.__table__.create)
            for table in facts:
                await conn.run_sync(table.create)
            await conn.execute(AdjustCallbackRaw.__table__.insert(), [
                {"id": 1, "payload_hash": "one", "payload": {}, "received_at": datetime.now(), "status": "processed", "attempts": 1},
                {"id": 2, "payload_hash": "two", "payload": {"campaign_id": "00999"}, "received_at": datetime.now(), "status": "processed", "attempts": 1},
            ])
            for table in facts:
                primary = next(iter(table.primary_key.columns))
                rows = []
                for index in range(4):
                    row = {}
                    for col in table.c:
                        if not col.nullable and col.default is None:
                            row[col.name] = {
                                str: f"value-{index}", int: 1, float: 1.0,
                                dict: {}, date: date.today(), datetime: datetime.now(),
                            }[col.type.python_type]
                    row[primary.name] = f"key-{index}"
                    row.update(raw_id=1, source_name="facebook", campaign_name="Campaign (001234)", campaign_id=None)
                    rows.append(row)
                rows[1]["campaign_id"] = "keep"
                rows[2]["source_name"] = "organic"
                rows[3].update(raw_id=2, campaign_id="{campaign_id}", campaign_name=None)
                await conn.execute(table.insert(), rows)
            updated = await backfill_campaign_ids(conn)
            assert len(updated) == 9
            assert all(count == 2 for count in updated.values())
            for table in facts:
                primary = next(iter(table.primary_key.columns))
                rows = (await conn.execute(select(table).order_by(primary))).mappings().all()
                assert [row["campaign_id"] for row in rows] == ["001234", "keep", None, "00999"]
                assert rows[0]["campaign_name"] == "Campaign"
                assert rows[1]["campaign_name"] == "Campaign (001234)"
            assert all(count == 0 for count in (await backfill_campaign_ids(conn)).values())
        await engine.dispose()
    asyncio.run(run())
