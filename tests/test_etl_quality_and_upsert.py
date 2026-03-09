"""Tests for ETL quality checks and upsert idempotency."""

from __future__ import annotations

from datetime import date, datetime
from unittest import IsolatedAsyncioTestCase, TestCase

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.base import SqliteBase
from app.db.models.external_api import Campaign, GoogleAds
from app.etl.load import upsert_ads_rows
from app.etl.quality import validate_ads_dataframe, validate_depo_dataframe
from app.etl.transform import dedupe_ads_dataframe


class TestEtlQuality(TestCase):
    """Unit tests for DQ validators."""

    def test_validate_depo_dataframe_raises_on_duplicate_business_key(self):
        df = pd.DataFrame(
            [
                {
                    "id": 1,
                    "tgl_regis": date(2026, 1, 1),
                    "campaignid": "abc",
                    "First Depo $": 100.0,
                },
                {
                    "id": 1,
                    "tgl_regis": date(2026, 1, 1),
                    "campaignid": "abc",
                    "First Depo $": 200.0,
                },
            ]
        )
        with self.assertRaisesRegex(ValueError, "duplicate key ratio"):
            validate_depo_dataframe(df)

    def test_validate_ads_dataframe_raises_on_negative_metric(self):
        df = pd.DataFrame(
            [
                {
                    "date": date(2026, 1, 1),
                    "campaign_id": "abc",
                    "ad_group": "grp",
                    "ad_name": "name",
                    "cost": -1.0,
                    "impressions": 10,
                    "clicks": 1,
                    "leads": 1,
                }
            ]
        )
        with self.assertRaisesRegex(ValueError, "negative metrics"):
            validate_ads_dataframe(df)

    def test_dedupe_ads_dataframe_keeps_last_duplicate(self):
        df = pd.DataFrame(
            [
                {
                    "date": date(2026, 1, 1),
                    "campaign_id": "abc",
                    "campaign_name": "TT - UA - Test",
                    "ad_group": "grp",
                    "ad_name": "ad",
                    "cost": 10.5,
                    "impressions": 100,
                    "clicks": 10,
                    "leads": 1,
                },
                {
                    "date": date(2026, 1, 1),
                    "campaign_id": "abc",
                    "campaign_name": "TT - UA - Test",
                    "ad_group": "grp",
                    "ad_name": "ad",
                    "cost": 20.0,
                    "impressions": 200,
                    "clicks": 20,
                    "leads": 2,
                },
            ]
        )
        deduped, dropped_count = dedupe_ads_dataframe(df)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(dropped_count, 1)
        self.assertEqual(float(deduped.iloc[0]["cost"]), 20.0)
        self.assertEqual(int(deduped.iloc[0]["impressions"]), 200)
        self.assertEqual(int(deduped.iloc[0]["clicks"]), 20)
        self.assertEqual(int(deduped.iloc[0]["leads"]), 2)


class TestEtlUpsert(IsolatedAsyncioTestCase):
    """Integration-like tests for SQLite upsert behavior."""

    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            poolclass=StaticPool,
            future=True,
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(SqliteBase.metadata.create_all)

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_upsert_ads_rows_is_idempotent_and_updates_metrics(self):
        async with self.session_factory() as session:
            session.add(
                Campaign(
                    campaign_id="cmp-1",
                    campaign_name="GG - UA - Test",
                    ad_source="google_ads",
                    ad_type="user_acquisition",
                    created_at=datetime.now(),
                )
            )
            await session.commit()

            rows_v1 = [
                {
                    "date": date(2026, 1, 1),
                    "campaign_id": "cmp-1",
                    "campaign_name": "GG - UA - Test",
                    "ad_group": "group-1",
                    "ad_name": "ad-1",
                    "cost": 10.0,
                    "impressions": 100,
                    "clicks": 10,
                    "leads": 2,
                    "pull_date": date(2026, 1, 2),
                }
            ]
            await upsert_ads_rows(session=session, model_cls=GoogleAds, rows=rows_v1)
            await session.commit()

            rows_v2 = [
                {
                    "date": date(2026, 1, 1),
                    "campaign_id": "cmp-1",
                    "campaign_name": "GG - UA - Test",
                    "ad_group": "group-1",
                    "ad_name": "ad-1",
                    "cost": 25.5,
                    "impressions": 150,
                    "clicks": 12,
                    "leads": 3,
                    "pull_date": date(2026, 1, 3),
                }
            ]
            await upsert_ads_rows(session=session, model_cls=GoogleAds, rows=rows_v2)
            await session.commit()

            result = await session.execute(
                select(GoogleAds).where(
                    GoogleAds.date == date(2026, 1, 1),
                    GoogleAds.campaign_id == "cmp-1",
                    GoogleAds.ad_group == "group-1",
                    GoogleAds.ad_name == "ad-1",
                )
            )
            rows = result.scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].cost, 25.5)
            self.assertEqual(rows[0].impressions, 150)
            self.assertEqual(rows[0].pull_date, date(2026, 1, 3))
