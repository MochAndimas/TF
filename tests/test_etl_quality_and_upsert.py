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
from app.db.models.external_api import Campaign, DataDepo, GoogleAds
from app.etl.load import build_first_deposit_rows, upsert_ads_rows, upsert_first_deposit_rows
from app.etl.quality import validate_ads_dataframe, validate_first_deposit_dataframe
from app.etl.transform import dedupe_ads_dataframe, parse_ads_dataframe, parse_first_deposit_dataframe


class TestEtlQuality(TestCase):
    """Unit tests for DQ validators."""

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

    def test_parse_first_deposit_dataframe_filters_positive_rows(self):
        df = parse_first_deposit_dataframe(
            [
                {
                    "id": 101,
                    "email": "foo@example.com",
                    "tgl_regis": "2026-01-10T05:00:00.000Z",
                    "campaignid": "",
                    "Status\nNew / Existing": "New",
                    "First Depo $": 50,
                },
                {
                    "id": 102,
                    "email": "bar@example.com",
                    "tgl_regis": "2026-01-10T05:00:00.000Z",
                    "campaignid": "cmp-1",
                    "Status\nNew / Existing": "Existing",
                    "First Depo $": 0,
                },
            ]
        )
        self.assertEqual(len(df), 1)
        self.assertEqual(int(df.iloc[0]["user_id"]), 101)
        self.assertEqual(str(df.iloc[0]["campaign_id"]), "-")
        self.assertEqual(float(df.iloc[0]["first_depo"]), 50.0)

    def test_parse_ads_dataframe_accepts_google_ads_api_payload(self):
        df = parse_ads_dataframe(
            [
                {
                    "date": "2026-01-10",
                    "campaign_id": "123456",
                    "campaign_name": "GG - UA - Test",
                    "ad_group": "Ad Group 1",
                    "ad_name": "Ad 1",
                    "cost": 12.5,
                    "impressions": 100,
                    "clicks": 5,
                    "leads": 2,
                }
            ]
        )
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["date"], date(2026, 1, 10))
        self.assertEqual(row["campaign_id"], "123456")
        self.assertEqual(row["campaign_name"], "GG - UA - Test")
        self.assertEqual(int(row["impressions"]), 100)
        self.assertEqual(int(row["clicks"]), 5)
        self.assertEqual(int(row["leads"]), 2)

    def test_parse_first_deposit_dataframe_maps_optional_fields(self):
        df = parse_first_deposit_dataframe(
            [
                {
                    "id": 201,
                    "fullname": "Test User",
                    "email": "USER@Example.COM",
                    "phone": 8123456789,
                    "tgl_regis": "2026-01-10T05:00:00.000Z",
                    "tag": "CP1Google",
                    "campaignid": "cmp-10",
                    "protection": 1,
                    "Status\nNew / Existing": "New",
                    "Assign Date": "2026-01-12T08:00:00.000Z",
                    "Analyst": 2,
                    "First Depo Date": "2026-01-13T09:00:00.000Z",
                    "First Depo $": 125,
                    "Time To Closing": "25:00:00",
                    "NMI": 50.5,
                    "Lot": 0.75,
                    "Cabang": "TFJ",
                    "Pool": True,
                }
            ]
        )
        self.assertEqual(len(df), 1)
        row = df.iloc[0]
        self.assertEqual(row["fullname"], "Test User")
        self.assertEqual(row["email"], "user@example.com")
        self.assertEqual(row["phone"], "8123456789")
        self.assertEqual(row["tag"], "CP1Google")
        self.assertEqual(int(row["protection"]), 1)
        self.assertEqual(row["assign_date"], date(2026, 1, 12))
        self.assertEqual(int(row["analyst"]), 2)
        self.assertEqual(row["first_depo_date"], date(2026, 1, 13))
        self.assertEqual(row["time_to_closing"], "25:00:00")
        self.assertEqual(float(row["nmi"]), 50.5)
        self.assertEqual(float(row["lot"]), 0.75)
        self.assertEqual(row["cabang"], "TFJ")
        self.assertTrue(bool(row["pool"]))

    def test_build_first_deposit_rows_includes_extended_data_depo_fields(self):
        df = pd.DataFrame(
            [
                {
                    "user_id": 201,
                    "tanggal_regis": date(2026, 1, 10),
                    "fullname": "Test User",
                    "email": "user@example.com",
                    "phone": "8123456789",
                    "user_status": "New",
                    "campaign_id": "cmp-10",
                    "tag": "CP1Google",
                    "protection": 1,
                    "assign_date": date(2026, 1, 12),
                    "analyst": 2,
                    "first_depo_date": date(2026, 1, 13),
                    "first_depo": 125.0,
                    "time_to_closing": "25:00:00",
                    "nmi": 50.5,
                    "lot": 0.75,
                    "cabang": "TFJ",
                    "pool": True,
                }
            ]
        )
        rows = build_first_deposit_rows(df, pull_date=date(2026, 1, 14))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["fullname"], "Test User")
        self.assertEqual(rows[0]["phone"], "8123456789")
        self.assertEqual(rows[0]["tag"], "CP1Google")
        self.assertEqual(rows[0]["assign_date"], date(2026, 1, 12))
        self.assertEqual(rows[0]["first_depo_date"], date(2026, 1, 13))
        self.assertEqual(rows[0]["time_to_closing"], "25:00:00")
        self.assertEqual(rows[0]["nmi"], 50.5)
        self.assertEqual(rows[0]["lot"], 0.75)
        self.assertEqual(rows[0]["cabang"], "TFJ")
        self.assertTrue(rows[0]["pool"])

    def test_validate_first_deposit_dataframe_raises_on_non_positive_metric(self):
        df = pd.DataFrame(
            [
                {
                    "user_id": 1,
                    "tanggal_regis": date(2026, 1, 1),
                    "campaign_id": "-",
                    "email": "foo@example.com",
                    "user_status": "New",
                    "first_depo": 0.0,
                }
            ]
        )
        with self.assertRaisesRegex(ValueError, "non-positive first deposit"):
            validate_first_deposit_dataframe(df)


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

    async def test_upsert_first_deposit_rows_is_idempotent_and_creates_placeholder_campaign(self):
        async with self.session_factory() as session:
            rows_v1 = [
                {
                    "user_id": 10,
                    "tanggal_regis": date(2026, 1, 5),
                    "campaign_id": "-",
                    "email": "foo@example.com",
                    "user_status": "New",
                    "first_depo": 50.0,
                    "pull_date": date(2026, 1, 6),
                }
            ]
            await upsert_first_deposit_rows(session=session, rows=rows_v1)
            await session.commit()

            rows_v2 = [
                {
                    "user_id": 10,
                    "tanggal_regis": date(2026, 1, 5),
                    "campaign_id": "-",
                    "email": "foo@example.com",
                    "user_status": "Existing",
                    "first_depo": 75.0,
                    "pull_date": date(2026, 1, 7),
                }
            ]
            await upsert_first_deposit_rows(session=session, rows=rows_v2)
            await session.commit()

            campaign_result = await session.execute(select(Campaign).where(Campaign.campaign_id == "-"))
            campaign = campaign_result.scalar_one_or_none()
            self.assertIsNotNone(campaign)

            result = await session.execute(
                select(DataDepo).where(
                    DataDepo.user_id == 10,
                    DataDepo.tanggal_regis == date(2026, 1, 5),
                    DataDepo.campaign_id == "-",
                )
            )
            rows = result.scalars().all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].first_depo, 75.0)
            self.assertEqual(rows[0].user_status, "Existing")
            self.assertEqual(rows[0].pull_date, date(2026, 1, 7))
