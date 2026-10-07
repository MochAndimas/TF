"""Run with python -m unittest scripts.verification.test_adjust_callbacks."""
import unittest
from datetime import datetime
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.api.v1.endpoint.adjust import router
from app.core.config import settings
from app.db.models.adjust import AdjustCallbackRaw, AdjustInstall
from app.db.session import get_db
from app.etl.adjust.pipeline import process_callbacks
from app.etl.adjust.transform import normalize_install

PAYLOAD = dict(app_token="test-app", environment="sandbox", activity_kind="install", adid="test-device", installed_at="1791219600")


class AdjustTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as connection:
            for model in (AdjustCallbackRaw, AdjustInstall):
                await connection.run_sync(model.__table__.create)
        self.factory = async_sessionmaker(self.engine, expire_on_commit=False)
        self.previous = {name: getattr(settings, name) for name in ("ADJUST_CALLBACK_SECRET", "ADJUST_APP_TOKENS", "ADJUST_CALLBACK_ENVIRONMENT", "ADJUST_CALLBACK_ALLOWED_CIDRS")}
        settings.ADJUST_CALLBACK_SECRET = "test-secret"
        settings.ADJUST_APP_TOKENS = "test-app"
        settings.ADJUST_CALLBACK_ENVIRONMENT = "sandbox"
        settings.ADJUST_CALLBACK_ALLOWED_CIDRS = ""
        app = FastAPI()
        app.include_router(router)
        async def database():
            async with self.factory() as session:
                yield session
        app.dependency_overrides[get_db] = database
        self.client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.engine.dispose()
        for name, value in self.previous.items():
            setattr(settings, name, value)

    async def send(self, payload=PAYLOAD, token="test-secret"):
        return await self.client.post("/api/adjust/callback", params={"token": token}, json=payload)

    async def test_delivery_retry_and_semantic_deduplication(self):
        self.assertEqual((await self.send()).status_code, 202)
        self.assertEqual((await self.send()).status_code, 202)
        self.assertEqual((await self.send(dict(PAYLOAD, campaign_name="changed"))).status_code, 202)
        async with self.factory() as session:
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustCallbackRaw)), 2)
            await process_callbacks(session)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)

    async def test_auth_environment_app_and_disabled_receiver(self):
        self.assertEqual((await self.send(token="wrong")).status_code, 403)
        self.assertEqual((await self.send(dict(PAYLOAD, environment="production"))).status_code, 400)
        self.assertEqual((await self.send(dict(PAYLOAD, app_token="other"))).status_code, 400)
        settings.ADJUST_CALLBACK_ALLOWED_CIDRS = "192.0.2.0/24"
        self.assertEqual((await self.send()).status_code, 403)
        settings.ADJUST_CALLBACK_SECRET = ""
        self.assertEqual((await self.send()).status_code, 503)

    async def test_invalid_callback_retained_and_manual_replay(self):
        await self.send(dict(PAYLOAD, installed_at="invalid"))
        await self.send()
        async with self.factory() as session:
            with self.assertRaises(Exception) as failure:
                await process_callbacks(session)
            self.assertEqual(failure.exception.status_code, 422)
            failed = await session.scalar(select(AdjustCallbackRaw).where(AdjustCallbackRaw.status == "failed"))
            self.assertIsNotNone(failed)
            await process_callbacks(session)
            self.assertEqual(failed.attempts, 1)
            failed.payload = dict(PAYLOAD, adid="repaired-device")
            await session.commit()
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(failed.status, "processed")
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 2)

    async def test_wib_boundary_and_flat_json(self):
        fact = normalize_install(dict(PAYLOAD, installed_at="1704128400"), 1)
        self.assertEqual(str(fact["date"]), "2024-01-02")
        self.assertEqual((await self.send(dict(PAYLOAD, nested={}))).status_code, 400)
        response = await self.client.post("/api/adjust/callback?token=test-secret", content=b"x" * 1048577, headers={"content-type": "application/json"})
        self.assertEqual(response.status_code, 413)

    async def test_batch_boundary_and_old_pending_data(self):
        async with self.factory() as session:
            for index in range(205):
                session.add(AdjustCallbackRaw(payload_hash=str(index), payload=dict(PAYLOAD, adid=f"device-{index}"), received_at=datetime(2020, 1, 1), status="pending", attempts=0))
            await session.commit()
            await process_callbacks(session)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 205)

    async def test_additive_migration_is_idempotent(self):
        from app.db.bootstrap import _migration_20261005_001_adjust_callbacks
        async with self.engine.begin() as connection:
            await _migration_20261005_001_adjust_callbacks(connection)
            await _migration_20261005_001_adjust_callbacks(connection)

    async def test_campaign_id_optional_and_enrichment(self):
        self.assertIsNone(normalize_install(PAYLOAD, 1)["campaign_id"])
        self.assertIsNone(normalize_install(dict(PAYLOAD, campaign_id="{dcp_campaign_id}"), 1)["campaign_id"])
        self.assertEqual(normalize_install(dict(PAYLOAD, campaign_id="001234"), 1)["campaign_id"], "001234")
        await self.send()
        async with self.factory() as session:
            await process_callbacks(session)
        await self.send(dict(PAYLOAD, campaign_id="001234"))
        async with self.factory() as session:
            await process_callbacks(session)
            fact = await session.scalar(select(AdjustInstall))
            self.assertEqual(fact.campaign_id, "001234")
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)
        await self.send(dict(PAYLOAD, campaign_id="different"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual((await session.scalar(select(AdjustInstall))).campaign_id, "001234")
            from app.db.bootstrap import _migration_20261005_002_adjust_campaign_id
            async with self.engine.begin() as connection:
                await _migration_20261005_002_adjust_campaign_id(connection)
                await _migration_20261005_002_adjust_campaign_id(connection)

    async def test_google_campaign_id_fallback_and_replay(self):
        payload = dict(PAYLOAD, network_name="Google+Ads+Performance+Max", campaign_name="Campaign+name+%2823774379523%29")
        fact = normalize_install(payload, 1)
        self.assertEqual(fact["campaign_id"], "23774379523")
        self.assertEqual(fact["campaign_name"], "Campaign name")
        self.assertEqual(fact["source_name"], "google")
        self.assertEqual(normalize_install(dict(payload, campaign_id="001"), 1)["campaign_id"], "001")
        self.assertIsNone(normalize_install(dict(payload, network_name="Organic"), 1)["campaign_id"])
        self.assertIsNone(normalize_install(dict(payload, campaign_name="Campaign (not-an-id)"), 1)["campaign_id"])
        await self.send(payload)
        async with self.factory() as session:
            await process_callbacks(session)
            fact = await session.scalar(select(AdjustInstall))
            fact.campaign_id = None
            fact.source_name = payload["network_name"]
            fact.campaign_name = payload["campaign_name"]
            await session.commit()
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            await session.refresh(fact)
            self.assertEqual(fact.campaign_id, "23774379523")
            self.assertEqual(fact.source_name, "google")
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)

    async def test_source_aliases_and_campaign_name_separation(self):
        from app.etl.adjust.transform import normalize_source_name
        for original, expected in (("Facebook Installs", "facebook"), ("Meta Ads", "facebook"), ("TikTok Installs", "tiktok"), ("Organic", "organic"), ("Google Ads ACI", "google"), ("Snapchat Installs", "snapchat")):
            self.assertEqual(normalize_source_name(original), expected)
        payload = dict(PAYLOAD, network_name="Facebook Installs", campaign_id="00123", campaign_name="Campaign (00123)")
        fact = normalize_install(payload, 1)
        self.assertEqual(fact["campaign_name"], "Campaign")
        self.assertEqual(fact["campaign_id"], "00123")
        self.assertNotIn("network_name", fact)
        self.assertEqual(normalize_install(dict(payload, campaign_name="Campaign (2026)"), 1)["campaign_name"], "Campaign (2026)")
