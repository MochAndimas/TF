"""Run with python -m unittest scripts.verification.test_adjust_callbacks."""
import unittest
from datetime import datetime
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.api.v1.endpoint.adjust import router
from app.core.config import settings
from app.db.models.adjust import AdjustCallbackRaw, AdjustInstall, AdjustLifecycle, AdjustAttributionHistory, AdjustEngagement
from app.db.models.adjust import AdjustCost
from app.etl.adjust.handlers.cost import normalize_cost
from app.db.session import get_db
from app.etl.adjust.pipeline import process_callbacks
from app.etl.adjust.transform import normalize_install, normalize_lifecycle
from app.etl.adjust.registry import count_callback_facts
from app.etl.adjust.handlers.attribution import normalize_attribution
from app.etl.adjust.handlers.engagement import normalize_engagement
from app.etl.adjust.handlers.event import BUSINESS_EVENTS, EVENT_MODELS, normalize_event

PAYLOAD = dict(app_token="test-app", environment="sandbox", activity_kind="install", adid="test-device", installed_at="1791219600")


class AdjustTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with self.engine.begin() as connection:
            for model in (AdjustCallbackRaw, AdjustInstall, AdjustLifecycle, AdjustAttributionHistory, AdjustEngagement, AdjustCost, *EVENT_MODELS.values()):
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

    async def test_optional_device_fields_and_enrichment(self):
        device = dict(gps_adid="google-id", idfa="ios-id", city="Jakarta+Selatan",
                      country="id", device_name="Pixel%209", os_version="15",
                      isp="Provider+Internet", mcc="510", mnc="01")
        fact = normalize_install(dict(PAYLOAD, **device), 1)
        self.assertEqual(fact["city"], "Jakarta Selatan")
        self.assertEqual(fact["device_name"], "Pixel 9")
        self.assertEqual(fact["isp"], "Provider Internet")
        self.assertEqual(fact["mnc"], "01")
        self.assertEqual(fact["adid"], PAYLOAD["adid"])
        for field in device:
            self.assertIsNone(normalize_install(PAYLOAD, 1)[field])
            self.assertIsNone(normalize_install(dict(PAYLOAD, **{field: "{" + field + "}"}), 1)[field])
        await self.send()
        async with self.factory() as session:
            await process_callbacks(session)
        await self.send(dict(PAYLOAD, **device))
        async with self.factory() as session:
            await process_callbacks(session)
            fact = await session.scalar(select(AdjustInstall))
            self.assertEqual(fact.gps_adid, "google-id")
            self.assertEqual(fact.mnc, "01")
            self.assertEqual(fact.city, "Jakarta Selatan")
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)
        await self.send(dict(PAYLOAD, **dict(device, gps_adid="other-id")))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual((await session.scalar(select(AdjustInstall))).gps_adid, "google-id")

    async def test_mixed_activity_dispatch_and_lifecycle_replay(self):
        self.assertEqual((await self.send()).status_code, 202)
        for kind, timestamp in (("uninstall", "uninstalled_at"), ("reinstall", "reinstalled_at")):
            payload = dict(app_token="test-app", environment="sandbox", adid="test-device",
                           activity_kind=kind, created_at="1791219600", **({timestamp: "1791219600"} if timestamp != "created_at" else {}))
            self.assertEqual((await self.send(payload)).status_code, 202)
            self.assertEqual((await self.send(payload)).status_code, 202)
            self.assertEqual((await self.send(dict(payload, city="Jakarta"))).status_code, 202)
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 1)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustLifecycle)), 2)
            self.assertEqual(await count_callback_facts(session), 3)
            from app.etl.job_runner import count_source_rows
            self.assertEqual(await count_source_rows(session, "adjust_callbacks"), 3)
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await count_callback_facts(session), 3)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustCallbackRaw).where(AdjustCallbackRaw.status == "processed")), 5)

    async def test_distinct_uninstalls_and_profile_enrichment(self):
        payload = dict(app_token="test-app", environment="sandbox", adid="test-device",
                       activity_kind="uninstall", uninstalled_at="1791219600", created_at="1791219600")
        await self.send(payload)
        async with self.factory() as session:
            await process_callbacks(session)
        profile = dict(username="Test User", email="test+tag@example.com", nomor_telepon="+62001234",
                       gps_adid="google-id", idfa="ios-id", mnc="01")
        await self.send(dict(payload, **profile))
        await self.send(dict(payload, **dict(profile, username="changed")))
        await self.send(dict(payload, created_at="1791221400", uninstalled_at="1791221400"))
        async with self.factory() as session:
            await process_callbacks(session)
            facts = (await session.scalars(select(AdjustLifecycle).order_by(AdjustLifecycle.occurred_at))).all()
            self.assertEqual(len(facts), 2)
            self.assertEqual(facts[0].username, "Test User")
            self.assertEqual(facts[0].email, "test+tag@example.com")
            self.assertEqual(facts[0].nomor_telepon, "+62001234")
            self.assertEqual(facts[0].mnc, "01")
            self.assertIsNone(facts[1].username)

    async def test_lifecycle_timestamps_wib_and_optional_placeholders(self):
        payload = dict(app_token="test-app", environment="sandbox", adid="test-device",
                       activity_kind="uninstall", created_at="1704214800", uninstalled_at="1704128400",
                       gps_adid="{gps_adid}", idfa="", username="[username]", email=None,
                       nomor_telepon="{nomor_telepon}")
        fact = normalize_lifecycle(payload, 1)
        self.assertEqual(str(fact["date"]), "2024-01-02")
        self.assertNotEqual(fact["created_at"], fact["occurred_at"])
        for field in ("gps_adid", "idfa", "username", "email", "nomor_telepon"):
            self.assertIsNone(fact[field])
        for field, value in (("uninstalled_at", "bad"), ("created_at", "NaN"), ("adid", ""), ("time_to_uninstall", "-1")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                normalize_lifecycle(dict(payload, **{field: value}), 1)
        with self.assertRaises(ValueError):
            normalize_lifecycle(dict(payload, activity_kind="reinstall"), 1)

    async def test_lifecycle_invalid_rows_commit_valid_and_manual_repair(self):
        payload = dict(app_token="test-app", environment="sandbox", adid="test-device",
                       activity_kind="uninstall", uninstalled_at="1791219600", created_at="invalid")
        await self.send(payload)
        await self.send(dict(payload, adid="valid-device", created_at="1791219600"))
        async with self.factory() as session:
            with self.assertRaises(Exception) as failure:
                await process_callbacks(session)
            self.assertEqual(failure.exception.status_code, 422)
            self.assertEqual(await count_callback_facts(session), 1)
            failed = await session.scalar(select(AdjustCallbackRaw).where(AdjustCallbackRaw.status == "failed"))
            await process_callbacks(session)
            self.assertEqual(failed.attempts, 1)
            failed.payload = dict(payload, created_at="1791219600")
            await session.commit()
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(failed.status, "processed")
            self.assertEqual(await count_callback_facts(session), 2)

    async def test_custom_profile_query_params_and_body_precedence(self):
        payload = dict(app_token="test-app", environment="sandbox", adid="test-device",
                       activity_kind="uninstall", uninstalled_at="1791219600", created_at="1791219600", username="Body User")
        response = await self.client.post("/api/adjust/callback", json=payload,
            params=dict(token="test-secret", username="Query User", email="test+tag@example.com", nomor_telepon="+62001"))
        self.assertEqual(response.status_code, 202)
        async with self.factory() as session:
            await process_callbacks(session)
            fact = await session.scalar(select(AdjustLifecycle))
            self.assertEqual(fact.username, "Body User")
            self.assertEqual(fact.email, "test+tag@example.com")
            self.assertEqual(fact.nomor_telepon, "+62001")

    async def test_unsupported_activities_remain_rejected(self):
        for kind in ("session", "unsupported_cost", "unknown"):
            self.assertEqual((await self.send(dict(PAYLOAD, activity_kind=kind))).status_code, 400)
        async with self.engine.begin() as connection:
            await connection.run_sync(AdjustLifecycle.__table__.drop)
            from app.db.base import SqliteBase
            await connection.run_sync(SqliteBase.metadata.create_all)

    async def test_attribution_dispatch_replay_and_install_preservation(self):
        await self.send(dict(PAYLOAD, network_name="Organic"))
        base = dict(app_token="test-app", environment="sandbox", adid="test-device",
                    created_at="1791219600", tracker_token="new-tracker", network_name="Google Ads",
                    campaign_name="New campaign (123)", username="Test User", email="test+tag@example.com",
                    nomor_telepon="+62001", gps_adid="google-id", mnc="01")
        for kind, extra in (
            ("reattribution", dict(reattributed_at="1791219600")),
            ("attribution_update", dict(attribution_updated_at="1791219600", outdated_tracker_token="old-tracker")),
            ("reattribution_reinstall", dict(reattributed_at="1791219600", reinstalled_at="1791219600")),
        ):
            payload = dict(base, activity_kind=kind, **extra)
            self.assertEqual((await self.send(payload)).status_code, 202)
            await self.send(payload)
            await self.send(dict(payload, city="Jakarta"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 4)
            self.assertEqual((await session.scalar(select(AdjustInstall))).source_name, "organic")
            facts = (await session.scalars(select(AdjustAttributionHistory))).all()
            self.assertEqual(len(facts), 3)
            for fact in facts:
                self.assertEqual(fact.email, "test+tag@example.com")
                self.assertEqual(fact.nomor_telepon, "+62001")
                self.assertEqual(fact.campaign_id, "123")
                self.assertEqual(fact.city, "Jakarta")
                self.assertEqual(fact.mnc, "01")
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await count_callback_facts(session), 4)

    async def test_attribution_distinct_changes_and_alias(self):
        base = dict(app_token="test-app", environment="sandbox", adid="test-device",
                    activity_kind="attribution_update", created_at="1791219600", attribution_updated_at="1791219600",
                    tracker_token="one", outdated_tracker_token="old")
        await self.send(base)
        await self.send(dict(base, activity_kind="updated_attribution"))
        await self.send(dict(base, tracker_token="two"))
        await self.send(dict(base, attribution_updated_at="1791221400"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustAttributionHistory)), 3)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 0)

    async def test_attribution_validation_and_wib_date(self):
        payload = dict(app_token="test-app", environment="sandbox", adid="test-device", activity_kind="reattribution",
                       created_at="1704214800", reattributed_at="1704128400", last_tracker_name="Old+Campaign",
                       username="{username}", email="[email]", nomor_telepon="")
        fact = normalize_attribution(payload, 1)
        self.assertEqual(str(fact["date"]), "2024-01-02")
        self.assertEqual(fact["last_tracker_name"], "Old Campaign")
        self.assertIsNone(fact["username"])
        self.assertIsNone(fact["email"])
        self.assertIsNone(fact["nomor_telepon"])
        for field in ("adid", "reattributed_at", "created_at"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                normalize_attribution(dict(payload, **{field: "invalid" if field != "adid" else ""}), 1)
        with self.assertRaises(ValueError):
            normalize_attribution(dict(payload, activity_kind="reattribution_reinstall"), 1)
        await self.send(dict(payload, reattributed_at="invalid"))
        await self.send(payload)
        async with self.factory() as session:
            with self.assertRaises(Exception) as failure:
                await process_callbacks(session)
            self.assertEqual(failure.exception.status_code, 422)
            self.assertEqual(await count_callback_facts(session), 1)
            failed = await session.scalar(select(AdjustCallbackRaw).where(AdjustCallbackRaw.status == "failed"))
            await process_callbacks(session)
            self.assertEqual(failed.attempts, 1)

    async def test_engagement_dispatch_retry_replay_and_profiles(self):
        base = dict(app_token="test-app", environment="sandbox", created_at="1791219600",
                    gps_adid="google-id", idfa="ios-id", username="Test User", email="test+tag@example.com",
                    nomor_telepon="+62001", mnc="01", network_name="Google Ads", campaign_name="Campaign (123)")
        for kind, field in (("click", "click_time"), ("impression", "impression_time")):
            payload = dict(base, activity_kind=kind, **{field: "1791219600"})
            self.assertEqual((await self.send(payload)).status_code, 202)
            self.assertEqual((await self.send(payload)).status_code, 202)
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 2)
            for fact in (await session.scalars(select(AdjustEngagement))).all():
                self.assertIsNone(fact.adid)
                self.assertEqual(fact.email, "test+tag@example.com")
                self.assertEqual(fact.nomor_telepon, "+62001")
                self.assertEqual(fact.mnc, "01")
                self.assertEqual(fact.campaign_id, "123")
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await count_callback_facts(session), 2)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustInstall)), 0)

    async def test_engagement_same_second_distinct_payloads_are_retained(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="impression",
                    created_at="1791219600", impression_time="1791219600", adid="device")
        await self.send(dict(base, tracker_token="one"))
        await self.send(dict(base, tracker_token="two"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await session.scalar(select(func.count()).select_from(AdjustEngagement)), 2)

    async def test_engagement_timestamp_fallback_validation_and_failure_retention(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="click",
                    created_at="1704128400", click_time="{click_time}", adid="{adid}", username="[username]")
        fact = normalize_engagement(base, 1)
        self.assertEqual(str(fact["date"]), "2024-01-02")
        self.assertIsNone(fact["adid"])
        self.assertIsNone(fact["username"])
        self.assertIsNone(fact["click_time"])
        for field in ("created_at", "click_time"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                normalize_engagement(dict(base, **{field: "invalid"}), 1)
        await self.send(base)
        await self.send(dict(base, created_at="invalid"))
        async with self.factory() as session:
            with self.assertRaises(Exception) as failure:
                await process_callbacks(session)
            self.assertEqual(failure.exception.status_code, 422)
            self.assertEqual(await count_callback_facts(session), 1)
            failed = await session.scalar(select(AdjustCallbackRaw).where(AdjustCallbackRaw.status == "failed"))
            await process_callbacks(session)
            self.assertEqual(failed.attempts, 1)

    async def test_business_events_dispatch_replay_and_same_transaction_across_types(self):
        for index, name in enumerate(BUSINESS_EVENTS):
            payload = dict(app_token="test-app", environment="sandbox", activity_kind="event", adid="device",
                           created_at="1704128400", event_token=f"event-{index}", event_name=name,
                           transaction_id="same-transaction", revenue_amount="1000.50", currency="idr",
                           username="Test User", email="test+tag@example.com", nomor_telepon="+62001", mnc="01")
            self.assertEqual((await self.send(payload)).status_code, 202)
            await self.send(payload)
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 4)
            facts = []
            for model in EVENT_MODELS.values():
                facts.extend((await session.scalars(select(model))).all())
            self.assertEqual({f.event_type for f in facts}, set(BUSINESS_EVENTS.values()))
            for fact in facts:
                self.assertEqual(fact.revenue_amount, "1000.50")
                self.assertEqual(fact.currency, "IDR")
                self.assertEqual(fact.email, "test+tag@example.com")
                self.assertEqual(fact.nomor_telepon, "+62001")
                self.assertEqual(fact.mnc, "01")
                self.assertEqual(str(fact.date), "2024-01-02")
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await count_callback_facts(session), 4)

    async def test_business_transaction_dedup_and_same_second_distinct_transactions(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="event", adid="device",
                    created_at="1791219600", event_token="deposit-token", event_name="Deposit Completed")
        await self.send(dict(base, transaction_id="one", currency="IDR"))
        await self.send(dict(base, transaction_id="one", revenue_float="0.12345678901234567890123456789", currency="USD"))
        await self.send(dict(base, transaction_id="two", revenue_amount="100", currency="USD"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 2)
            first = await session.scalar(select(EVENT_MODELS["deposit"]).where(EVENT_MODELS["deposit"].transaction_id == "one"))
            self.assertEqual(first.revenue_amount, "0.12345678901234567890123456789")
            self.assertEqual(first.currency, "USD")
        await self.send(dict(base, transaction_id="one", revenue_amount="999", currency="USD"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual((await session.scalar(select(EVENT_MODELS["deposit"]).where(EVENT_MODELS["deposit"].transaction_id == "one"))).revenue_amount,
                             "0.12345678901234567890123456789")

    async def test_business_events_optional_revenue_validation_and_failure_retention(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="event", adid="device",
                    created_at="1791219600", event_token="register-token", event_name="Register+Completed",
                    revenue_amount="{revenue_float}", currency="{currency}")
        fact = normalize_event(base, 1)
        self.assertIsNone(fact["revenue_amount"])
        self.assertEqual(fact["event_type"], "register")
        for field, value in (("event_name", "Other Event"), ("event_token", "{event}"), ("adid", ""),
                             ("created_at", "bad"), ("revenue_amount", "NaN"), ("revenue_amount", "-1"), ("currency", "bad-code")):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                normalize_event(dict(base, **{field: value}), 1)
        with self.assertRaises(ValueError):
            normalize_event(dict(base, revenue_amount="10"), 1)
        await self.send(base)
        await self.send(dict(base, event_name="Unsupported"))
        async with self.factory() as session:
            with self.assertRaises(Exception) as failure:
                await process_callbacks(session)
            self.assertEqual(failure.exception.status_code, 422)
            self.assertEqual(await count_callback_facts(session), 1)

    async def test_business_transaction_custom_query_parameter(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="event", adid="device",
                    created_at="1791219600", event_token="subscribe-token", event_name="Subscribe Success", channel_id="channel-01", price="100.50", future_parameter="preserved")
        response = await self.client.post("/api/adjust/callback", json=base,
            params=dict(token="test-secret", transaction_id="order-001", username="User", query_custom="also-preserved"))
        self.assertEqual(response.status_code, 202)
        async with self.factory() as session:
            await process_callbacks(session)
            fact = await session.scalar(select(EVENT_MODELS["subscribe_success"]))
            self.assertEqual(fact.transaction_id, "order-001")
            self.assertEqual(fact.username, "User")
            self.assertEqual(fact.channel_id, "channel-01")
            self.assertEqual(fact.price, "100.50")
            self.assertEqual(fact.custom_parameters["future_parameter"], "preserved")
            self.assertEqual(fact.custom_parameters["query_custom"], "also-preserved")
            self.assertNotIn("token", fact.custom_parameters)

    async def test_cost_alias_precision_optional_device_and_replay(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="cost_update",
                    created_at="1704128400", cost_amount="0.123456789012345678901", cost_currency="usd",
                    reporting_cost="100.50", cost_type="cpi", cost_id_md5="id-01", extra="retained")
        await self.send(base)
        await self.send(base)
        await self.send(dict(base, activity_kind="cost_updated", cost_id_md5="id-02"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 2)
            rows = (await session.scalars(select(AdjustCost))).all()
            for row in rows:
                self.assertEqual(row.cost_amount, "0.123456789012345678901")
                self.assertEqual(row.cost_currency, "USD")
                self.assertEqual(row.activity_kind, "cost_update")
                self.assertEqual(row.custom_parameters["extra"], "retained")
                self.assertIsNone(row.adid)
                self.assertEqual(str(row.date), "2024-01-02")
            await process_callbacks(session, "manual", datetime.now(), datetime.now())
            self.assertEqual(await count_callback_facts(session), 2)

    async def test_cost_validation_and_distinct_updates(self):
        base = dict(app_token="test-app", environment="sandbox", activity_kind="cost_update",
                    created_at="1791219600", cost_id_md5="same", cost_amount="{cost_amount}", cost_currency="{cost_currency}")
        self.assertIsNone(normalize_cost(base, 1)["cost_amount"])
        for field, value in (("cost_amount", "NaN"), ("reporting_cost", "-1"), ("cost_currency", "bad-code"), ("created_at", "invalid")):
            with self.assertRaises(ValueError):
                normalize_cost(dict(base, **{field: value}), 1)
        with self.assertRaises(ValueError):
            normalize_cost(dict(base, cost_amount="10"), 1)
        await self.send(dict(base, cost_amount="10", cost_currency="USD"))
        await self.send(dict(base, cost_amount="20", cost_currency="USD"))
        async with self.factory() as session:
            await process_callbacks(session)
            self.assertEqual(await count_callback_facts(session), 2)
