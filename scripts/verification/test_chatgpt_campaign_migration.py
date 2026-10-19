"""Checks for the targeted ChatGPT campaign data correction."""
import unittest
from datetime import datetime
from unittest.mock import patch
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from app.db import bootstrap
from app.db.models.external_api import Campaign

MIGRATION_ID = "20261019_001_chatgpt_ua_campaign"
TARGET_ID = "cmpn_1d048d0540fc8193bcee7a294f6ebfe1"


class ChatGptCampaignMigrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine("sqlite+aiosqlite:///:memory:")

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_targeted_idempotent_update_preserves_other_fields(self):
        async with self.engine.begin() as connection:
            await connection.run_sync(Campaign.__table__.create)
            values = dict(campaign_name="GPT - UA - Existing", ad_source="unknown", ad_type="unknown",
                          created_at=datetime(2026, 10, 1))
            await connection.execute(insert(Campaign), [dict(values, campaign_id=TARGET_ID),
                                                        dict(values, campaign_id="another-gpt-campaign")])
            await bootstrap._migration_20261019_001_chatgpt_ua_campaign(connection)
            await bootstrap._migration_20261019_001_chatgpt_ua_campaign(connection)
            rows = (await connection.execute(select(Campaign.__table__).order_by(Campaign.campaign_id))).mappings().all()
            target = next(row for row in rows if row["campaign_id"] == TARGET_ID)
            other = next(row for row in rows if row["campaign_id"] != TARGET_ID)
            self.assertEqual((target["ad_source"], target["ad_type"]), ("chatgpt", "user_acquisition"))
            self.assertEqual(target["campaign_name"], values["campaign_name"])
            self.assertEqual(target["created_at"], values["created_at"])
            self.assertEqual((other["ad_source"], other["ad_type"]), ("unknown", "unknown"))

    async def test_registry_tracks_once_and_handles_missing_campaign(self):
        migration = next(entry for entry in bootstrap.SCHEMA_MIGRATIONS if entry[0] == MIGRATION_ID)
        with patch.object(bootstrap, "SCHEMA_MIGRATIONS", (migration,)):
            async with self.engine.begin() as connection:
                self.assertEqual(await bootstrap.apply_schema_migrations(connection), [MIGRATION_ID])
                self.assertEqual(await bootstrap.apply_schema_migrations(connection), [])
                count = await connection.scalar(text("SELECT COUNT(*) FROM schema_migration WHERE migration_id = :id"), {"id": MIGRATION_ID})
                self.assertEqual(count, 1)
                self.assertEqual(await connection.scalar(text("SELECT COUNT(*) FROM campaign")), 0)

    async def test_name_correction_preserves_classification_and_other_campaigns(self):
        async with self.engine.begin() as connection:
            await connection.run_sync(Campaign.__table__.create)
            values = dict(campaign_name="Unknown Campaign " + TARGET_ID, ad_source="chatgpt", ad_type="user_acquisition", created_at=datetime(2026, 10, 1))
            await connection.execute(insert(Campaign), [dict(values, campaign_id=TARGET_ID), dict(values, campaign_id="other")])
            await bootstrap._migration_20261019_002_chatgpt_campaign_name(connection)
            await bootstrap._migration_20261019_002_chatgpt_campaign_name(connection)
            rows = (await connection.execute(select(Campaign.__table__))).mappings().all()
            target = next(row for row in rows if row["campaign_id"] == TARGET_ID)
            other = next(row for row in rows if row["campaign_id"] != TARGET_ID)
            self.assertEqual(target["campaign_name"], "GPT - UA - BAU- CPC - WEB - ID - OCT 2026")
            self.assertEqual((target["ad_source"], target["ad_type"]), ("chatgpt", "user_acquisition"))
            self.assertEqual(other["campaign_name"], values["campaign_name"])
