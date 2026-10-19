"""Unique Campaign sync includes GPT naming rules and campaign-grain facts only."""
import unittest
from datetime import date, datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.db.models.external_api import Campaign, GoogleAds, FacebookAds, TikTokAds
from app.db.models.gpt_ads import GptAdsDaily
from app.etl.load import rebuild_unique_campaign


class UniqueCampaignGptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        async with self.engine.begin() as connection:
            for model in [Campaign, GoogleAds, FacebookAds, TikTokAds, GptAdsDaily]:
                await connection.run_sync(model.__table__.create)
        self.factory = async_sessionmaker(self.engine)

    async def asyncTearDown(self):
        await self.engine.dispose()

    async def test_gpt_prefix_objectives_placeholder_repair_and_replay(self):
        async with self.factory() as session:
            session.add(Campaign(campaign_id='ua', campaign_name='Unknown Campaign ua', ad_source='unknown', ad_type='unknown', created_at=datetime(2026,1,1)))
            for level in ['campaign','ad_group','ad']:
                for key, name in [('ua','GPT - UA - Test'),('ba','GPT - BA - Test'),('rm','GPT - RM - Test'),('other','GPT Test')]:
                    session.add(GptAdsDaily(account_id='account', date=date(2026,10,19), report_level=level,entity_id=key+level,campaign_id=key,
                        campaign_name=name if level=='campaign' else 'GPT - UA - Wrong lower-level name',
                        impressions=0,clicks=0,currency='IDR',timezone='Asia/Jakarta',attribution_window_days=30,
                        view_through_attribution_window_days=1,attribution_time_basis='ad_event_time',updated_at=datetime.now()))
            await session.commit()
            await rebuild_unique_campaign(session)
            await rebuild_unique_campaign(session)
            rows=(await session.execute(select(Campaign.__table__))).mappings().all()
            self.assertEqual(len(rows),4)
            expected={'ua':'user_acquisition','ba':'brand_awareness','rm':'remarketing','other':'unknown'}
            for row in rows:
                self.assertEqual(row['ad_source'],'chatgpt')
                self.assertEqual(row['ad_type'],expected[row['campaign_id']])
                self.assertNotIn('Wrong',row['campaign_name'])
                self.assertNotIn('Unknown Campaign',row['campaign_name'])

    async def test_existing_platform_rules_are_preserved(self):
        async with self.factory() as session:
            for model, key, name in [(GoogleAds,'google','GG - UA - Test'),(FacebookAds,'facebook','FB - BA - Test'),(TikTokAds,'tiktok','TT - RM - Test')]:
                session.add(model(date=date(2026,10,19),campaign_id=key,campaign_name=name,ad_group='group',ad_name='ad',pull_date=date(2026,10,19)))
            await session.commit()
            await rebuild_unique_campaign(session)
            rows=(await session.execute(select(Campaign.__table__))).mappings().all()
            actual={row['campaign_id']:(row['ad_source'],row['ad_type']) for row in rows}
            self.assertEqual(actual,{'google':('google_ads','user_acquisition'),'facebook':('facebook_ads','brand_awareness'),'tiktok':('tiktok_ads','remarketing')})
