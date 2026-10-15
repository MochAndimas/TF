"""UA integration checks for GPT reporting grain and internal register semantics."""
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.db.models.gpt_ads import GptAdsDaily
from app.db.models.external_api import DailyRegister, Campaign
from app.utils.campaign import CampaignData
from app.utils.campaign.cache_adapter import CampaignCacheAdapter
from app.utils.campaign.sources import USER_ACQUISITION_SOURCE_OPTIONS
from streamlit_app.page.campaign_components.user_acquisition import build_performance_dataframe

DAY=date(2026,10,10)


class GptCampaignTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine=create_async_engine('sqlite+aiosqlite:///:memory:')
        async with self.engine.begin() as connection:
            for model in [GptAdsDaily,DailyRegister,Campaign]:await connection.run_sync(model.__table__.create)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)
        self.patcher=patch('app.utils.campaign.cache_adapter.analytics_dataframe_cache_enabled',return_value=False)
        self.patcher.start()

    async def asyncTearDown(self):
        self.patcher.stop()
        await self.engine.dispose()

    async def seed(self):
        async with self.factory() as session:
            for level in ['campaign','ad_group','ad']:
                row=dict(report_level=level,account_id='account',entity_id=level,campaign_id='campaign',
                         campaign_name='GPT - UA - Test',date=DAY,currency='IDR',timezone='Asia/Jakarta',
                         ad_group_id='group' if level!='campaign' else None,ad_group_name='Group' if level!='campaign' else None,
                         ad_id='ad' if level=='ad' else None,ad_name='Ad' if level=='ad' else None,
                         impressions=100,clicks=10,spend=Decimal('50'),conversions=99,
                         attribution_window_days=30,view_through_attribution_window_days=1,
                         attribution_time_basis='ad_event_time',updated_at=datetime.now())
                session.add(GptAdsDaily(**row))
            await session.commit()

    async def test_metrics_and_each_dimension_do_not_triple_count(self):
        await self.seed()
        async with self.factory() as session:
            service=CampaignData(session,DAY,DAY)
            metrics=await service.ads_metrics('gpt_ads',DAY,DAY,'user_acquisition')
            self.assertEqual(metrics['cost'],50)
            self.assertEqual(metrics['impressions'],100)
            self.assertEqual(metrics['leads'],0) # API conversions are NOT internal registrations.
            details=await service.ads_campaign_details_table('gpt_ads',DAY,DAY,'user_acquisition')
            self.assertEqual(len(details['rows']),3)
            for dimension in ['campaign_id','ad_group','ad_name']:
                daily=await service._ads_daily_dimension_dataframe(data='gpt_ads',dimension=dimension,from_date=DAY,to_date=DAY,campaign_type='user_acquisition',include_leads=True)
                self.assertEqual(daily['cost'].sum(),50)
                performance=await service._ads_performance_dataframe('gpt_ads',DAY,DAY,dimension,'user_acquisition')
                self.assertEqual(performance['spend'].sum(),50)
                ui=build_performance_dataframe(details['rows'],dimension)
                self.assertEqual(ui['spend'].sum(),50)
            growth=await service.ads_metrics_with_growth('gpt_ads',DAY,DAY,'user_acquisition')
            self.assertEqual(growth['previous_period']['metrics']['cost'],0)
            self.assertEqual(USER_ACQUISITION_SOURCE_OPTIONS['GPT Ads'],'gpt_ads')

    async def test_naming_filter_and_empty_dates(self):
        await self.seed()
        async with self.factory() as session:
            service=CampaignData(session,DAY,DAY)
            metrics=await service.ads_metrics('gpt_ads',DAY,DAY,'brand_awareness')
            self.assertEqual(metrics['cost'],0)
            details=await service.ads_campaign_details_table('gpt_ads',date(2025,1,1),date(2025,1,2),'user_acquisition')
            self.assertEqual(details['rows'],[])

    async def test_internal_register_is_allocated_once_per_level(self):
        await self.seed()
        async with self.factory() as session:
            session.add(DailyRegister(date=DAY,campaign_id='campaign',tag_name='CP1',total_regis=8,pull_date=DAY))
            await session.commit()
            service=CampaignData(session,DAY,DAY)
            metrics=await service.ads_metrics('gpt_ads',DAY,DAY,'user_acquisition')
            self.assertEqual(metrics['leads'],8)
            for dimension in ['campaign_id','ad_group','ad_name']:
                rows=await service._ads_performance_dataframe('gpt_ads',DAY,DAY,dimension,'user_acquisition')
                self.assertEqual(rows['leads'].sum(),8)

    async def test_ua_payload_sources_do_not_change_other_page_sources(self):
        from app.api.v1.functions.fetch_campaign import ALL_SOURCES, fetch_ads_metrics_with_growth_payload
        self.assertEqual(ALL_SOURCES,('google','facebook','tiktok'))
        class FakeService:
            async def ads_metrics_with_growth(self,**kwargs):return {'source':kwargs['data']}
        payload=await fetch_ads_metrics_with_growth_payload(FakeService(),DAY,DAY)
        self.assertEqual(set(payload),{'google','facebook','tiktok','gpt_ads'})
