"""API contract, validation, replacement and failure atomicity checks."""
import asyncio
import json
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from unittest.mock import patch
import httpx
from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import settings
from app.db.models.external_api import StgAdsRaw
from app.etl.gpt_ads.client import GptAdsClient
from app.etl.gpt_ads.config import REPORT_LEVELS, report_fields, resolve_gpt_ads_window
from app.db.models.gpt_ads import GptAdsDaily
from app.etl.gpt_ads.pipeline import run_gpt_ads
from app.etl.gpt_ads.transform import normalize_row

ACCOUNT = dict(id='account-1', timezone='Asia/Jakarta', currency_code='IDR')
DAY = datetime.now(ZoneInfo('Asia/Jakarta')).date() - timedelta(days=2)


def raw_row(level, *, day=DAY, **overrides):
    row = dict(readable_time=day.isoformat(), timezone=ACCOUNT['timezone'],
               campaign_id='campaign-1', campaign_name='Campaign', impressions=100, clicks=10,
               spend='123.456', ctr='0.1', cpc='15.23', cpm='1234.56', conversions=2)
    if level in {'ad_group','ad'}:row.update(ad_group_id='group-1', ad_group_name='Group')
    if level=='ad':row.update(ad_id='ad-1', ad_name='Ad')
    return dict(row, **overrides)


async def no_sleep(_): pass


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_contract_pagination_and_date_chunks(self):
        calls=[]
        def handle(request):
            self.assertEqual(request.headers['authorization'],'Bearer test-key')
            self.assertEqual(request.url.path,'/v1/ad_account/insights')
            params=request.url.params
            self.assertEqual(params['aggregation_level'],'ad')
            self.assertEqual(params['time_granularity'],'daily')
            self.assertEqual(params['attribution_window_days'],'30')
            self.assertEqual(params['view_through_attribution_window_days'],'1')
            self.assertEqual(params.get_list('fields[]'),report_fields('ad'))
            window=json.loads(params['time_ranges[]'])
            self.assertEqual(window['timezone'],'Asia/Jakarta')
            self.assertIn('ad_group.id',params.get_list('fields[]'))
            self.assertIn('conversions',params.get_list('fields[]'))
            calls.append((window['since'],window['until'],params.get('after')))
            more=params.get('after') is None
            return httpx.Response(200,json=dict(data=[dict(id='one')],has_more=more,last_id='next' if more else 'last'))
        async with GptAdsClient(api_key='test-key',transport=httpx.MockTransport(handle)) as client:
            pages=[p async for p in client.report('ad',DAY-timedelta(days=31),DAY,ACCOUNT['timezone'])]
        self.assertEqual(len(pages),4)
        self.assertEqual(calls[0][2],None)
        self.assertEqual(calls[1][2],'next')
        self.assertEqual(calls[2][0],DAY.isoformat())
        self.assertIsNone(calls[2][2])

    async def test_retry_and_credentials_not_in_error(self):
        calls=[]
        def handle(req):
            calls.append(req)
            return httpx.Response(429,headers={'Retry-After':'0'},json={'secret':'test-key'}) if len(calls)<3 else httpx.Response(200,json=ACCOUNT)
        async with GptAdsClient(api_key='test-key',transport=httpx.MockTransport(handle),sleep=no_sleep) as client:
            self.assertEqual((await client.account())['id'],ACCOUNT['id'])
        self.assertEqual(len(calls),3)
        async with GptAdsClient(api_key='test-key',transport=httpx.MockTransport(lambda _:httpx.Response(401,text='test-key'))) as client:
            with self.assertRaises(HTTPException) as caught:await client.account()
            self.assertNotIn('test-key',str(caught.exception.detail))
            self.assertIn('401',str(caught.exception.detail))

    async def test_bad_response_and_nonadvancing_cursor_fail(self):
        for body in [dict(data=[],has_more=True,last_id='x'),dict(data=[]),dict(data=[{}],has_more=True,last_id='same')]:
            with self.subTest(body=body):
                async with GptAdsClient(api_key='key',transport=httpx.MockTransport(lambda _:httpx.Response(200,json=body))) as client:
                    with self.assertRaises(HTTPException):
                        _=[p async for p in client.report('campaign',DAY,DAY,ACCOUNT['timezone'])]
        async with GptAdsClient(api_key='key',transport=httpx.MockTransport(lambda _:httpx.Response(200,json=dict(ACCOUNT,timezone='UTC')))) as client:
            with self.assertRaises(HTTPException):await client.account()


class TransformTests(unittest.TestCase):
    def test_decimal_metric_semantics_and_missing_values(self):
        row=normalize_row(raw_row('ad',cpc='15.23',conversions=None),'ad',ACCOUNT,DAY,DAY,'run')
        self.assertEqual(row['cpc'],Decimal('15.23'))  # Preserve API finalized cost metric.
        self.assertEqual(row['ctr'],Decimal('0.1'))
        self.assertIsNone(row['conversions'])
        self.assertEqual(row['entity_id'],'ad-1')
        for field,value in [('ad_group_id',None),('readable_time','invalid'),('timezone','UTC'),('clicks',1.5),('impressions',-1),('spend','NaN'),('ctr',2)]:
            with self.subTest(field=field),self.assertRaises(ValueError):
                normalize_row(raw_row('ad',**{field:value}),'ad',ACCOUNT,DAY,DAY,'run')
        missing=raw_row('ad');del missing['conversions']
        with self.assertRaises(ValueError):normalize_row(missing,'ad',ACCOUNT,DAY,DAY,'run')
        with self.assertRaises(ValueError):normalize_row(raw_row('ad',day=DAY-timedelta(days=1)),'ad',ACCOUNT,DAY,DAY,'run')

    def test_auto_refreshes_31_completed_account_days(self):
        lower,upper=resolve_gpt_ads_window('auto')
        self.assertEqual((upper-lower).days,30)
        self.assertEqual(upper,datetime.now(ZoneInfo(ACCOUNT['timezone'])).date()-timedelta(days=1))


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine=create_async_engine('sqlite+aiosqlite:///:memory:')
        async with self.engine.begin() as connection:
            for model in [StgAdsRaw,GptAdsDaily]:await connection.run_sync(model.__table__.create)
        self.factory=async_sessionmaker(self.engine,expire_on_commit=False)

    async def asyncTearDown(self):await self.engine.dispose()

    def client(self, *, clicks=10, fail=None, empty=False, invalid=None):
        class FakeClient:
            async def __aenter__(self):return self
            async def __aexit__(self,*_):pass
            async def account(self):return ACCOUNT
            async def report(self,level,start,end,timezone):
                if level==fail:raise HTTPException(502,'Upstream failed')
                row=raw_row(level,clicks=clicks)
                if level==invalid:del row['conversions']
                yield ([] if empty else [row]),start,end
        return FakeClient

    async def run_pipeline(self, **kwargs):
        async with self.factory() as session:
            return await run_gpt_ads(session,'manual',DAY,DAY,'run',client_factory=self.client(**kwargs))

    async def test_three_levels_replay_correction_and_staging(self):
        await self.run_pipeline()
        await self.run_pipeline(clicks=15)
        async with self.factory() as session:
            for level in REPORT_LEVELS:
                self.assertEqual(await session.scalar(select(func.count()).select_from(GptAdsDaily).where(GptAdsDaily.report_level==level)),1)
                row=await session.scalar(select(GptAdsDaily).where(GptAdsDaily.report_level==level))
                self.assertEqual(row.clicks,15)
                self.assertEqual(row.currency,'IDR')
                self.assertEqual(row.ctr,Decimal('0.1'))
            self.assertEqual(await session.scalar(select(func.count()).select_from(StgAdsRaw)),6)

    async def test_failure_and_invalid_report_preserve_all_fact_levels(self):
        await self.run_pipeline()
        for kwargs in [dict(clicks=99,fail='ad'),dict(clicks=99,invalid='ad_group')]:
            with self.assertRaises(HTTPException):await self.run_pipeline(**kwargs)
            async with self.factory() as session:
                for level in REPORT_LEVELS:
                    self.assertEqual((await session.scalar(select(GptAdsDaily).where(GptAdsDaily.report_level==level))).clicks,10)

    async def test_complete_empty_replaces_only_target_account_window(self):
        await self.run_pipeline()
        async with self.factory() as session:
            for level in REPORT_LEVELS:
                row=normalize_row(raw_row(level),level,dict(ACCOUNT,id='other-account'),DAY,DAY,None)
                session.add(GptAdsDaily(**row))
                row=normalize_row(raw_row(level,day=DAY-timedelta(days=1)),level,ACCOUNT,DAY-timedelta(days=1),DAY,None)
                session.add(GptAdsDaily(**row))
            await session.commit()
        await self.run_pipeline(empty=True)
        async with self.factory() as session:
            for level in REPORT_LEVELS:
                self.assertEqual(await session.scalar(select(func.count()).select_from(GptAdsDaily).where(GptAdsDaily.report_level==level)),2)

    async def test_db_failure_rolls_back_all_report_levels(self):
        await self.run_pipeline()
        async with self.factory() as session:
            execute=session.execute
            async def failing(statement,*args,**kwargs):
                if getattr(statement,'is_insert',False) and getattr(statement,'table',None).name=='gpt_ads_daily':
                    raise RuntimeError('simulated load failure')
                return await execute(statement,*args,**kwargs)
            with patch.object(session,'execute',side_effect=failing):
                with self.assertRaises(RuntimeError):
                    await run_gpt_ads(session,'manual',DAY,DAY,'run',client_factory=self.client(clicks=99))
        async with self.factory() as session:
            for level in REPORT_LEVELS:self.assertEqual((await session.scalar(select(GptAdsDaily).where(GptAdsDaily.report_level==level))).clicks,10)

    async def test_job_registration_count_and_invalid_windows(self):
        from app.etl.job_runner import count_source_rows, DEFAULT_SCHEDULED_SOURCES, PIPELINE_EXECUTORS
        self.assertIn('gpt_ads', DEFAULT_SCHEDULED_SOURCES)
        self.assertIn('gpt_ads', PIPELINE_EXECUTORS)
        await self.run_pipeline()
        async with self.factory() as session:
            self.assertEqual(await count_source_rows(session,'gpt_ads',DAY,DAY),3)
            with self.assertRaises(HTTPException):
                await run_gpt_ads(session,'manual',DAY,DAY+timedelta(days=3),'run',client_factory=self.client())
            with self.assertRaises(HTTPException):
                await run_gpt_ads(session,'manual',DAY-timedelta(days=400),DAY,'run',client_factory=self.client())

    async def test_same_entity_id_different_levels_remains_distinct(self):
        async with self.factory() as session:
            for level in REPORT_LEVELS:
                row = normalize_row(raw_row(level, campaign_id='same', ad_group_id='same', ad_id='same'), level, ACCOUNT, DAY, DAY, None)
                session.add(GptAdsDaily(**row))
            await session.commit()
            self.assertEqual(await session.scalar(select(func.count()).select_from(GptAdsDaily)), 3)
