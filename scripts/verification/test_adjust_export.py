"""Permission, date-boundary and CSV integrity checks for Adjust exports."""
import unittest
import csv
import io
from types import SimpleNamespace
from datetime import datetime
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.api.v1.endpoint.adjust_export import router, MODELS
from app.db.session import get_db
from app.api.v1.endpoint.common import get_current_user
from app.db.models.adjust import AdjustCallbackRaw


class AdjustExportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = create_async_engine('sqlite+aiosqlite:///:memory:')
        async with self.engine.begin() as conn:
            for model in [AdjustCallbackRaw, *MODELS.values()]:
                await conn.run_sync(model.__table__.create)
        self.factory = async_sessionmaker(self.engine)
        self.app = FastAPI()
        self.app.include_router(router)
        async def db():
            async with self.factory() as session:
                yield session
        self.app.dependency_overrides[get_db] = db
        self.client = AsyncClient(transport=ASGITransport(app=self.app), base_url='http://test')

    async def asyncTearDown(self):
        await self.client.aclose()
        await self.engine.dispose()

    async def seed(self):
        async with self.factory() as session:
            for i, timestamp in enumerate([datetime(2026,10,13,16,59,59), datetime(2026,10,13,17), datetime(2026,10,14,16,59,59), datetime(2026,10,14,17)]):
                session.add(AdjustCallbackRaw(payload_hash=str(i), payload={'custom':'=SUM(A1:A2)','unicode':'日本'}, received_at=timestamp,status='pending',attempts=0))
            await session.commit()

    async def test_unauthenticated_is_denied(self):
        for path in ['tables','summary?table=adjust_callback_raw&start_date=2026-10-14&end_date=2026-10-14','csv?table=adjust_callback_raw&start_date=2026-10-14&end_date=2026-10-14']:
            self.assertIn((await self.client.get('/api/adjust-export/'+path)).status_code,[401,403])

    async def test_allowlist_boundary_and_csv(self):
        async def admin(): return SimpleNamespace(role="admin")
        self.app.dependency_overrides[get_current_user] = admin
        from app.db.models.adjust import AdjustInstall
        from datetime import date
        async with self.factory() as session:
            for i, day in enumerate([date(2026,10,13),date(2026,10,14),date(2026,10,14),date(2026,10,15)]):
                session.add(AdjustInstall(install_key=str(i),app_token='app',environment='production',adid=str(i),installed_at=datetime.combine(day,datetime.min.time()),date=day,raw_id=i+1,city='日本'))
            await session.commit()
        params=dict(table='adjust_install',start_date='2026-10-14',end_date='2026-10-14')
        summary=(await self.client.get('/api/adjust-export/summary',params=params)).json()
        self.assertEqual(summary['row_count'],2)
        self.assertEqual(summary['data_start'],'2026-10-14')
        response=await self.client.get('/api/adjust-export/csv',params=params)
        self.assertEqual(response.status_code,200)
        rows=list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig'))))
        self.assertEqual(len(rows),2)
        self.assertEqual(response.headers['x-export-row-count'],'2')
        import json
        self.assertEqual(rows[0]['city'],'日本')
        self.assertEqual((await self.client.get('/api/adjust-export/summary',params=dict(params,table='tf_user'))).status_code,400)
        self.assertEqual((await self.client.get('/api/adjust-export/csv',params=dict(params,start_date='2026-10-15'))).status_code,400)
        tables=(await self.client.get('/api/adjust-export/tables')).json()['tables']
        self.assertEqual(len(tables),len(MODELS))
        self.assertNotIn('adjust_callback_raw', [t['table'] for t in tables])
        for endpoint in ['summary','csv']:
            self.assertEqual((await self.client.get('/api/adjust-export/'+endpoint,params=dict(params,table='adjust_callback_raw'))).status_code,400)
        self.assertEqual(next(t for t in tables if t['table']=='adjust_deposit')['row_count'],0)

    async def test_other_roles_are_denied_and_superadmin_allowed(self):
        for role in ['analyst', 'finance', 'digital_marketing', 'tech_it']:
            async def user(): return SimpleNamespace(role=role)
            self.app.dependency_overrides[get_current_user] = user
            for path in ['tables', 'summary', 'csv']:
                response = await self.client.get('/api/adjust-export/'+path, params=dict(table='adjust_install',start_date='2026-10-14',end_date='2026-10-14'))
                self.assertEqual(response.status_code,403)
        async def superadmin(): return SimpleNamespace(role='superadmin')
        self.app.dependency_overrides[get_current_user] = superadmin
        self.assertEqual((await self.client.get('/api/adjust-export/tables')).status_code,200)

    def test_formula_cells_are_escaped(self):
        from app.api.v1.endpoint.adjust_export import csv_value
        self.assertEqual(csv_value('=HYPERLINK("x")'), '\'=HYPERLINK("x")')
        self.assertEqual(csv_value('normal'), 'normal')
