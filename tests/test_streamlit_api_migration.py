"""Regression tests for the Streamlit-to-FastAPI account/home migration."""

from __future__ import annotations

from datetime import datetime
from unittest import IsolatedAsyncioTestCase, TestCase

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.v1.endpoint.auth import get_accounts, home_context, patch_account
from app.db.base import SqliteBase
from app.db.models.etl_run import EtlRun
from app.db.models.user import TfUser
from app.schemas.user import AccountUpdateRequest
from streamlit_app.functions.accounts import get_accounts as fetch_accounts_dataframe


class TestStreamlitAccountHelpers(TestCase):
    """Frontend helper tests for account-data API consumption."""

    def test_get_accounts_returns_empty_dataframe_with_expected_columns_on_error(self):
        async def run_test():
            from unittest.mock import patch

            with patch(
                "streamlit_app.functions.accounts.fetch_data",
                return_value={"success": False, "message": "forbidden"},
            ):
                dataframe = await fetch_accounts_dataframe("http://backend:8000")
            self.assertEqual(
                list(dataframe.columns),
                ["user_id", "fullname", "email", "role", "created_at", "updated_at"],
            )
            self.assertTrue(dataframe.empty)

        import asyncio

        asyncio.run(run_test())


class TestAccountApiMigration(IsolatedAsyncioTestCase):
    """Integration-style tests for the new account/home API layer."""

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

    async def test_accounts_endpoint_returns_active_users_only_for_superadmin(self):
        async with self.session_factory() as session:
            superadmin = TfUser(
                user_id="super-1",
                fullname="Super Admin",
                email="super@example.com",
                role="superadmin",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            active_user = TfUser(
                user_id="user-1",
                fullname="Active User",
                email="active@example.com",
                role="sales",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            deleted_user = TfUser(
                user_id="user-2",
                fullname="Deleted User",
                email="deleted@example.com",
                role="sales",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deleted_at=datetime.now(),
            )
            session.add_all([superadmin, active_user, deleted_user])
            await session.commit()

            response = await get_accounts(session=session, current_user=superadmin)

        self.assertTrue(response.success)
        self.assertEqual([item.email for item in response.data], ["active@example.com", "super@example.com"])

    async def test_accounts_endpoint_blocks_non_superadmin(self):
        async with self.session_factory() as session:
            admin = TfUser(
                user_id="admin-1",
                fullname="Admin User",
                email="admin@example.com",
                role="admin",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            session.add(admin)
            await session.commit()

            with self.assertRaises(HTTPException) as error:
                await get_accounts(session=session, current_user=admin)

        self.assertEqual(error.exception.status_code, 403)

    async def test_patch_account_updates_user_via_backend_layer(self):
        async with self.session_factory() as session:
            superadmin = TfUser(
                user_id="super-1",
                fullname="Super Admin",
                email="super@example.com",
                role="superadmin",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            target = TfUser(
                user_id="user-1",
                fullname="Old Name",
                email="old@example.com",
                role="sales",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            session.add_all([superadmin, target])
            await session.commit()

            response = await patch_account(
                user_id="user-1",
                payload=AccountUpdateRequest(
                    fullname="Updated Name",
                    email="updated@example.com",
                    role="digital_marketing",
                ),
                session=session,
                current_user=superadmin,
            )

        self.assertTrue(response.success)
        self.assertEqual(response.data.fullname, "Updated Name")
        self.assertEqual(response.data.email, "updated@example.com")
        self.assertEqual(response.data.role, "digital_marketing")

    async def test_home_context_returns_current_account_and_latest_run(self):
        async with self.session_factory() as session:
            user = TfUser(
                user_id="user-1",
                fullname="Portal User",
                email="portal@example.com",
                role="admin",
                password="hashed",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            latest_run = EtlRun(
                run_id="run-1",
                pipeline="scheduled_etl",
                source="google_ads",
                mode="auto",
                status="success",
                message="Updated successfully",
                error_detail=None,
                window_start=None,
                window_end=None,
                started_at=datetime.now(),
                ended_at=datetime.now(),
                triggered_by="user-1",
            )
            session.add_all([user, latest_run])
            await session.commit()

            response = await home_context(session=session, current_user=user)

        self.assertTrue(response.success)
        self.assertEqual(response.data.account.email, "portal@example.com")
        self.assertIsNotNone(response.data.latest_run)
        self.assertEqual(response.data.latest_run.run_id, "run-1")
