"""Security regression tests for auth/session hardening."""

from __future__ import annotations

from datetime import datetime
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.v1.endpoint.auth import _RATE_LIMIT_BUCKETS, _enforce_rate_limit
from app.api.v1.endpoint.google_ads_oauth import (
    _authorized_oauth_actor_from_state,
    _create_google_ads_oauth_state,
)
from app.core.security import (
    create_session_access_token,
    create_session_refresh_token,
    pwd_context,
    rotate_refresh_token,
    verify_access_token,
)
from app.db.base import SqliteBase
from app.db.models.user import AuthAuditEvent, LoginThrottle, TfUser, UserToken
from app.utils.app_utils import FastApiApp
from app.utils.user_utils import MAX_FAILED_LOGIN_ATTEMPTS, authenticate_user, require_roles, user_token


class TestSecurityHelpers(TestCase):
    """Pure unit tests for RBAC, redaction, and rate limiting."""

    def test_require_roles_blocks_unauthorized_user(self):
        user = TfUser(
            user_id="u-1",
            fullname="Sales User",
            email="sales@example.com",
            role="sales",
            password="hashed",
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        with self.assertRaises(PermissionError):
            require_roles(user, "admin", "superadmin")

    def test_redaction_masks_sensitive_fields_recursively(self):
        payload = {
            "access_token": "plain-access",
            "nested": {
                "refresh_token": "plain-refresh",
                "Authorization": "Bearer secret",
                "client_secret": "oauth-secret",
            },
        }
        sanitized = FastApiApp._sanitize_log_payload(payload)
        self.assertEqual(sanitized["access_token"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["refresh_token"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["Authorization"], "[REDACTED]")
        self.assertEqual(sanitized["nested"]["client_secret"], "[REDACTED]")

    def test_rate_limit_raises_after_budget_is_exhausted(self):
        _RATE_LIMIT_BUCKETS.clear()
        request = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/login",
                "headers": [],
                "client": ("127.0.0.1", 5000),
                "scheme": "http",
                "server": ("testserver", 80),
            }
        )
        for _ in range(2):
            _enforce_rate_limit(request=request, scope="test-login", max_requests=2, window_seconds=60)
        with self.assertRaises(HTTPException):
            _enforce_rate_limit(request=request, scope="test-login", max_requests=2, window_seconds=60)


class TestSecurityFlows(IsolatedAsyncioTestCase):
    """Integration-like tests for auth/session persistence behavior."""

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

    async def test_failed_login_creates_audit_event_and_lockout(self):
        async with self.session_factory() as session:
            session.add(
                TfUser(
                    user_id="user-1",
                    fullname="Admin",
                    email="admin@example.com",
                    role="admin",
                    password=pwd_context.hash("correct-password"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            for _ in range(MAX_FAILED_LOGIN_ATTEMPTS):
                with self.assertRaises(HTTPException):
                    await authenticate_user(
                        email="admin@example.com",
                        password="wrong-password",
                        session=session,
                        client_ip="127.0.0.1",
                        user_agent="pytest",
                    )

            throttle_result = await session.execute(
                select(LoginThrottle).where(LoginThrottle.email == "admin@example.com")
            )
            throttle = throttle_result.scalar_one()
            self.assertEqual(throttle.failed_attempts, MAX_FAILED_LOGIN_ATTEMPTS)
            self.assertIsNotNone(throttle.locked_until)

            audit_result = await session.execute(select(AuthAuditEvent))
            audit_events = audit_result.scalars().all()
            self.assertEqual(len(audit_events), MAX_FAILED_LOGIN_ATTEMPTS)
            self.assertTrue(all(event.event_type == "login_failed" for event in audit_events))

            with self.assertRaises(HTTPException) as locked_error:
                await authenticate_user(
                    email="admin@example.com",
                    password="correct-password",
                    session=session,
                    client_ip="127.0.0.1",
                    user_agent="pytest",
                )
            self.assertEqual(locked_error.exception.status_code, 423)

    async def test_refresh_rotation_invalidates_old_tokens_and_keeps_only_fingerprints(self):
        async with self.session_factory() as session:
            session.add(
                TfUser(
                    user_id="user-2",
                    fullname="Security User",
                    email="security@example.com",
                    role="superadmin",
                    password=pwd_context.hash("secret"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            session_id = "session-123"
            access_token = create_session_access_token(subject="user-2", session_id=session_id)
            refresh_token = create_session_refresh_token(subject="user-2", session_id=session_id)
            await user_token(
                session=session,
                user_id="user-2",
                role="superadmin",
                access_token=access_token,
                refresh_token=refresh_token,
                session_id=session_id,
            )

            token_result = await session.execute(select(UserToken).where(UserToken.user_id == "user-2"))
            stored_token = token_result.scalar_one()
            self.assertNotEqual(stored_token.access_token, access_token)
            self.assertNotEqual(stored_token.refresh_token, refresh_token)

            await verify_access_token(session, access_token)
            new_access_token, new_refresh_token, _, _ = await rotate_refresh_token(session, refresh_token)

            with self.assertRaises(Exception):
                await verify_access_token(session, access_token)
            await verify_access_token(session, new_access_token)

            with self.assertRaises(Exception):
                await rotate_refresh_token(session, refresh_token)

            token_result = await session.execute(select(UserToken).where(UserToken.user_id == "user-2"))
            stored_token = token_result.scalar_one()
            self.assertTrue(stored_token.is_revoked)

    async def test_google_ads_oauth_actor_requires_active_superadmin_state(self):
        async with self.session_factory() as session:
            session.add_all(
                [
                    TfUser(
                        user_id="user-superadmin",
                        fullname="Super Admin",
                        email="superadmin@example.com",
                        role="superadmin",
                        password=pwd_context.hash("secret"),
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    ),
                    TfUser(
                        user_id="user-sales",
                        fullname="Sales User",
                        email="sales@example.com",
                        role="sales",
                        password=pwd_context.hash("secret"),
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    ),
                ]
            )
            await session.commit()

        with patch("app.api.v1.endpoint.google_ads_oauth.sqlite_async_session", self.session_factory):
            actor = await _authorized_oauth_actor_from_state(
                _create_google_ads_oauth_state("user-superadmin")
            )
            self.assertIsNotNone(actor)
            self.assertEqual(actor.user_id, "user-superadmin")

            unauthorized_actor = await _authorized_oauth_actor_from_state(
                _create_google_ads_oauth_state("user-sales")
            )
            self.assertIsNone(unauthorized_actor)

            missing_actor = await _authorized_oauth_actor_from_state(
                _create_google_ads_oauth_state("missing-user")
            )
            self.assertIsNone(missing_actor)

            invalid_actor = await _authorized_oauth_actor_from_state("invalid-state-token")
            self.assertIsNone(invalid_actor)
