"""Security regression tests for auth/session hardening."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.requests import Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api.v1.endpoint.auth import (
    _RATE_LIMIT_BUCKETS,
    _enforce_rate_limit,
    login_user,
    logout_all_user_sessions,
    refresh_user_token,
)
from app.api.v1.endpoint.google_ads_oauth import (
    _authorized_oauth_actor_from_state,
    _create_google_ads_oauth_state,
)
from app.core.security import (
    create_session_access_token,
    create_session_refresh_token,
    pwd_context,
    rotate_refresh_token,
    validate_password_policy,
    verify_access_token,
)
from app.db.base import SqliteBase
from app.db.models.user import AuthAuditEvent, LoginThrottle, TfUser, UserToken
from app.utils.app_utils import FastApiApp
from app.utils.user_utils import (
    MAX_FAILED_LOGIN_ATTEMPTS,
    authenticate_user,
    create_account,
    get_current_token_data,
    require_roles,
    user_token,
)


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

    def test_password_policy_rejects_weak_password(self):
        with self.assertRaisesRegex(ValueError, "at least 12 characters"):
            validate_password_policy("Weak1!")


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
            self.assertNotEqual(stored_token.session_id, session_id)

            await verify_access_token(session, access_token)
            new_access_token, new_refresh_token, _, _, rotated_session_id = await rotate_refresh_token(
                session,
                refresh_token,
            )

            with self.assertRaises(Exception):
                await verify_access_token(session, access_token)
            await verify_access_token(session, new_access_token)
            self.assertEqual(rotated_session_id, session_id)

            with self.assertRaises(Exception):
                await rotate_refresh_token(session, refresh_token)

            token_result = await session.execute(select(UserToken).where(UserToken.user_id == "user-2"))
            stored_token = token_result.scalar_one()
            self.assertTrue(stored_token.is_revoked)

    async def test_distinct_logins_create_distinct_session_rows(self):
        async with self.session_factory() as session:
            session.add(
                TfUser(
                    user_id="user-3",
                    fullname="Multi Session User",
                    email="multi@example.com",
                    role="admin",
                    password=pwd_context.hash("secret"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            access_token_a = create_session_access_token(subject="user-3", session_id="session-a")
            refresh_token_a = create_session_refresh_token(subject="user-3", session_id="session-a")
            await user_token(
                session=session,
                user_id="user-3",
                role="admin",
                access_token=access_token_a,
                refresh_token=refresh_token_a,
                session_id="session-a",
            )

            access_token_b = create_session_access_token(subject="user-3", session_id="session-b")
            refresh_token_b = create_session_refresh_token(subject="user-3", session_id="session-b")
            await user_token(
                session=session,
                user_id="user-3",
                role="admin",
                access_token=access_token_b,
                refresh_token=refresh_token_b,
                session_id="session-b",
            )

            token_result = await session.execute(select(UserToken).where(UserToken.user_id == "user-3"))
            stored_tokens = token_result.scalars().all()
            self.assertEqual(len(stored_tokens), 2)
            self.assertTrue(all(token.session_id not in {"session-a", "session-b"} for token in stored_tokens))

    async def test_login_response_keeps_session_handle_out_of_json_body(self):
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
        creds = SimpleNamespace(username="admin@example.com", password="secret")

        async with self.session_factory() as session:
            with (
                patch("app.api.v1.endpoint.auth.authenticate_user", return_value=(SimpleNamespace(user_id="user-10"), "admin")),
                patch(
                    "app.api.v1.endpoint.auth.user_token",
                    return_value={
                        "access_token": "issued-access-token",
                        "refresh_token": "issued-refresh-token",
                        "session_id": "opaque-session-handle",
                    },
                ),
            ):
                response = await login_user(
                    request=request,
                    creds=creds,
                    remember_me=True,
                    session=session,
                )

        payload = json.loads(response.body.decode("utf-8"))
        self.assertNotIn("session_id", payload)
        self.assertEqual(payload["access_token"], "issued-access-token")
        self.assertIn("tf_session=opaque-session-handle", response.headers.get("set-cookie", ""))

    async def test_refresh_requires_cookie_only_and_does_not_return_session_id(self):
        request_without_cookie = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/token/refresh",
                "headers": [(b"content-type", b"application/json")],
                "client": ("127.0.0.1", 5000),
                "scheme": "http",
                "server": ("testserver", 80),
            }
        )
        async with self.session_factory() as session:
            response = await refresh_user_token(
                request=request_without_cookie,
                session=session,
            )
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["success"])

        request_with_cookie = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/token/refresh",
                "headers": [(b"cookie", b"tf_session=opaque-session-handle")],
                "client": ("127.0.0.1", 5000),
                "scheme": "http",
                "server": ("testserver", 80),
            }
        )
        async with self.session_factory() as session:
            with patch(
                "app.api.v1.endpoint.auth.rotate_session_handle",
                return_value=("new-access-token", "admin", "user-10", "opaque-session-handle"),
            ):
                response = await refresh_user_token(
                    request=request_with_cookie,
                    session=session,
                )

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["access_token"], "new-access-token")
        self.assertNotIn("session_id", payload)
        self.assertIn("tf_session=opaque-session-handle", response.headers.get("set-cookie", ""))

    async def test_expired_access_token_returns_401(self):
        async with self.session_factory() as session:
            session_id = "expired-session"
            session.add(
                TfUser(
                    user_id="user-expired",
                    fullname="Expired User",
                    email="expired@example.com",
                    role="admin",
                    password=pwd_context.hash("StrongPass1!"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            expired_access_token = create_session_access_token(
                subject="user-expired",
                session_id=session_id,
                expires_delta=timedelta(minutes=-1),
            )
            refresh_token = create_session_refresh_token(subject="user-expired", session_id=session_id)
            await user_token(
                session=session,
                user_id="user-expired",
                role="admin",
                access_token=expired_access_token,
                refresh_token=refresh_token,
                session_id=session_id,
            )

            with self.assertRaises(HTTPException) as error:
                await get_current_token_data(token=expired_access_token, session=session)

        self.assertEqual(error.exception.status_code, 401)
        self.assertEqual(error.exception.detail, "Access token has expired")

    async def test_revoked_session_cookie_refresh_returns_401_and_clears_cookie(self):
        async with self.session_factory() as session:
            session.add(
                TfUser(
                    user_id="user-revoked",
                    fullname="Revoked User",
                    email="revoked@example.com",
                    role="admin",
                    password=pwd_context.hash("StrongPass1!"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            await session.commit()

            session_id = "revoked-session"
            access_token = create_session_access_token(subject="user-revoked", session_id=session_id)
            refresh_token = create_session_refresh_token(subject="user-revoked", session_id=session_id)
            await user_token(
                session=session,
                user_id="user-revoked",
                role="admin",
                access_token=access_token,
                refresh_token=refresh_token,
                session_id=session_id,
            )

            token_row = (
                await session.execute(select(UserToken).where(UserToken.user_id == "user-revoked"))
            ).scalar_one()
            token_row.logged_in = False
            token_row.is_revoked = True
            await session.commit()

            request = Request(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/api/token/refresh",
                    "headers": [(b"cookie", b"tf_session=revoked-session")],
                    "client": ("127.0.0.1", 5000),
                    "scheme": "http",
                    "server": ("testserver", 80),
                }
            )
            response = await refresh_user_token(request=request, session=session)

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(response.status_code, 401)
        self.assertFalse(payload["success"])
        self.assertIn("Max-Age=0", response.headers.get("set-cookie", ""))

    async def test_logout_all_sessions_self_service_revokes_all_and_clears_cookie(self):
        async with self.session_factory() as session:
            current_user = TfUser(
                user_id="user-self",
                fullname="Self User",
                email="self@example.com",
                role="admin",
                password=pwd_context.hash("StrongPass1!"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            session.add(current_user)
            await session.commit()

            for session_id in ("self-a", "self-b"):
                await user_token(
                    session=session,
                    user_id="user-self",
                    role="admin",
                    access_token=create_session_access_token(subject="user-self", session_id=session_id),
                    refresh_token=create_session_refresh_token(subject="user-self", session_id=session_id),
                    session_id=session_id,
                )

            request = Request(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/api/logout-all",
                    "headers": [(b"cookie", b"tf_session=self-a")],
                    "client": ("127.0.0.1", 5000),
                    "scheme": "http",
                    "server": ("testserver", 80),
                }
            )
            response = await logout_all_user_sessions(
                request=request,
                payload=SimpleNamespace(user_id=None),
                session=session,
                current_user=current_user,
            )

            token_rows = (
                await session.execute(select(UserToken).where(UserToken.user_id == "user-self"))
            ).scalars().all()

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["revoked_sessions"], 2)
        self.assertTrue(all(token.is_revoked and not token.logged_in for token in token_rows))
        self.assertIn("Max-Age=0", response.headers.get("set-cookie", ""))

    async def test_logout_all_sessions_allows_superadmin_to_target_other_user(self):
        async with self.session_factory() as session:
            superadmin = TfUser(
                user_id="super-1",
                fullname="Super Admin",
                email="super@example.com",
                role="superadmin",
                password=pwd_context.hash("StrongPass1!"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            target_user = TfUser(
                user_id="target-1",
                fullname="Target User",
                email="target@example.com",
                role="sales",
                password=pwd_context.hash("StrongPass1!"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )
            session.add_all([superadmin, target_user])
            await session.commit()

            await user_token(
                session=session,
                user_id="target-1",
                role="sales",
                access_token=create_session_access_token(subject="target-1", session_id="target-session"),
                refresh_token=create_session_refresh_token(subject="target-1", session_id="target-session"),
                session_id="target-session",
            )

            request = Request(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/api/logout-all",
                    "headers": [],
                    "client": ("127.0.0.1", 5000),
                    "scheme": "http",
                    "server": ("testserver", 80),
                }
            )
            response = await logout_all_user_sessions(
                request=request,
                payload=SimpleNamespace(user_id="target-1"),
                session=session,
                current_user=superadmin,
            )

            token_rows = (
                await session.execute(select(UserToken).where(UserToken.user_id == "target-1"))
            ).scalars().all()

        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["revoked_sessions"], 1)
        self.assertTrue(all(token.is_revoked and not token.logged_in for token in token_rows))

    async def test_duplicate_email_and_session_constraints_raise_integrity_error(self):
        async with self.session_factory() as session:
            await create_account(
                session=session,
                fullname="Primary User",
                email="duplicate@example.com",
                role="admin",
                password="StrongPass1!",
            )

            session.add(
                TfUser(
                    user_id="dup-user-2",
                    fullname="Duplicate User",
                    email="duplicate@example.com",
                    role="sales",
                    password=pwd_context.hash("StrongPass1!"),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            )
            with self.assertRaises(IntegrityError):
                await session.commit()
            await session.rollback()

            session.add_all(
                [
                    UserToken(
                        session_id="same-session",
                        user_id="u1",
                        page="home",
                        logged_in=True,
                        role="admin",
                        expiry=datetime.now(),
                        access_token="access-a",
                        refresh_token="refresh-a",
                        is_revoked=False,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    ),
                    UserToken(
                        session_id="same-session",
                        user_id="u2",
                        page="home",
                        logged_in=True,
                        role="sales",
                        expiry=datetime.now(),
                        access_token="access-b",
                        refresh_token="refresh-b",
                        is_revoked=False,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                    ),
                ]
            )
            with self.assertRaises(IntegrityError):
                await session.commit()

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
