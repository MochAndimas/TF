"""Persisted auth-session lifecycle helpers."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import fingerprint_session_id, fingerprint_token
from app.db.models.user import TfUser, UserToken


async def user_token(
    session: AsyncSession,
    user_id: str,
    role: str,
    access_token: str,
    refresh_token: str,
    session_id: str | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
):
    """Create or update token/session row for a user login."""
    today = datetime.now()
    session_id = session_id or str(uuid.uuid4())
    expiry = today + timedelta(days=7)

    session_fingerprint = fingerprint_session_id(session_id)
    result = await session.execute(
        select(UserToken).where(UserToken.session_id == session_fingerprint)
    )
    user = result.scalars().first()

    if user:
        user.session_id = session_fingerprint
        user.user_id = user_id
        user.logged_in = True
        user.expiry = expiry
        user.is_revoked = False
        user.role = role
        user.access_token = fingerprint_token(access_token)
        user.refresh_token = fingerprint_token(refresh_token)
        user.updated_at = today
        user.last_seen_ip = client_ip
        user.last_seen_user_agent = user_agent
        user.last_rotated_at = today
    else:
        session.add(
            UserToken(
                session_id=session_fingerprint,
                user_id=user_id,
                page="home",
                logged_in=True,
                role=role,
                expiry=expiry,
                access_token=fingerprint_token(access_token),
                refresh_token=fingerprint_token(refresh_token),
                is_revoked=False,
                created_at=today,
                updated_at=today,
                created_ip=client_ip,
                last_seen_ip=client_ip,
                last_seen_user_agent=user_agent,
                last_rotated_at=today,
            )
        )
    await session.commit()

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "session_id": session_id,
    }


async def logout(
    session: AsyncSession,
    user_id,
    session_id: str | None = None,
):
    """Revoke a user's active token row and mark the session as logged out."""
    today = datetime.now()
    query = select(UserToken).where(UserToken.user_id == user_id)
    if session_id:
        query = query.where(UserToken.session_id == fingerprint_session_id(session_id))

    user_data = await session.execute(query)
    user = user_data.scalars().first()
    if user is None:
        return False

    user.logged_in = False
    user.is_revoked = True
    user.expiry = today
    user.updated_at = today

    await session.commit()
    return True


async def logout_all_sessions(
    session: AsyncSession,
    *,
    actor: TfUser,
    target_user_id: str | None = None,
) -> int:
    """Revoke all sessions for the actor or a superadmin-selected target user."""
    normalized_role = (actor.role or "").strip().lower()
    resolved_target_user_id = target_user_id or actor.user_id
    if normalized_role != "superadmin" and resolved_target_user_id != actor.user_id:
        raise PermissionError("Not authorized to revoke sessions for another user.")

    query = select(UserToken).where(
        UserToken.user_id == resolved_target_user_id,
        UserToken.logged_in.is_(True),
        UserToken.is_revoked.is_(False),
    )
    token_rows = (await session.execute(query)).scalars().all()
    if not token_rows:
        return 0

    now = datetime.now()
    for token_row in token_rows:
        token_row.logged_in = False
        token_row.is_revoked = True
        token_row.expiry = now
        token_row.updated_at = now

    await session.commit()
    return len(token_rows)
