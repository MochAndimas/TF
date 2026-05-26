"""Orchestration layer for auth endpoint flows."""

from __future__ import annotations

import secrets

from fastapi import Request, status
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from jose import JWTError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.auth_helpers import (
    clear_auth_session_cookie,
    set_auth_session_cookie,
)
from app.core.config import settings
from app.core.security import (
    create_session_access_token,
    create_session_refresh_token,
    rotate_session_handle,
)
from app.db.models.user import TfUser
from app.schemas.user import (
    LoginResponse,
    LogoutAllSessionsResponse,
    MessageResponse,
    TokenData,
    TokenRefreshResponse,
)
from app.utils.user_utils import (
    authenticate_user,
    logout,
    logout_all_sessions,
    record_auth_event,
    user_token,
)


def _no_store_headers(response: JSONResponse) -> JSONResponse:
    """Attach no-store headers for sensitive auth responses."""
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


async def login_response(
    *,
    request: Request,
    creds: OAuth2PasswordRequestForm,
    remember_me: bool,
    session: AsyncSession,
) -> JSONResponse:
    """Authenticate credentials and return login response with cookie policy."""
    user, user_role = await authenticate_user(
        email=creds.username,
        password=creds.password,
        session=session,
        client_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )

    provisional_session_id = secrets.token_urlsafe(32)
    access_token = create_session_access_token(subject=user.user_id, session_id=provisional_session_id)
    refresh_token = create_session_refresh_token(subject=user.user_id, session_id=provisional_session_id)
    personal_token = await user_token(
        session=session,
        user_id=user.user_id,
        role=user_role,
        access_token=access_token,
        refresh_token=refresh_token,
        session_id=provisional_session_id,
        client_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
        auto_commit=False,
    )
    await session.commit()

    payload = LoginResponse(
        access_token=personal_token.get("access_token"),
        token_type="Bearer",
        role=user_role,
        user_id=user.user_id,
        success=True,
        message="Authentication successful.",
    )
    response = JSONResponse(content=payload.model_dump(mode="json"))
    if remember_me:
        set_auth_session_cookie(response, personal_token["session_id"])
    else:
        clear_auth_session_cookie(response)
    return _no_store_headers(response)


async def refresh_response(
    *,
    request: Request,
    session: AsyncSession,
) -> TokenRefreshResponse | MessageResponse | JSONResponse:
    """Rotate tokens with session cookie and return updated auth response."""
    session_id = request.cookies.get(settings.auth_cookie_name)
    if not session_id:
        return MessageResponse(success=False, message="No persisted session found.")

    try:
        access_token, role, user_id, refreshed_session_id = await rotate_session_handle(
            sqlite_session=session,
            session_id=session_id,
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
            auto_commit=False,
        )
    except JWTError:
        await session.commit()
        response = JSONResponse(
            content=MessageResponse(
                success=False,
                message="Refresh session is invalid or expired.",
            ).model_dump(mode="json"),
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
        clear_auth_session_cookie(response)
        return response
    await session.commit()

    payload = TokenRefreshResponse(
        access_token=access_token,
        token_type="Bearer",
        role=role,
        user_id=user_id,
        success=True,
        message="Token refreshed successfully.",
    )
    response = JSONResponse(content=payload.model_dump(mode="json"))
    set_auth_session_cookie(response, refreshed_session_id)
    return _no_store_headers(response)


async def logout_response(
    *,
    request: Request,
    session: AsyncSession,
    current_user: TfUser,
    token_data: TokenData,
) -> JSONResponse:
    """Logout current session, write audit trail, and clear auth cookie."""
    result = await logout(
        session=session,
        user_id=current_user.user_id,
        session_id=token_data.session_id,
        auto_commit=False,
    )
    await record_auth_event(
        session=session,
        email=current_user.email,
        user_id=current_user.user_id,
        event_type="logout_success" if result else "logout_failed",
        success=result,
        detail="User logged out from current session." if result else "Session data not found during logout.",
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    await session.commit()
    if not result:
        return JSONResponse(
            content=MessageResponse(
                message="Session data not found",
                success=False,
            ).model_dump(mode="json"),
            status_code=status.HTTP_404_NOT_FOUND,
        )

    response = JSONResponse(
        content=MessageResponse(
            message="Successfully logged out",
            success=True,
        ).model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )
    clear_auth_session_cookie(response)
    return response


async def logout_all_response(
    *,
    request: Request,
    session: AsyncSession,
    current_user: TfUser,
    target_user_id: str | None,
) -> JSONResponse:
    """Logout all matching sessions and clear cookie for self-target actions."""
    revoked_sessions = await logout_all_sessions(
        session=session,
        actor=current_user,
        target_user_id=target_user_id,
        auto_commit=False,
    )
    resolved_target_user_id = target_user_id or current_user.user_id
    await record_auth_event(
        session=session,
        email=current_user.email,
        user_id=current_user.user_id,
        event_type="logout_all_sessions",
        success=True,
        detail=f"Revoked {revoked_sessions} session(s) for user {resolved_target_user_id}.",
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    await session.commit()
    payload = LogoutAllSessionsResponse(
        success=True,
        message="All matching sessions have been revoked.",
        revoked_sessions=revoked_sessions,
    )
    response = JSONResponse(
        content=payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )
    if resolved_target_user_id == current_user.user_id:
        clear_auth_session_cookie(response)
    return response
