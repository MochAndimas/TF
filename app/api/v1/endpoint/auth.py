"""Auth module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import secrets
from collections import defaultdict, deque
from datetime import datetime
from threading import Lock

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.etl_run import EtlRun
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.user_utils import (
    authenticate_user,
    create_account,
    delete_account,
    get_home_context,
    get_current_token_data,
    get_current_user,
    list_accounts,
    logout,
    logout_all_sessions,
    record_auth_event,
    require_roles,
    update_account,
    user_token,
    validate_role_assignment,
)
from app.core.security import (
    create_session_access_token,
    create_session_refresh_token,
    rotate_session_handle,
)
from app.schemas.user import (
    AccountListResponse,
    AccountSummary,
    AccountUpdateRequest,
    AccountUpdateResponse,
    HomeContextResponse,
    HomeAccountSummary,
    LoginResponse,
    LatestEtlRunSummary,
    LogoutAllSessionsRequest,
    LogoutAllSessionsResponse,
    MessageResponse,
    RegisterBase,
    RegisterResponse,
    TokenData,
    TokenRefreshResponse,
)


router = APIRouter()
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()


def _serialize_account(user: TfUser) -> AccountSummary:
    """Convert one SQLAlchemy user model into a response-safe account payload."""
    return AccountSummary(
        user_id=user.user_id,
        fullname=user.fullname,
        email=user.email,
        role=user.role,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


def _serialize_latest_run(run: EtlRun | None) -> LatestEtlRunSummary | None:
    """Convert the latest ETL run model into a compact API payload."""
    if run is None:
        return None
    return LatestEtlRunSummary(
        run_id=run.run_id,
        pipeline=run.pipeline,
        source=run.source,
        mode=run.mode,
        status=run.status,
        message=run.message,
        error_detail=run.error_detail,
        window_start=run.window_start,
        window_end=run.window_end,
        started_at=run.started_at,
        ended_at=run.ended_at,
        triggered_by=run.triggered_by,
    )


def _set_auth_session_cookie(response: JSONResponse, session_id: str) -> None:
    """Persist the opaque session handle as an HttpOnly cookie."""
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=session_id,
        max_age=settings.auth_cookie_max_age,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        path="/",
    )


def _clear_auth_session_cookie(response: JSONResponse) -> None:
    """Expire the persistent auth cookie in the browser."""
    response.delete_cookie(
        key=settings.auth_cookie_name,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        path="/",
    )


def _client_identifier(request: Request, scope: str, extra: str | None = None) -> str:
    """Build a stable in-memory rate-limit key for one client/scope pair.

    Args:
        request (Request): Incoming HTTP request used to extract client network
            metadata.
        scope (str): Logical limiter namespace such as ``login`` or
            ``token_refresh``.
        extra (str | None): Optional discriminator, typically a username/email,
            that narrows the bucket when multiple identities may share one IP.

    Returns:
        str: Deterministic bucket identifier used by the in-process limiter to
        track request timestamps for a specific client and auth scope.
    """
    client_host = request.client.host if request.client else "unknown"
    suffix = f":{extra.strip().lower()}" if extra else ""
    return f"{scope}:{client_host}{suffix}"


def _enforce_rate_limit(
    request: Request,
    scope: str,
    max_requests: int,
    window_seconds: int,
    extra: str | None = None,
) -> None:
    """Enforce a simple sliding-window rate limit for sensitive auth actions.

    Args:
        request (Request): Incoming request used to derive the limiter bucket.
        scope (str): Logical limiter namespace for this endpoint family.
        max_requests (int): Maximum allowed requests in the active window.
        window_seconds (int): Sliding-window size in seconds.
        extra (str | None): Optional identity discriminator appended to the
            client bucket key.

    Returns:
        None: Records the current request timestamp when the budget has not been
        exceeded.

    Raises:
        HTTPException: ``429`` when the client exceeds the configured request
        budget for the window.
    """
    now = datetime.now().timestamp()
    bucket_key = _client_identifier(request, scope, extra)
    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and now - bucket[0] > window_seconds:
            bucket.popleft()
        if len(bucket) >= max_requests:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests. Please try again later.",
            )
        bucket.append(now)


@router.post("/api/register", response_model=RegisterResponse)
async def register(
    data: RegisterBase,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Register a new user account.

    Args:
        data (RegisterBase): Registration payload containing profile and password fields.
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user performing the action.
        csrf_token (str): Verified CSRF token from cookie/session check.

    Returns:
        JSONResponse: Success payload with created user identifier.

    Raises:
        HTTPException: Raised when password confirmation fails or email already exists.
    """
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(error),
        ) from error

    if data.password != data.confirm_password:
        raise HTTPException(
            status_code=400,
            detail="Password confirmation does not match",
        )

    try:
        validate_role_assignment(current_user.role, data.role)
    except PermissionError as error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(error),
        ) from error
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error

    try:
        user = await create_account(
            session=session,
            fullname=data.fullname,
            email=data.email,
            role=data.role,
            password=data.password
        )
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    
    if not user:
        raise HTTPException(
            status_code=409,
            detail="Email already registered",
        )
    
    return JSONResponse(
        content={
            "success": True,
            "message": "Account created successfully",
            "user_id": user.user_id,
        }
    )


@router.get("/api/accounts", response_model=AccountListResponse)
async def get_accounts(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Return all active accounts for the account-management page."""
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(error),
        ) from error

    users = await list_accounts(session=session)
    return AccountListResponse(
        success=True,
        message="Accounts loaded successfully.",
        data=[_serialize_account(user) for user in users],
    )


@router.patch("/api/accounts/{user_id}", response_model=AccountUpdateResponse)
async def patch_account(
    user_id: str,
    payload: AccountUpdateRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Update mutable account profile fields through the backend layer."""
    try:
        user = await update_account(
            session=session,
            actor=current_user,
            user_id=user_id,
            fullname=payload.fullname,
            email=payload.email,
            role=payload.role,
        )
    except PermissionError as error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(error),
        ) from error
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found!",
        )

    return AccountUpdateResponse(
        success=True,
        message="Account updated successfully.",
        data=_serialize_account(user),
    )


@router.get("/api/home/context", response_model=HomeContextResponse)
async def home_context(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Return the authenticated user's home-page context payload."""
    account, latest_run = await get_home_context(
        session=session,
        user_id=current_user.user_id,
    )
    if account is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found!",
        )

    return HomeContextResponse(
        success=True,
        message="Home context loaded successfully.",
        data={
            "account": HomeAccountSummary(
                user_id=account.user_id,
                fullname=account.fullname,
                email=account.email,
                role=account.role,
            ),
            "latest_run": _serialize_latest_run(latest_run),
        },
    )


@router.post("/api/login", response_model=LoginResponse)
async def login_user(
    request: Request,
    creds: OAuth2PasswordRequestForm = Depends(),
    remember_me: bool = Form(False),
    session: AsyncSession = Depends(get_db),
):
    """Authenticate user credentials and issue access/refresh tokens.

    Args:
        creds (OAuth2PasswordRequestForm): Login form with username/email and password.
        session (AsyncSession): Database session injected by FastAPI.
        csrf_token (str): Verified CSRF token from cookie/session check.

    Returns:
        JSONResponse: Authentication payload containing access token and role.

    Raises:
        HTTPException: Raised when account is deleted or credentials are invalid.
    """
    _enforce_rate_limit(
        request=request,
        scope="login",
        max_requests=8,
        window_seconds=60,
        extra=creds.username,
    )
    user, user_role = await authenticate_user(
        email=creds.username,
        password=creds.password,
        session=session,
        client_ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    
    # Create JWT token and store it to database
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
    )

    # Return only the short-lived access token in JSON response. The persistent
    # session handle stays cookie-only so browser-side code never sees it.
    response = JSONResponse(
        content={
            "access_token": personal_token.get("access_token"),
            "token_type": "Bearer",
            "role": user_role,
            "user_id": user.user_id,
            "success": True,
        }
    )
    if remember_me:
        _set_auth_session_cookie(response, personal_token["session_id"])
    else:
        _clear_auth_session_cookie(response)

    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"

    return response


@router.post("/api/token/refresh", response_model=TokenRefreshResponse)
async def refresh_user_token(
    request: Request,
    session: AsyncSession = Depends(get_db),
):
    """Rotate bearer tokens using an active persisted session handle."""
    _enforce_rate_limit(
        request=request,
        scope="token_refresh",
        max_requests=15,
        window_seconds=60,
    )
    session_id = request.cookies.get(settings.auth_cookie_name)

    if not session_id:
        return JSONResponse(
            content={"success": False, "message": "No persisted session found."},
            status_code=status.HTTP_200_OK,
        )

    try:
        access_token, role, user_id, refreshed_session_id = await rotate_session_handle(
            sqlite_session=session,
            session_id=session_id,
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
    except Exception:
        response = JSONResponse(
            content={"detail": "Refresh session is invalid or expired.", "success": False},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
        _clear_auth_session_cookie(response)
        return response

    response = JSONResponse(
        content={
            "access_token": access_token,
            "token_type": "Bearer",
            "role": role,
            "user_id": user_id,
            "success": True,
        }
    )
    _set_auth_session_cookie(response, refreshed_session_id)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


@router.post("/api/logout", response_model=MessageResponse)
async def logout_user(
    request: Request,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
    token_data: TokenData = Depends(get_current_token_data),
):
    """Log out the current user and revoke persisted token state.

    Args:
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user resolved from bearer token.

    Returns:
        JSONResponse: Success/failure status message for logout action.
    """
    result = await logout(
        session=session,
        user_id=current_user.user_id,
        session_id=token_data.session_id,
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
            content={"message": "Session data not found", "success": False},
            status_code=status.HTTP_404_NOT_FOUND,
        )

    response = JSONResponse(
        content={"message": "Successfully logged out", "success": True},
        status_code=status.HTTP_200_OK,
    )
    _clear_auth_session_cookie(response)
    return response


@router.post("/api/logout-all", response_model=LogoutAllSessionsResponse)
async def logout_all_user_sessions(
    request: Request,
    payload: LogoutAllSessionsRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Revoke all sessions owned by the caller or by a superadmin-selected user."""
    try:
        revoked_sessions = await logout_all_sessions(
            session=session,
            actor=current_user,
            target_user_id=payload.user_id,
        )
    except PermissionError as error:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(error)) from error

    target_user_id = payload.user_id or current_user.user_id
    await record_auth_event(
        session=session,
        email=current_user.email,
        user_id=current_user.user_id,
        event_type="logout_all_sessions",
        success=True,
        detail=f"Revoked {revoked_sessions} session(s) for user {target_user_id}.",
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    await session.commit()
    response = JSONResponse(
        content={
            "success": True,
            "message": "All matching sessions have been revoked.",
            "revoked_sessions": revoked_sessions,
        },
        status_code=status.HTTP_200_OK,
    )
    if target_user_id == current_user.user_id:
        _clear_auth_session_cookie(response)
    return response


@router.delete("/api/delete_account/{user_id}", status_code=status.HTTP_200_OK)
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user)
):  
    """Soft-delete a target user account.

    Args:
        user_id (str): Identifier of the user account to be deleted.
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user performing the action.

    Returns:
        dict[str, str | bool]: Success payload with deleted user information.

    Raises:
        HTTPException: Raised for authorization errors, invalid operations, or missing user.
    """
    try:
        require_roles(current_user, "superadmin")
        result = await delete_account(
            session=session,
            user_id=user_id,
            current_user=current_user
        )

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found!"
            )
        return {
            "message": "User Deleted Successfully!",
            "deleted_user_id": user_id,
            "success": True
        }
    except PermissionError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not Authorized"
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
