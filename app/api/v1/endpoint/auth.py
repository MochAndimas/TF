"""Auth module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import secrets
from datetime import datetime
from collections import defaultdict, deque
from threading import Lock
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from app.core.config import settings
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.user_utils import (
    authenticate_user,
    create_account,
    delete_account,
    get_current_user,
    logout,
    require_roles,
    user_token,
    validate_role_assignment,
)
from app.core.security import (
    create_session_access_token,
    create_session_refresh_token,
    rotate_refresh_token,
)
from app.schemas.user import (
    LoginResponse,
    MessageResponse,
    RegisterBase,
    RegisterResponse,
    TokenRefreshRequest,
    TokenRefreshResponse,
)


router = APIRouter()
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()


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
        require_roles(current_user, "admin", "superadmin")
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

    user = await create_account(
        session=session,
        fullname=data.fullname,
        email=data.email,
        role=data.role,
        password=data.password
    )

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


@router.post("/api/login", response_model=LoginResponse)
async def login_user(
    request: Request,
    creds: OAuth2PasswordRequestForm = Depends(),
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
    )

    # Return access token in JSON response and refresh token in headers
    response = JSONResponse(
        content={
            "access_token": personal_token.get("access_token"),
            "token_type": "Bearer",
            "role": user_role,
            "user_id": user.user_id,
            "refresh_token": personal_token.get("refresh_token"),
            "session_id": personal_token.get("session_id"),
            "success": True
        }
    )

    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Authentication"] = str(user.user_id)  # consumed by Streamlit client

    return response


@router.post("/api/token/refresh", response_model=TokenRefreshResponse)
async def refresh_user_token(
    request: Request,
    payload: TokenRefreshRequest,
    session: AsyncSession = Depends(get_db),
):
    """Rotate bearer tokens using an active refresh token."""
    _enforce_rate_limit(
        request=request,
        scope="token_refresh",
        max_requests=15,
        window_seconds=60,
    )
    try:
        access_token, refresh_token, role, user_id = await rotate_refresh_token(
            sqlite_session=session,
            refresh_token=payload.refresh_token,
        )
    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token is invalid or expired.",
        ) from error

    response = JSONResponse(
        content={
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "Bearer",
            "role": role,
            "user_id": user_id,
            "success": True,
        }
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


@router.post("/api/logout", response_model=MessageResponse)
async def logout_user(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Log out the current user and revoke persisted token state.

    Args:
        session (AsyncSession): Database session injected by FastAPI.
        current_user (TfUser): Authenticated user resolved from bearer token.

    Returns:
        JSONResponse: Success/failure status message for logout action.
    """
    result = await logout(session=session, user_id=current_user.user_id)
    if not result:
        return JSONResponse(
            content={"message": "Session data not found", "success": False},
            status_code=status.HTTP_404_NOT_FOUND,
        )

    response = JSONResponse(
        content={"message": "Successfully logged out", "success": True},
        status_code=status.HTTP_200_OK,
    )
    response.delete_cookie(
        key="refresh_token",
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
    )
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
        require_roles(current_user, "admin", "superadmin")
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
