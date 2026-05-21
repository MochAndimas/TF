"""Auth module.

This module is part of `app.api.v1.endpoint` and contains runtime logic used by the
Traders Family application.
"""

import secrets

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.rbac import ANALYTICS_ROLES, FINANCE_ANALYTICS_ROLES
from app.api.v1.endpoint.auth_helpers import (
    clear_auth_session_cookie,
    enforce_rate_limit,
    serialize_account,
    serialize_latest_run,
    set_auth_session_cookie,
)
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
    AccountUpdateRequest,
    AccountUpdateResponse,
    DeleteAccountResponse,
    HomeContextResponse,
    HomeAccountSummary,
    LoginResponse,
    LogoutAllSessionsRequest,
    LogoutAllSessionsResponse,
    MessageResponse,
    RegisterBase,
    RegisterResponse,
    TokenData,
    TokenRefreshResponse,
)


router = APIRouter()


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
    
    return RegisterResponse(
        success=True,
        message="Account created successfully",
        user_id=user.user_id,
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
        data=[serialize_account(user) for user in users],
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
        data=serialize_account(user),
    )


@router.get("/api/home/context", response_model=HomeContextResponse)
async def home_context(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Return the authenticated user's home-page context payload."""
    try:
        require_roles(current_user, *ANALYTICS_ROLES, "finance")
    except PermissionError as error:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(error)) from error

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
            "latest_run": serialize_latest_run(latest_run),
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
    enforce_rate_limit(
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
    payload = LoginResponse(
        access_token=personal_token.get("access_token"),
        token_type="Bearer",
        role=user_role,
        user_id=user.user_id,
        success=True,
        message="Authentication successful.",
    )
    response = JSONResponse(
        content=payload.model_dump(mode="json")
    )
    if remember_me:
        set_auth_session_cookie(response, personal_token["session_id"])
    else:
        clear_auth_session_cookie(response)

    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"

    return response


@router.post("/api/token/refresh", response_model=TokenRefreshResponse | MessageResponse)
async def refresh_user_token(
    request: Request,
    session: AsyncSession = Depends(get_db),
):
    """Rotate bearer tokens using an active persisted session handle."""
    enforce_rate_limit(
        request=request,
        scope="token_refresh",
        max_requests=15,
        window_seconds=60,
    )
    session_id = request.cookies.get(settings.auth_cookie_name)

    if not session_id:
        return MessageResponse(success=False, message="No persisted session found.")

    try:
        access_token, role, user_id, refreshed_session_id = await rotate_session_handle(
            sqlite_session=session,
            session_id=session_id,
            ip_address=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
    except Exception:
        response = JSONResponse(
            content=MessageResponse(
                success=False,
                message="Refresh session is invalid or expired.",
            ).model_dump(mode="json"),
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
        clear_auth_session_cookie(response)
        return response

    payload = TokenRefreshResponse(
        access_token=access_token,
        token_type="Bearer",
        role=role,
        user_id=user_id,
        success=True,
        message="Token refreshed successfully.",
    )
    response = JSONResponse(
        content=payload.model_dump(mode="json")
    )
    set_auth_session_cookie(response, refreshed_session_id)
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
    payload = LogoutAllSessionsResponse(
        success=True,
        message="All matching sessions have been revoked.",
        revoked_sessions=revoked_sessions,
    )
    response = JSONResponse(
        content=payload.model_dump(mode="json"),
        status_code=status.HTTP_200_OK,
    )
    if target_user_id == current_user.user_id:
        clear_auth_session_cookie(response)
    return response


@router.delete(
    "/api/delete_account/{user_id}",
    status_code=status.HTTP_200_OK,
    response_model=DeleteAccountResponse,
)
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
        return DeleteAccountResponse(
            message="User Deleted Successfully!",
            deleted_user_id=user_id,
            success=True,
        )
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
