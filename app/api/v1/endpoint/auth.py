"""Auth HTTP endpoints."""

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import (
    raise_bad_request,
    raise_forbidden,
    require_roles_dep,
)
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.rbac import ANALYTICS_ROLES
from app.api.v1.endpoint.auth_helpers import (
    enforce_rate_limit,
    serialize_account,
    serialize_latest_run,
)
from app.services.auth_orchestrator import (
    login_response,
    logout_all_response,
    logout_response,
    refresh_response,
)
from app.utils.user_utils import (
    create_account,
    delete_account,
    get_home_context,
    get_current_token_data,
    get_current_user,
    list_accounts,
    update_account,
    validate_role_assignment,
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
    current_user: TfUser = Depends(require_roles_dep("superadmin")),
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
    if data.password != data.confirm_password:
        raise HTTPException(
            status_code=400,
            detail="Password confirmation does not match",
        )

    try:
        validate_role_assignment(current_user.role, data.role)
    except PermissionError as error:
        raise_forbidden(error)
    except ValueError as error:
        raise_bad_request(error)

    try:
        user = await create_account(
            session=session,
            fullname=data.fullname,
            email=data.email,
            role=data.role,
            password=data.password
        )
    except ValueError as error:
        raise_bad_request(error)
    
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
    current_user: TfUser = Depends(require_roles_dep("superadmin")),
):
    """Return all active accounts for the account-management page."""
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
        raise_forbidden(error)
    except ValueError as error:
        raise_bad_request(error)

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
    current_user: TfUser = Depends(require_roles_dep(*ANALYTICS_ROLES, "finance")),
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
    await enforce_rate_limit(
        request=request,
        session=session,
        scope="login",
        max_requests=8,
        window_seconds=60,
        extra=creds.username,
    )
    return await login_response(
        request=request,
        creds=creds,
        remember_me=remember_me,
        session=session,
    )


@router.post("/api/token/refresh", response_model=TokenRefreshResponse | MessageResponse)
async def refresh_user_token(
    request: Request,
    session: AsyncSession = Depends(get_db),
):
    """Rotate bearer tokens using an active persisted session handle."""
    await enforce_rate_limit(
        request=request,
        session=session,
        scope="token_refresh",
        max_requests=15,
        window_seconds=60,
    )
    return await refresh_response(
        request=request,
        session=session,
    )


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
    return await logout_response(
        request=request,
        session=session,
        current_user=current_user,
        token_data=token_data,
    )


@router.post("/api/logout-all", response_model=LogoutAllSessionsResponse)
async def logout_all_user_sessions(
    request: Request,
    payload: LogoutAllSessionsRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Revoke all sessions owned by the caller or by a superadmin-selected user."""
    try:
        return await logout_all_response(
            request=request,
            session=session,
            current_user=current_user,
            target_user_id=payload.user_id,
        )
    except PermissionError as error:
        raise_forbidden(error)


@router.delete(
    "/api/delete_account/{user_id}",
    status_code=status.HTTP_200_OK,
    response_model=DeleteAccountResponse,
)
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep("superadmin"))
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
    except ValueError as e:
        raise_bad_request(e)
