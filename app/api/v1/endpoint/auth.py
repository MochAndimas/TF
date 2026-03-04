import secrets
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
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
    user_token,
)
from app.core.security import create_access_token, create_refresh_token, verify_csrf_token
from app.schemas.user import LoginResponse, MessageResponse, RegisterBase, RegisterResponse


router = APIRouter()


def _set_csrf_cookie(response: Response, csrf_token: str) -> None:
    """Attach CSRF token cookie with environment-aware security flags."""
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
    )


@router.post("/api/register", response_model=RegisterResponse)
async def register(
    data: RegisterBase,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
    csrf_token: str = Depends(verify_csrf_token),
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
    creds: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Depends(verify_csrf_token)
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
    user, user_role = await authenticate_user(
        email=creds.username,
        password=creds.password,
        session=session,
    )
    
    # Create JWT token and store it to database
    personal_token = await user_token(
        session=session,
        user_id=user.user_id,
        role=user_role,
        access_token=create_access_token(subject=user.user_id),
        refresh_token=create_refresh_token(subject=user.user_id)
    )

    # Return access token in JSON response and refresh token in headers
    response = JSONResponse(
        content={
            "access_token": personal_token.get("access_token"),
            "token_type": "Bearer",
            "role": user_role,
            "success": True
        }
    )

    response.headers["Authentication"] = str(user.user_id)  # consumed by Streamlit client

    return response


@router.post("/api/login/csrf-token", response_model=MessageResponse)
async def get_csrf_token(
    response: Response,
    request: Request,
    session: AsyncSession = Depends(get_db),
    creds: OAuth2PasswordRequestForm = Depends()
):
    """Initialize CSRF token for a valid login attempt.

    Args:
        response (Response): Mutable FastAPI response used to set cookies.
        request (Request): Incoming request containing the session store.
        session (AsyncSession): Database session injected by FastAPI.
        creds (OAuth2PasswordRequestForm): Login form with username/email and password.

    Returns:
        dict[str, str]: Confirmation message after CSRF cookie is set.

    Raises:
        HTTPException: Raised when user credentials are invalid.
    """
    await authenticate_user(
        email=creds.username,
        password=creds.password,
        session=session,
    )

    if "csrf_token" not in request.session:
        csrf_token = secrets.token_hex(16)
        request.session["csrf_token"] = csrf_token
    else:
        csrf_token = request.session["csrf_token"]

    _set_csrf_cookie(response, csrf_token)

    return {"message": "CSRF token initialized.", "success": True}


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
        secure=True,
        samesite="strict",
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
