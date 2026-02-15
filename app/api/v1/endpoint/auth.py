import secrets
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, APIRouter, HTTPException
from fastapi import status, Response, Request, Header
from fastapi.responses import JSONResponse
from fastapi.security.oauth2 import OAuth2PasswordRequestForm
from app.core.config import settings
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.user_utils import get_current_user, user_token, delete_account
from app.utils.user_utils import roles, logout, create_account
from app.core.security import verify_password, create_access_token
from app.core.security import create_refresh_token, verify_csrf_token
from app.schemas.user import TokenBase, RegisterBase


router = APIRouter()


@router.post("/api/register", response_model=RegisterBase)
async def register(
    request: Request,
    data: RegisterBase,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
    x_csrf_token: str = Header(None),
):
    """
    Docstring for register
    
    :param data: Description
    :type data: RegisterBase
    :param session: Description
    :type session: AsyncSession
    """
    csrf_cookie = request.headers.get("X-CSRF-Token")
    print(csrf_cookie)

    if not csrf_cookie or csrf_cookie != x_csrf_token:
        raise HTTPException(status_code=403, detail="Invalid CSRF token")
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
    
    response = JSONResponse(
        content={
            "success": True,
            "message": "Account created successfully",
            "user_id": user.user_id,
        }
    )

    return response


@router.post("/api/login", response_model=TokenBase)
async def login_user(
    creds: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_db),
    csrf_token: str = Depends(verify_csrf_token)
):
    """
    Docstring for login_user
    
    :param creds: Oauth2 password request Form
    :type creds: OAuth2PasswordRequestForm
    :param session: Sqlite AsyncSession Maker
    :type session: AsyncSession
    :param csrf_token: csrf security token for http form.
    :type csrf_token: str
    """
    query = await session.execute(
        select(
            TfUser
        ).where(
            TfUser.email == creds.username,
            TfUser.deleted_at == None
        )
    )
    user = query.scalar()
    user_role = await roles(creds.username, session)
    
    if user == None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account has been deleted!",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if not user or not user_role or not verify_password(creds.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or Password!",
            headers={"WWW-Authenticate": "Bearer"}
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
    response.headers["Authentication"] = str(user.user_id)

    return response


@router.post("/api/login/csrf-token")
async def get_csrf_token(
    response: Response,
    request: Request,
    session: AsyncSession = Depends(get_db),
    creds: OAuth2PasswordRequestForm = Depends()
):
    """
    This endpoint initializes the CSRF token by setting it as an HTTP-only cookie.
    
    :param response: Fast API HTTPS response Form
    :type response: Response
    :param request: Fast API HTTPS response Form
    :type request: Request
    :param session: Sqlite AsyncSession Maker
    :type session: AsyncSession
    :param creds: Oauth2 password request Form
    :type creds: OAuth2PasswordRequestForm
    """
    # retrieve user from the database by email
    query = await session.execute(select(TfUser).where(TfUser.email == creds.username, TfUser.deleted_at == None))
    user = query.scalar()
    user_role = await roles(creds.username, session)

    if user == None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account has been deleted!",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if not user or not user_role or not verify_password(creds.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or Password!",
            headers={"WWW-Authenticate": "Bearer"}
        )
    if "csrf_token" not in request.session:
        csrf_token = secrets.token_hex(16)
        request.session["csrf_token"] = csrf_token
    else:
        csrf_token = request.session["csrf_token"]

    # Set CSRF token as an HTTP-only cookie
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=True,
        secure=False if settings.DEBUG else True
    )

    return {"message": "CSRF token initialized."}


@router.post("/api/logout")
async def logout_user(
    response: Response,
    session: AsyncSession = Depends(get_db), 
    current_user: TfUser = Depends(get_current_user)):
    """
    Logs out a user by clearing the refresh token cookie.
    """
    try:
        await logout(session=session, user_id=current_user.user_id)
    except Exception:
        response = response = JSONResponse(
            content={"message": "Something error, please try again!", "success": False},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
    # Clear the refresh token by setting an expired cookie
    response = JSONResponse(
        content={"message": "Successfully logged out", "success": True},
        status_code=status.HTTP_200_OK
    )
    response.delete_cookie(
        key="refresh_token",
        httponly=True,  # Prevents JavaScript access
        secure=True,    # Ensures it's only sent over HTTPS
        samesite="strict",  # Adjust as needed)
    )
    return response


@router.delete("/api/delete_account/{user_id}", status_code=status.HTTP_200_OK)
async def delete_user(
    user_id: str,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user)
):  
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
