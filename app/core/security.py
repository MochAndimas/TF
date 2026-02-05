from fastapi import Request, HTTPException, status
from datetime import datetime, timedelta
from typing import Any
from jose import jwt, JWTError
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.config import settings
from app.schemas.user import TokenData
from app.db.models.user import UserToken


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str):
    """
    Docstring for verify_password
    Verify a plain password against a hashed password.
    
    :param plain_password: plain user password
    :type plain_password: str
    :param hashed_password: hashed user password
    :type hash_password: str
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
        subject: str | Any,
        expires_delta: timedelta = timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTE
        )
) -> str:
    """
    Docstring for create_access_token
    
    :param subject: user_id of an user account
    :type subject: str | Any
    :param expires_delta: how long access token will last in minute
    :type expires_delta: timedelta
    :return: Access Token
    :rtype: str
    """
    expire = datetime.now() + expires_delta
    to_encode = {
        "exp": expire.timestamp(),
        "sub": str(subject),
        "type": "access"
    }
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def create_refresh_token(
        subject: str | Any,
        expires_delta: timedelta = timedelta(
            days=settings.REFRESH_TOKEN_EXPIRE_DAYS
        )
) -> str:
    """
    Docstring for create_refresh_token
    
    :param subject: Description
    :type subject: str | Any
    :param expires_delta: Description
    :type expires_delta: timedelta
    :return: Description
    :rtype: str
    """
    expire = datetime.now() + expires_delta
    to_encode = {
        "exp": expire.timestamp(),
        "sub": str(subject),
        "type": "refresh"
    }
    encode_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.ALGORITHM
    )
    return encode_jwt


async def refresh_access_token(
        sqlite_session: AsyncSession,
        refresh_token: str
) -> str:
    """
    Docstring for refresh_access_token
    
    :param sqlite_session: sqlite session maker
    :type sqlite_session: AsyncSession
    :param refresh_token: refresh token
    :type refresh_token: str
    :return: new access token
    :rtype: str
    """
    payload = jwt.decode(
        refresh_token,
        settings.JWT_SECRET_KEY,
        algorithms=[settings.ALGORITHM]
    )
    user_id = payload.get("sub")
    query_personal_token = select(UserToken).filter_by(user_id=user_id)
    personal_token_data = await sqlite_session.execute(query_personal_token)
    personal_token = personal_token_data.scalars().first()

    if not personal_token or payload.get("type") != "refresh":
        if personal_token.is_revoked:
            raise JWTError("Invalid refresh token")
        
    new_access_token = create_access_token(subject=user_id)
    personal_token.access_token = new_access_token
    personal_token.updated_at = datetime.now()
    await sqlite_session.commit()
    await sqlite_session.close()

    return new_access_token


async def verify_access_token(
        sqlite_session: AsyncSession,
        token: str
) -> TokenData:
    """
    Docstring for verify_access_token
    
    :param sqlite_session: sqlite session maker
    :type sqlite_session: AsyncSession
    :param token: user access token
    :type token: str
    :return: Access token data
    :rtype: TokenData
    """
    payload = jwt.decode(
        token,
        settings.JWT_SECRET_KEY,
        algorithms=[settings.ALGORITHM]
    )
    id: str = payload.get("sub")
    query_personal_token = select(UserToken).filter_by(user_id=id)
    personal_token_data = await sqlite_session.execute(query_personal_token)
    personal_token = personal_token_data.scalars().first()

    if not personal_token or payload.get("type") != "access":
        if personal_token.is_revoked:
            raise JWTError("Invalid access token")
        
    return TokenData(id=personal_token.user_id)


async def verify_csrf_token(
        request: Request
) -> str:
    csrf_token_from_request = request.cookies.get("csrf_token")
    csrf_token_from_session = request.session.get("csrf_token")

    if not csrf_token_from_request\
        or csrf_token_from_request\
        != csrf_token_from_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Ivalid CSRF Token!"
        )
    return csrf_token_from_request
