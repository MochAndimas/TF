"""Security and token utilities for authentication flows."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import HTTPException, Request, status
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.user import UserToken
from app.schemas.user import TokenData

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify plain-text password against stored hash.

    Args:
        plain_password (str): Raw password submitted by client.
        hashed_password (str): Hashed password stored in database.

    Returns:
        bool: ``True`` when the password is valid, otherwise ``False``.
    """
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(subject: str | Any, expires_delta: timedelta | None = None) -> str:
    """Create signed access JWT token.

    Args:
        subject (str | Any): Identity value to store in JWT ``sub`` claim.
        expires_delta (timedelta | None): Optional custom token lifetime.

    Returns:
        str: Encoded access JWT token.
    """
    expiry = datetime.now() + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTE))
    return _encode_token(
        subject=subject,
        token_type="access",
        expiry=expiry,
        secret=settings.JWT_SECRET_KEY,
    )


def create_refresh_token(subject: str | Any, expires_delta: timedelta | None = None) -> str:
    """Create signed refresh JWT token.

    Args:
        subject (str | Any): Identity value to store in JWT ``sub`` claim.
        expires_delta (timedelta | None): Optional custom token lifetime.

    Returns:
        str: Encoded refresh JWT token.
    """
    expiry = datetime.now() + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
    return _encode_token(
        subject=subject,
        token_type="refresh",
        expiry=expiry,
        secret=settings.JWT_REFRESH_SECRET_KEY,
    )


def _encode_token(subject: str | Any, token_type: str, expiry: datetime, secret: str) -> str:
    """Encode JWT payload with standardized claims.

    Args:
        subject (str | Any): User identifier or principal value.
        token_type (str): Logical token type such as ``access`` or ``refresh``.
        expiry (datetime): Token expiration timestamp.
        secret (str): Signing secret for JWT encoding.

    Returns:
        str: Encoded JWT token string.
    """
    payload = {"exp": expiry.timestamp(), "sub": str(subject), "type": token_type}
    return jwt.encode(payload, secret, algorithm=settings.ALGORITHM)


def _decode_token(token: str, secret: str) -> dict[str, Any]:
    """Decode JWT token payload.

    Args:
        token (str): Encoded JWT token.
        secret (str): Secret key used for signature verification.

    Returns:
        dict[str, Any]: Decoded payload claims.

    Raises:
        JWTError: Raised when token is invalid or expired.
    """
    return jwt.decode(token, secret, algorithms=[settings.ALGORITHM])


async def _get_user_token(sqlite_session: AsyncSession, user_id: str | None) -> UserToken | None:
    """Retrieve persisted token record for a user.

    Args:
        sqlite_session (AsyncSession): Active async SQLAlchemy session.
        user_id (str | None): User identifier from JWT payload.

    Returns:
        UserToken | None: Persisted token row when available, otherwise ``None``.
    """
    if not user_id:
        return None

    token_result = await sqlite_session.execute(select(UserToken).filter_by(user_id=user_id))
    return token_result.scalars().first()


async def refresh_access_token(sqlite_session: AsyncSession, refresh_token: str) -> str:
    """Validate refresh token and rotate access token in storage.

    Args:
        sqlite_session (AsyncSession): Active async SQLAlchemy session.
        refresh_token (str): Refresh token sent by client.

    Returns:
        str: Newly generated access token.

    Raises:
        JWTError: Raised when refresh token is invalid, revoked, or mismatched.
    """
    payload = _decode_token(refresh_token, settings.JWT_REFRESH_SECRET_KEY)
    user_id = payload.get("sub")
    stored_token = await _get_user_token(sqlite_session, user_id)

    if not stored_token or payload.get("type") != "refresh" or stored_token.is_revoked:
        raise JWTError("Invalid refresh token")

    new_access_token = create_access_token(subject=user_id)
    stored_token.access_token = new_access_token
    stored_token.updated_at = datetime.now()
    await sqlite_session.commit()
    return new_access_token


async def verify_access_token(sqlite_session: AsyncSession, token: str) -> TokenData:
    """Validate access token and map it into ``TokenData``.

    Args:
        sqlite_session (AsyncSession): Active async SQLAlchemy session.
        token (str): Access token to validate.

    Returns:
        TokenData: Parsed token data containing authenticated user ID.

    Raises:
        JWTError: Raised when token is invalid, revoked, or mismatched.
    """
    payload = _decode_token(token, settings.JWT_SECRET_KEY)
    user_id = payload.get("sub")
    stored_token = await _get_user_token(sqlite_session, user_id)

    if not stored_token or payload.get("type") != "access" or stored_token.is_revoked:
        raise JWTError("Invalid access token")

    return TokenData(id=stored_token.user_id)


async def verify_csrf_token(request: Request) -> str:
    """Validate CSRF token by comparing cookie and server-side session values.

    Args:
        request (Request): Incoming request containing session and cookie state.

    Returns:
        str: Valid CSRF token.

    Raises:
        HTTPException: Raised when CSRF token is missing or mismatched.
    """
    csrf_from_cookie = request.cookies.get("csrf_token")
    csrf_from_session = request.session.get("csrf_token")

    if not csrf_from_cookie or csrf_from_cookie != csrf_from_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid CSRF Token!",
        )
    return csrf_from_cookie

