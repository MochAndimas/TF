"""Security and token utilities for authentication flows."""

from __future__ import annotations

import hashlib
import base64
import uuid
from datetime import datetime, timedelta
from typing import Any

from jose import JWTError, jwt
from cryptography.fernet import Fernet
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


def create_session_access_token(
    subject: str | Any,
    session_id: str,
    expires_delta: timedelta | None = None,
) -> str:
    """Create an access token bound to a specific persisted session."""
    expiry = datetime.now() + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTE))
    return _encode_token(
        subject=subject,
        session_id=session_id,
        token_type="access",
        expiry=expiry,
        secret=settings.JWT_SECRET_KEY,
    )


def create_session_refresh_token(
    subject: str | Any,
    session_id: str,
    expires_delta: timedelta | None = None,
) -> str:
    """Create a refresh token bound to a specific persisted session."""
    expiry = datetime.now() + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
    return _encode_token(
        subject=subject,
        session_id=session_id,
        token_type="refresh",
        expiry=expiry,
        secret=settings.JWT_REFRESH_SECRET_KEY,
    )


def fingerprint_token(token: str) -> str:
    """Return a non-reversible fingerprint for a token value."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def fingerprint_session_id(session_id: str) -> str:
    """Return a stable fingerprint for a session identifier."""
    return fingerprint_token(session_id)


def _session_matches(stored_session_id: str | None, session_id: str | None) -> bool:
    """Support raw-to-hashed session ID transition during lookups."""
    if not stored_session_id or not session_id:
        return False
    return stored_session_id in {session_id, fingerprint_session_id(session_id)}


def _encode_token(
    subject: str | Any,
    session_id: str | None,
    token_type: str,
    expiry: datetime,
    secret: str,
) -> str:
    """Encode JWT payload with standardized claims.

    Args:
        subject (str | Any): User identifier or principal value.
        token_type (str): Logical token type such as ``access`` or ``refresh``.
        expiry (datetime): Token expiration timestamp.
        secret (str): Signing secret for JWT encoding.

    Returns:
        str: Encoded JWT token string.
    """
    payload = {
        "exp": expiry.timestamp(),
        "sub": str(subject),
        "type": token_type,
        "jti": str(uuid.uuid4()),
    }
    if session_id:
        payload["sid"] = session_id
    return jwt.encode(payload, secret, algorithm=settings.ALGORITHM)


def _fernet_key_material() -> bytes:
    """Derive a valid Fernet key from configured application secret material."""
    seed = (settings.APP_ENCRYPTION_KEY or settings.JWT_SECRET_KEY).encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(seed).digest())


def encrypt_secret(value: str) -> str:
    """Encrypt a secret value for backend-at-rest storage."""
    return Fernet(_fernet_key_material()).encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str) -> str:
    """Decrypt a previously encrypted backend secret."""
    return Fernet(_fernet_key_material()).decrypt(value.encode("utf-8")).decode("utf-8")


def _decode_token(token: str, secret: str) -> dict[str, Any]:
    """Decode and verify a JWT payload using the supplied signing secret.

    Args:
        token (str): Encoded JWT token.
        secret (str): Secret key used for signature verification.

    Returns:
        dict[str, Any]: Decoded payload claims.

    Raises:
        JWTError: Raised when token is invalid or expired.
    """
    return jwt.decode(token, secret, algorithms=[settings.ALGORITHM])


async def _get_user_token(
    sqlite_session: AsyncSession,
    user_id: str | None,
    session_id: str | None = None,
) -> UserToken | None:
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
    token_rows = token_result.scalars().all()
    if not session_id:
        return token_rows[0] if token_rows else None
    return next(
        (row for row in token_rows if _session_matches(row.session_id, session_id)),
        None,
    )


async def rotate_refresh_token(
    sqlite_session: AsyncSession,
    refresh_token: str,
) -> tuple[str, str, str, str]:
    """Rotate both access and refresh token for an active session."""
    payload = _decode_token(refresh_token, settings.JWT_REFRESH_SECRET_KEY)
    user_id = payload.get("sub")
    session_id = payload.get("sid")
    stored_token = await _get_user_token(sqlite_session, user_id, session_id)

    if not stored_token or payload.get("type") != "refresh":
        raise JWTError("Invalid refresh token")

    if stored_token.refresh_token != fingerprint_token(refresh_token):
        stored_token.is_revoked = True
        stored_token.logged_in = False
        stored_token.updated_at = datetime.now()
        await sqlite_session.commit()
        raise JWTError("Refresh token reuse detected")

    if stored_token.is_revoked or not stored_token.logged_in or stored_token.expiry < datetime.now():
        raise JWTError("Invalid refresh token")

    new_access_token = create_session_access_token(subject=user_id, session_id=str(session_id))
    new_refresh_token = create_session_refresh_token(subject=user_id, session_id=str(session_id))
    stored_token.access_token = fingerprint_token(new_access_token)
    stored_token.refresh_token = fingerprint_token(new_refresh_token)
    stored_token.expiry = datetime.now() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    stored_token.updated_at = datetime.now()
    await sqlite_session.commit()
    return new_access_token, new_refresh_token, stored_token.role, stored_token.user_id


async def verify_access_token(sqlite_session: AsyncSession, token: str) -> TokenData:
    """Validate an access token and map it into application ``TokenData``.

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
    session_id = payload.get("sid")
    stored_token = await _get_user_token(sqlite_session, user_id, session_id)

    if (
        not stored_token
        or payload.get("type") != "access"
        or stored_token.is_revoked
        or not stored_token.logged_in
        or stored_token.expiry < datetime.now()
        or stored_token.access_token != fingerprint_token(token)
    ):
        raise JWTError("Invalid access token")

    return TokenData(
        id=stored_token.user_id,
        session_id=stored_token.session_id,
        jti=payload.get("jti"),
    )


