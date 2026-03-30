"""Security and token utilities for authentication flows."""

from __future__ import annotations

import base64
import hashlib
import re
import uuid
from datetime import datetime, timedelta
from typing import Any

from jose import JWTError, jwt
from cryptography.fernet import Fernet
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.user import AuthAuditEvent, TfUser, UserToken
from app.schemas.user import TokenData

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
PASSWORD_MIN_LENGTH = 12
PASSWORD_SPECIAL_PATTERN = re.compile(r"[^A-Za-z0-9]")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify plain-text password against stored hash.

    Args:
        plain_password (str): Raw password submitted by client.
        hashed_password (str): Hashed password stored in database.

    Returns:
        bool: ``True`` when the password is valid, otherwise ``False``.
    """
    return pwd_context.verify(plain_password, hashed_password)


def validate_password_policy(password: str) -> None:
    """Enforce the shared password policy used by registration and bootstrap."""
    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters long.")
    if not re.search(r"[A-Z]", password):
        raise ValueError("Password must contain at least one uppercase letter.")
    if not re.search(r"[a-z]", password):
        raise ValueError("Password must contain at least one lowercase letter.")
    if not re.search(r"\d", password):
        raise ValueError("Password must contain at least one number.")
    if not PASSWORD_SPECIAL_PATTERN.search(password):
        raise ValueError("Password must contain at least one special character.")
    if len(set(password)) < 6:
        raise ValueError("Password is too weak. Use more unique characters.")


def create_session_access_token(
    subject: str | Any,
    session_id: str,
    expires_delta: timedelta | None = None,
) -> str:
    """Create a signed short-lived access token bound to one stored session.

    Args:
        subject (str | Any): Principal identifier embedded into the token.
        session_id (str): Persisted session identifier linked to the login row.
        expires_delta (timedelta | None): Optional override for token lifetime.

    Returns:
        str: Encoded JWT access token carrying standard claims and session
        binding metadata.
    """
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
    """Create a signed long-lived refresh token tied to one stored session.

    Args:
        subject (str | Any): Principal identifier embedded into the token.
        session_id (str): Persisted session identifier linked to the login row.
        expires_delta (timedelta | None): Optional override for refresh-token
            lifetime.

    Returns:
        str: Encoded JWT refresh token used for session rotation.
    """
    expiry = datetime.now() + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
    return _encode_token(
        subject=subject,
        session_id=session_id,
        token_type="refresh",
        expiry=expiry,
        secret=settings.JWT_REFRESH_SECRET_KEY,
    )


def fingerprint_token(token: str) -> str:
    """Hash a token into a non-reversible fingerprint for safe persistence.

    Args:
        token (str): Raw token string that must not be stored directly.

    Returns:
        str: SHA-256 digest suitable for comparison and audit storage.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def fingerprint_session_id(session_id: str) -> str:
    """Hash a session identifier using the same strategy as token fingerprints.

    Args:
        session_id (str): Raw session identifier value.

    Returns:
        str: Stable digest used during lookups and gradual data migrations.
    """
    return fingerprint_token(session_id)


def _session_matches(stored_session_id: str | None, session_id: str | None) -> bool:
    """Compare stored and presented session IDs across raw and hashed formats.

    Returns:
        bool: ``True`` when the stored value matches either the raw session ID
        or its fingerprint, supporting backward-compatible lookups during
        migration.
    """
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
    """Derive a Fernet-compatible key from configured application secrets.

    Returns:
        bytes: URL-safe base64-encoded key material suitable for Fernet
        encryption helpers.
    """
    seed = (settings.APP_ENCRYPTION_KEY or settings.JWT_SECRET_KEY).encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(seed).digest())


def encrypt_secret(value: str) -> str:
    """Encrypt one sensitive value before it is persisted to application data.

    Args:
        value (str): Plaintext secret such as an external API access token.

    Returns:
        str: Encrypted string that can be stored at rest in the database.
    """
    return Fernet(_fernet_key_material()).encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str) -> str:
    """Decrypt one previously stored secret back into plaintext form.

    Args:
        value (str): Encrypted string produced by :func:`encrypt_secret`.

    Returns:
        str: Plaintext secret value ready for outbound API use.
    """
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


async def _resolve_user_email(
    sqlite_session: AsyncSession,
    user_id: str | None,
) -> str | None:
    """Best-effort resolve of user email for audit logging."""
    if not user_id:
        return None
    user = await sqlite_session.get(TfUser, user_id)
    if user is None:
        return None
    return user.email


async def _record_security_event(
    sqlite_session: AsyncSession,
    *,
    event_type: str,
    success: bool,
    user_id: str | None,
    email: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    detail: str | None = None,
) -> None:
    """Persist one auth/security event inside the current transaction."""
    sqlite_session.add(
        AuthAuditEvent(
            email=email or await _resolve_user_email(sqlite_session, user_id) or "unknown",
            user_id=user_id,
            event_type=event_type,
            success=success,
            ip_address=ip_address,
            user_agent=user_agent,
            detail=detail,
            created_at=datetime.now(),
        )
    )


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
    if not user_id and not session_id:
        return None

    if session_id and not user_id:
        token_result = await sqlite_session.execute(
            select(UserToken).where(
                UserToken.session_id.in_([session_id, fingerprint_session_id(session_id)])
            )
        )
        return token_result.scalars().first()

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
    *,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> tuple[str, str, str, str, str | None]:
    """Rotate session-bound bearer tokens after validating a refresh token.

    Args:
        sqlite_session (AsyncSession): Database session used to validate and
            update persisted auth-session state.
        refresh_token (str): Incoming refresh token presented by the client.

    Returns:
        tuple[str, str, str, TfUser]: New access token, new refresh token,
        resolved session ID, and the authenticated user bound to that session.
    """
    payload = _decode_token(refresh_token, settings.JWT_REFRESH_SECRET_KEY)
    user_id = payload.get("sub")
    session_id = payload.get("sid")
    stored_token = await _get_user_token(sqlite_session, user_id, session_id)

    if not stored_token or payload.get("type") != "refresh":
        await _record_security_event(
            sqlite_session,
            event_type="refresh_failed",
            success=False,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            detail="Refresh token does not match an active session.",
        )
        await sqlite_session.commit()
        raise JWTError("Invalid refresh token")

    if stored_token.refresh_token != fingerprint_token(refresh_token):
        stored_token.is_revoked = True
        stored_token.logged_in = False
        stored_token.updated_at = datetime.now()
        stored_token.last_seen_ip = ip_address
        stored_token.last_seen_user_agent = user_agent
        await _record_security_event(
            sqlite_session,
            event_type="refresh_reuse_detected",
            success=False,
            user_id=stored_token.user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            detail="Refresh token fingerprint mismatch detected.",
        )
        await sqlite_session.commit()
        raise JWTError("Refresh token reuse detected")

    if stored_token.is_revoked or not stored_token.logged_in or stored_token.expiry < datetime.now():
        await _record_security_event(
            sqlite_session,
            event_type="refresh_failed",
            success=False,
            user_id=stored_token.user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            detail="Refresh token belongs to a revoked or expired session.",
        )
        await sqlite_session.commit()
        raise JWTError("Invalid refresh token")

    new_access_token = create_session_access_token(subject=user_id, session_id=str(session_id))
    new_refresh_token = create_session_refresh_token(subject=user_id, session_id=str(session_id))
    stored_token.access_token = fingerprint_token(new_access_token)
    stored_token.refresh_token = fingerprint_token(new_refresh_token)
    stored_token.expiry = datetime.now() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    stored_token.updated_at = datetime.now()
    stored_token.last_seen_ip = ip_address
    stored_token.last_seen_user_agent = user_agent
    stored_token.last_rotated_at = datetime.now()
    await _record_security_event(
        sqlite_session,
        event_type="refresh_success",
        success=True,
        user_id=stored_token.user_id,
        ip_address=ip_address,
        user_agent=user_agent,
        detail="Refresh token rotated successfully.",
    )
    await sqlite_session.commit()
    return new_access_token, new_refresh_token, stored_token.role, stored_token.user_id, session_id


async def rotate_session_handle(
    sqlite_session: AsyncSession,
    session_id: str,
    *,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> tuple[str, str, str, str]:
    """Issue a fresh access token for an active persisted session handle.

    Args:
        sqlite_session (AsyncSession): Database session used to validate and
            update persisted auth-session state.
        session_id (str): Opaque session handle presented by a trusted caller
            or recovered from an HttpOnly cookie.

    Returns:
        tuple[str, str, str, str]: New access token, role, user ID, and the
        stable session identifier bound to the persisted login row.
    """
    if not session_id:
        raise JWTError("Missing session handle")

    stored_token = await _get_user_token(sqlite_session, user_id=None, session_id=session_id)

    if not stored_token:
        await _record_security_event(
            sqlite_session,
            event_type="refresh_invalid_cookie",
            success=False,
            user_id=None,
            ip_address=ip_address,
            user_agent=user_agent,
            detail="Session cookie does not match any active session.",
        )
        await sqlite_session.commit()
        raise JWTError("Invalid session handle")

    if stored_token.is_revoked or not stored_token.logged_in or stored_token.expiry < datetime.now():
        await _record_security_event(
            sqlite_session,
            event_type="refresh_failed",
            success=False,
            user_id=stored_token.user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            detail="Session cookie belongs to a revoked or expired session.",
        )
        await sqlite_session.commit()
        raise JWTError("Invalid session handle")

    new_access_token = create_session_access_token(
        subject=stored_token.user_id,
        session_id=session_id,
    )
    new_refresh_token = create_session_refresh_token(
        subject=stored_token.user_id,
        session_id=session_id,
    )
    stored_token.access_token = fingerprint_token(new_access_token)
    stored_token.refresh_token = fingerprint_token(new_refresh_token)
    stored_token.expiry = datetime.now() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    stored_token.updated_at = datetime.now()
    stored_token.last_seen_ip = ip_address
    stored_token.last_seen_user_agent = user_agent
    stored_token.last_rotated_at = datetime.now()
    await _record_security_event(
        sqlite_session,
        event_type="refresh_success",
        success=True,
        user_id=stored_token.user_id,
        ip_address=ip_address,
        user_agent=user_agent,
        detail="Session cookie rotated successfully.",
    )
    await sqlite_session.commit()
    return new_access_token, stored_token.role, stored_token.user_id, session_id


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
        session_id=session_id,
        jti=payload.get("jti"),
    )
