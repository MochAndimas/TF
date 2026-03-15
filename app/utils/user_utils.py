"""User Utils module.

This module is part of `app.utils` and contains runtime logic used by the
Traders Family application.
"""

import uuid
from datetime import datetime, timedelta
from typing import Final
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose.exceptions import ExpiredSignatureError, JWSSignatureError, JWTError
from app.db.models.user import AuthAuditEvent, LoginThrottle, TfUser, UserToken
from app.db.session import get_db
from app.core.security import (
    fingerprint_session_id,
    fingerprint_token,
    verify_access_token,
    pwd_context,
    verify_password,
)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

ALLOWED_ROLES: Final[set[str]] = {"superadmin", "admin", "digital_marketing", "sales"}
ROLE_CREATION_POLICY: Final[dict[str, set[str]]] = {
    "superadmin": ALLOWED_ROLES,
    "admin": {"digital_marketing", "sales"},
}
MAX_FAILED_LOGIN_ATTEMPTS: Final[int] = 5
LOCKOUT_MINUTES: Final[int] = 15


def normalize_email(email: str) -> str:
    """Normalize an email address for case-insensitive auth storage/lookups."""
    return email.lower().strip()


def _credential_exception(detail: str = "Invalid email or Password!") -> HTTPException:
    """Build the standard authentication error used across user helpers.

    Args:
        detail (str): Human-readable error message exposed to the client.

    Returns:
        HTTPException: Preconfigured 401 exception with the bearer-auth header
        required by FastAPI auth flows.
    """
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_user_by_email(
        email: str,
        session: AsyncSession,
        include_deleted: bool = False
) -> TfUser | None:
    """Fetch user by email with optional soft-deleted rows.

    Args:
        email (str): User email used for lookup.
        session (AsyncSession): Async database session.
        include_deleted (bool): Include rows with non-null `deleted_at`.

    Returns:
        TfUser | None: Matching user model or `None`.
    """
    normalized_email = normalize_email(email)
    query = select(TfUser).where(TfUser.email == normalized_email)
    if not include_deleted:
        query = query.where(TfUser.deleted_at == None)

    result = await session.execute(query)
    return result.scalars().first()


async def authenticate_user(
        email: str,
        password: str,
        session: AsyncSession,
        client_ip: str | None = None,
        user_agent: str | None = None,
) -> tuple[TfUser, str]:
    """Validate user credentials and return active user + role.

    Args:
        email (str): Login email value.
        password (str): Raw password provided by user.
        session (AsyncSession): Async database session.

    Returns:
        tuple[TfUser, str]: Authenticated user and role string.

    Raises:
        HTTPException: Raised when credentials are invalid or account is deleted.
    """
    normalized_email = normalize_email(email)
    throttle = await _get_or_create_login_throttle(session=session, email=normalized_email)
    now = datetime.now()
    if throttle.locked_until and throttle.locked_until > now:
        await _record_auth_event(
            session=session,
            email=normalized_email,
            user_id=None,
            event_type="login_locked",
            success=False,
            detail="Temporary lockout active.",
            ip_address=client_ip,
            user_agent=user_agent,
        )
        await session.commit()
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED,
            detail="Account temporarily locked due to repeated failed login attempts.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    active_user = await get_user_by_email(email=normalized_email, session=session, include_deleted=False)
    if active_user and verify_password(password, active_user.password):
        throttle.failed_attempts = 0
        throttle.locked_until = None
        throttle.updated_at = now
        await _record_auth_event(
            session=session,
            email=normalized_email,
            user_id=active_user.user_id,
            event_type="login_success",
            success=True,
            detail="User authenticated successfully.",
            ip_address=client_ip,
            user_agent=user_agent,
        )
        await session.commit()
        return active_user, active_user.role

    deleted_user = await get_user_by_email(email=normalized_email, session=session, include_deleted=True)
    throttle.failed_attempts += 1
    throttle.last_failed_at = now
    throttle.updated_at = now
    if throttle.failed_attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
        throttle.locked_until = now + timedelta(minutes=LOCKOUT_MINUTES)
    await _record_auth_event(
        session=session,
        email=normalized_email,
        user_id=deleted_user.user_id if deleted_user else active_user.user_id if active_user else None,
        event_type="login_failed",
        success=False,
        detail="Invalid credentials supplied.",
        ip_address=client_ip,
        user_agent=user_agent,
    )
    await session.commit()
    if deleted_user and deleted_user.deleted_at is not None:
        raise _credential_exception("Account has been deleted!")

    raise _credential_exception()


async def _get_or_create_login_throttle(
    session: AsyncSession,
    email: str,
) -> LoginThrottle:
    """Return login throttle record, creating one when missing."""
    result = await session.execute(select(LoginThrottle).where(LoginThrottle.email == email))
    throttle = result.scalar_one_or_none()
    if throttle is not None:
        return throttle

    now = datetime.now()
    throttle = LoginThrottle(
        email=email,
        failed_attempts=0,
        locked_until=None,
        last_failed_at=None,
        created_at=now,
        updated_at=now,
    )
    session.add(throttle)
    await session.flush()
    return throttle


async def _record_auth_event(
    session: AsyncSession,
    email: str,
    user_id: str | None,
    event_type: str,
    success: bool,
    detail: str,
    ip_address: str | None,
    user_agent: str | None,
) -> None:
    """Persist one authentication audit event."""
    session.add(
        AuthAuditEvent(
            email=email,
            user_id=user_id,
            event_type=event_type,
            success=success,
            ip_address=ip_address,
            user_agent=user_agent,
            detail=detail,
            created_at=datetime.now(),
        )
    )

async def get_user_by_id(
        user_id: str,
        session: AsyncSession = Depends(get_db)
) -> TfUser | None:
    """Fetch active user record by ID.

    Args:
        user_id (str): User identifier.
        session (AsyncSession): Async database session.

    Returns:
        TfUser | None: Active user object or `None` if not found.
    """
    return (
        (await session.execute(
            select(TfUser).filter_by(user_id=user_id, deleted_at=None)
        )).scalars().first()
    )


async def get_current_user(
        token: str = Depends(oauth2_scheme),
        session: AsyncSession = Depends(get_db)
):
    """Resolve authenticated user from bearer token.

    Args:
        token (str): Access token from OAuth2 bearer dependency.
        session (AsyncSession): Async database session.

    Returns:
        TfUser: Authenticated user model.

    Raises:
        HTTPException: Raised when token is invalid, revoked, or user is missing.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )

    try:
        token_data = await verify_access_token(session, token)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (JWSSignatureError, JWTError):
        raise credentials_exception
    
    # fetch the user from the database
    user = await get_user_by_id(user_id=token_data.id, session=session)

    if user is None:
        raise credentials_exception
    
    return user


def validate_role_assignment(actor_role: str, target_role: str) -> None:
    """Validate whether the authenticated actor may assign the requested role."""
    normalized_actor_role = actor_role.strip().lower()
    normalized_target_role = target_role.strip().lower()

    if normalized_target_role not in ALLOWED_ROLES:
        raise ValueError("Invalid role selected.")

    allowed_roles = ROLE_CREATION_POLICY.get(normalized_actor_role, set())
    if normalized_target_role not in allowed_roles:
        raise PermissionError("Not authorized to assign the requested role.")


def require_roles(current_user: TfUser, *allowed_roles: str) -> None:
    """Enforce that a user belongs to one of the allowed RBAC roles."""
    normalized_role = (current_user.role or "").strip().lower()
    normalized_allowed = {role.strip().lower() for role in allowed_roles}
    if normalized_role not in normalized_allowed:
        raise PermissionError("Not authorized to access this resource.")


async def user_token(
        session: AsyncSession,
        user_id: str,
        role: str,
        access_token: str,
        refresh_token: str,
        session_id: str | None = None,
):
    """Create or update token/session row for a user login.

    Args:
        session (AsyncSession): Async database session.
        user_id (str): User identifier.
        role (str): Role assigned to the user.
        access_token (str): Newly issued access token.
        refresh_token (str): Newly issued refresh token.

    Returns:
        dict[str, str]: Access and refresh token pair.
    """
    today = datetime.now()
    session_id = session_id or str(uuid.uuid4())
    expiry = today + timedelta(days=7)

    query = select(UserToken).where(UserToken.user_id == user_id)
    result = await session.execute(query)
    user = result.scalars().first()

    if user:
        user.session_id = fingerprint_session_id(session_id)
        user.logged_in = True
        user.expiry = expiry
        user.is_revoked = False
        user.access_token = fingerprint_token(access_token)
        user.refresh_token = fingerprint_token(refresh_token)
        user.updated_at = today
    else:
        session.add(
            UserToken(
                session_id=fingerprint_session_id(session_id),
                user_id=user_id,
                page="home",
                logged_in=True,
                role=role,
                expiry=expiry,
                access_token=fingerprint_token(access_token),
                refresh_token=fingerprint_token(refresh_token),
                is_revoked=False,
                created_at=today,
                updated_at=today,
            )
        )
    await session.commit()

    data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "session_id": session_id,
    }

    return data
        

async def create_account(
        session: AsyncSession,
        fullname: str,
        email: str,
        role: str,
        password: str
):
    """Create a new user account record.

    Args:
        session (AsyncSession): Async database session.
        fullname (str): User full name.
        email (str): User email address.
        role (str): Role assigned at creation.
        password (str): Raw password that will be hashed before persisting.

    Returns:
        TfUser | None: Created user object, or `None` when email already exists.
    """
    email = email.lower()
    today = datetime.now()

    query = select(TfUser).filter_by(email=email)
    user_data = await session.execute(query)
    user = user_data.scalar_one_or_none()

    if user:
        return None
        
    new_account = TfUser(
        user_id=str(uuid.uuid4()),
        fullname=fullname,
        email=email,
        role=role,
        password=pwd_context.hash(password),
        created_at=today,
        updated_at=today
    )

    session.add(new_account)
    await session.commit()

    return new_account
        

async def logout(
        session: AsyncSession,
        user_id
):
    """Revoke a user's active token row and mark the session as logged out.

    Args:
        session (AsyncSession): Async database session.
        user_id: User identifier associated with token row.

    Returns:
        bool: `True` when a token row was found and updated, otherwise `False`.
    """
    today = datetime.now()
    query = select(UserToken).filter_by(user_id=user_id)
    user_data = await session.execute(query)
    user = user_data.scalars().first()
    if user is None:
        return False

    user.logged_in = False
    user.is_revoked = True
    user.expiry = today
    user.updated_at = today

    await session.commit()
    return True


async def delete_account(
        session: AsyncSession,
        user_id: str,
        current_user: TfUser
):
    """Soft-delete a user account after authorization checks.

    Args:
        session (AsyncSession): Async database session.
        user_id (str): Target user identifier to delete.
        current_user (TfUser): Authenticated actor performing the operation.

    Returns:
        TfUser | None: Updated user object when deleted, otherwise `None`.

    Raises:
        PermissionError: Raised when current user is not `superadmin`.
        ValueError: Raised when attempting to delete own account.
    """
    require_roles(current_user, "admin", "superadmin")
    if current_user.role == "admin":
        query = await session.execute(
            select(TfUser).where(
                TfUser.user_id == user_id,
                TfUser.deleted_at == None,
            )
        )
        target_user = query.scalar_one_or_none()
        if target_user is None:
            return None
        if target_user.role not in {"digital_marketing", "sales"}:
            raise PermissionError("Admins may only delete non-admin accounts.")
    else:
        target_user = None

    if current_user.role not in {"admin", "superadmin"}:
        raise PermissionError("Not authorized!")
    
    if current_user.user_id == user_id:
        raise ValueError("You cannot delete your own account!")

    if target_user is None:
        query = await session.execute(
            select(TfUser).where(
                TfUser.user_id == user_id,
                TfUser.deleted_at == None
            )
        )
        user = query.scalar_one_or_none()
    else:
        user = target_user

    if not user:
        return None

    user.deleted_at = datetime.now()
    user.updated_at = datetime.now()
    await session.commit()

    return user
