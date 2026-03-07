"""User Utils module.

This module is part of `app.utils` and contains runtime logic used by the
Traders Family application.
"""

import uuid
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose.exceptions import ExpiredSignatureError, JWSSignatureError, JWTError
from app.db.models.user import TfUser, UserToken
from app.db.session import get_db
from app.core.security import verify_access_token, refresh_access_token, pwd_context, verify_password


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")


def _credential_exception(detail: str = "Invalid email or Password!") -> HTTPException:
    """Build a consistent HTTP 401 exception for authentication failures."""
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
    normalized_email = email.lower().strip()
    query = select(TfUser).where(TfUser.email == normalized_email)
    if not include_deleted:
        query = query.where(TfUser.deleted_at == None)

    result = await session.execute(query)
    return result.scalars().first()


async def authenticate_user(
        email: str,
        password: str,
        session: AsyncSession
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
    active_user = await get_user_by_email(email=email, session=session, include_deleted=False)
    if active_user and verify_password(password, active_user.password):
        return active_user, active_user.role

    deleted_user = await get_user_by_email(email=email, session=session, include_deleted=True)
    if deleted_user and deleted_user.deleted_at is not None:
        raise _credential_exception("Account has been deleted!")

    raise _credential_exception()

async def roles(
        email: str,
        session: AsyncSession
        ):
    """Retrieve role value for a user by email.

    Args:
        email (str): User email used for lookup.
        session (AsyncSession): Async database session.

    Returns:
        str | bool: User role string when found, otherwise `False`.
    """
    user = await get_user_by_email(email=email, session=session, include_deleted=True)
    
    if user:
        return user.role
    else:
        return False
    

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

    #if the access token has expired, attempt to refresh it
    query_refresh_token = select(UserToken).filter_by(access_token=token)
    data_refresh_token = await session.execute(query_refresh_token)
    user_token = data_refresh_token.scalars().first()
    
    if user_token is None:
        raise credentials_exception
    if user_token.is_revoked:
        raise credentials_exception
    
    try:
        # try to verify access token
        token_data = await verify_access_token(session, token)
    except ExpiredSignatureError:
        # verify and refresh the access token using the refresh token
        new_access_token = await refresh_access_token(
            session, user_token.refresh_token
        )
        # re-attemp to verify the new access token
        token_data = await verify_access_token(
            session, new_access_token
        )
    except (JWSSignatureError, JWTError):
        raise credentials_exception
    
    # fetch the user from the database
    user = await get_user_by_id(user_id=token_data.id, session=session)

    if user is None:
        raise credentials_exception
    
    return user


async def user_token(
        session: AsyncSession,
        user_id: str,
        role: str,
        access_token: str,
        refresh_token: str
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
    session_id = str(uuid.uuid4())
    expiry = today + timedelta(days=7)

    query = select(UserToken).where(UserToken.user_id == user_id)
    result = await session.execute(query)
    user = result.scalars().first()

    if user:
        user.session_id = session_id
        user.logged_in = True
        user.expiry = expiry
        user.is_revoked = False
        user.access_token = access_token
        user.refresh_token = refresh_token
        user.updated_at = today
    else:
        session.add(
            UserToken(
                session_id=session_id,
                user_id=user_id,
                page="home",
                logged_in=True,
                role=role,
                expiry=expiry,
                access_token=access_token,
                refresh_token=refresh_token,
                is_revoked=False,
                created_at=today,
                updated_at=today,
            )
        )
    await session.commit()

    data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
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
    """Revoke active token row and set user session as logged out.

    Args:
        session (AsyncSession): Async database session.
        user_id: User identifier associated with token row.
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
    if current_user.role != "superadmin":
        raise PermissionError("Not authorized!")
    
    if current_user.user_id == user_id:
        raise ValueError("You cannot delete your own account!")

    query = await session.execute(
        select(TfUser).where(
            TfUser.user_id == user_id,
            TfUser.deleted_at == None
        )
    )
    user = query.scalar_one_or_none()

    if not user:
        return None

    user.deleted_at = datetime.now()
    user.updated_at = datetime.now()
    await session.commit()

    return user
