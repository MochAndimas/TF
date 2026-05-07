"""FastAPI auth dependencies for resolving the current user."""

from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose.exceptions import ExpiredSignatureError, JWSSignatureError, JWTError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import verify_access_token
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.user import TokenData

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")


async def get_user_by_id(
    user_id: str,
    session: AsyncSession = Depends(get_db),
) -> TfUser | None:
    """Fetch active user record by ID."""
    return (
        (
            await session.execute(
                select(TfUser).filter_by(user_id=user_id, deleted_at=None)
            )
        )
        .scalars()
        .first()
    )


async def get_current_token_data(
    token: str = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_db),
) -> TokenData:
    """Resolve validated token metadata for the active bearer session."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        return await verify_access_token(session, token)
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (JWSSignatureError, JWTError):
        raise credentials_exception


async def get_current_user(
    token_data: TokenData = Depends(get_current_token_data),
    session: AsyncSession = Depends(get_db),
) -> TfUser:
    """Resolve authenticated user from bearer token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    user = await get_user_by_id(user_id=token_data.id, session=session)
    if user is None:
        raise credentials_exception
    return user
