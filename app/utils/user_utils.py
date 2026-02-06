import uuid
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose.exceptions import ExpiredSignatureError, JWSSignatureError, JWTError
from app.db.models.user import TfUser, UserToken
from app.db.session import get_db
from app.core.security import verify_access_token, refresh_access_token


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

async def roles(
        email: str,
        session: AsyncSession = Depends(get_db)
        ):
    """
    Docstring for roles
    
    :param email: Give user email.
    :type email: str
    :param session: sqlite Session maker.
    :type session: AsyncSession
    """
    user_query = select(UserToken).filter_by(email=email)
    user_data = await session.execute(user_query)
    user = user_data.scalars().first()

    if user:
        return user.role
    else:
        return False
    

async def get_user_by_id(
        id: int,
        session: AsyncSession = Depends(get_db)
) -> TfUser | None:
    """
    Docstring for get_user_by_id
    
    :param id: user id of a user account
    :type id: int
    :param session: sqlite session maker
    :type session: AsyncSession
    :return: User data
    :rtype: TfUser | None
    """
    return (
        (await session.execute(
            select(TfUser).filter_by(id=id)
        )).scalars().first()
    )


async def get_current_user(
        token: str = Depends(oauth2_scheme),
        session: AsyncSession = Depends(get_db)
):
    """
    Docstring for get_current_user
    
    :param token: user access token
    :type token: str
    :param session: sqlite session maker
    :type session: AsyncSession
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
    user = await get_user_by_id(id=token_data.id, session=session)

    if user is None:
        raise credentials_exception
    
    return user


async def user_token(
        session: AsyncSession,
        user_id: int,
        role: str,
        access_token: str,
        refresh_token: str
):
    """
    Docstring for user_token
    
    :param session: sqlite session maker.
    :type session: AsyncSession
    :param user_id: user_id of a user account
    :type user_id: int
    :param role: role of a user account
    :type role: str
    :param access_token: access token of a user account
    :type access_token: str
    :param refresh_token: refresh token of a user account
    :type refresh_token: str
    """
    today = datetime.now()
    session_id = str(uuid.uuid4())
    expiry = datetime.now() + timedelta(days=7)

    if session is None:
        async_gen = get_db()
        session = await anext(async_gen)

    async with session.begin():
        query = select(UserToken).filter_by(user_id=user_id)
        user_data = await session.execute(query)
        user = user_data.scalars().first()

        if user:
            user.session_id = session_id
            user.logged_in = True
            user.expiry = expiry
            user.is_revoked = False
            user.access_token = access_token
            user.refresh_token = refresh_token
            user.updated_at = today
        else:
            data = UserToken(
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
                updated_at=today
            )
            session.add(data)
            await session.commit()
            await session.close()

            data= {
                "access_token": access_token,
                "refresh_token": refresh_token
            }

            return data
        


