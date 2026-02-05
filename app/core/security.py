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


