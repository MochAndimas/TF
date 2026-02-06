from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class TfUser(BaseModel):
    """
    Docstring for TfUser
    """
    user_id: int
    fullname: str
    email: str
    role: str
    password: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime


class TokenBase(BaseModel):
    """
    Docstring for TokenBase
    """
    access_token: str
    token_type: str
    success: bool


class TokenData(BaseModel):
    """
    Docstring for TokenData
    """
    id: Optional[int] = None
