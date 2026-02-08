from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


class RegisterBase(BaseModel):
    email: EmailStr
    fullname: str
    role: str
    password: str
    confirm_password: str


class TfUser(BaseModel):
    """
    Docstring for TfUser
    """
    user_id: str
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
    id: Optional[str] = None
