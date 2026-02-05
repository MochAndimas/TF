from pydantic import BaseModel
from typing import Optional


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
