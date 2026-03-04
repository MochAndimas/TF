from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime


class RegisterBase(BaseModel):
    """Schema for account registration payload.

    Attributes:
        email (EmailStr): User email address.
        fullname (str): User full name.
        role (str): User role code.
        password (str): Raw password input.
        confirm_password (str): Password confirmation input.
    """
    email: EmailStr
    fullname: str
    role: str
    password: str
    confirm_password: str


class TfUser(BaseModel):
    """Schema representation of a user account entity.

    Attributes:
        user_id (str): Unique identifier of the user.
        fullname (str): Full name of the user.
        email (str): User email address.
        role (str): Role assigned to the user.
        password (str): Stored hashed password.
        created_at (datetime): Record creation timestamp.
        updated_at (datetime): Last update timestamp.
        deleted_at (datetime): Soft-delete timestamp, if deleted.
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
    """Schema for login response payload.

    Attributes:
        access_token (str): JWT access token.
        token_type (str): Token type string (e.g., `Bearer`).
        success (bool): Login status indicator.
    """
    access_token: str
    token_type: str
    success: bool


class LoginResponse(TokenBase):
    """Schema for login response payload including user role."""

    role: str


class RegisterResponse(BaseModel):
    """Schema for account creation response payload."""

    success: bool
    message: str
    user_id: str


class MessageResponse(BaseModel):
    """Schema for generic response messages with success flag."""

    success: bool
    message: str


class TokenData(BaseModel):
    """Schema for decoded token data used internally by auth flows.

    Attributes:
        id (Optional[str]): User identifier extracted from token claims.
    """
    id: Optional[str] = None
