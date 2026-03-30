"""User and account-related API schemas."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


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
    password: str = Field(min_length=12)
    confirm_password: str = Field(min_length=12)


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


class AccountSummary(BaseModel):
    """Public-safe account payload returned to frontend consumers."""

    user_id: str
    fullname: str
    email: str
    role: str
    created_at: datetime
    updated_at: datetime


class AccountListResponse(BaseModel):
    """Response wrapper for account listing endpoints."""

    success: bool
    message: str
    data: list[AccountSummary]


class AccountUpdateRequest(BaseModel):
    """Payload used to update mutable account profile fields."""

    fullname: str | None = None
    email: EmailStr | None = None
    role: str | None = None


class AccountUpdateResponse(BaseModel):
    """Response wrapper for account update operations."""

    success: bool
    message: str
    data: AccountSummary


class HomeAccountSummary(BaseModel):
    """Compact current-account payload used by the home page."""

    user_id: str
    fullname: str
    email: str
    role: str


class LatestEtlRunSummary(BaseModel):
    """Compact ETL-run payload used by the home page."""

    run_id: str
    pipeline: str
    source: str
    mode: str
    status: str
    message: str | None = None
    error_detail: str | None = None
    window_start: date | None = None
    window_end: date | None = None
    started_at: datetime
    ended_at: datetime | None = None
    triggered_by: str | None = None


class HomeContextData(BaseModel):
    """Aggregated home-page context payload."""

    account: HomeAccountSummary
    latest_run: LatestEtlRunSummary | None = None


class HomeContextResponse(BaseModel):
    """Response wrapper for home-page context endpoint."""

    success: bool
    message: str
    data: HomeContextData


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
    user_id: str


class TokenRefreshRequest(BaseModel):
    """Legacy placeholder schema kept for backward-compatible empty payloads."""


class TokenRefreshResponse(LoginResponse):
    """Schema returned after successful refresh-token rotation."""


class RegisterResponse(BaseModel):
    """Schema for account creation response payload."""

    success: bool
    message: str
    user_id: str


class MessageResponse(BaseModel):
    """Schema for generic response messages with success flag."""

    success: bool
    message: str


class LogoutAllSessionsRequest(BaseModel):
    """Payload for revoking all sessions owned by a user."""

    user_id: str | None = None


class LogoutAllSessionsResponse(MessageResponse):
    """Response emitted after revoking one or more user sessions."""

    revoked_sessions: int


class TokenData(BaseModel):
    """Schema for decoded token data used internally by auth flows.

    Attributes:
        id (Optional[str]): User identifier extracted from token claims.
    """
    id: Optional[str] = None
    session_id: Optional[str] = None
    jti: Optional[str] = None
