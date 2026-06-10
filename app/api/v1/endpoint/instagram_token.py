"""Instagram token exchange, refresh, and storage endpoints."""

from __future__ import annotations

from datetime import datetime
from datetime import date
import logging

import httpx
from decouple import config as env
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.endpoint.common import build_analytics_response, require_roles_dep, validate_date_range
from app.api.v1.functions.fetch_instagram import fetch_instagram_analytics_payload
from app.core.security import decrypt_secret, encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.models.user import TfUser
from app.db.session import get_db
from app.schemas.responses import AnalyticsResponse, ApiResponseV1
from app.utils.rbac import SOCMED_ANALYTICS_ROLES
from app.utils.user_utils import get_current_user, require_roles

router = APIRouter()
logger = logging.getLogger(__name__)

INSTAGRAM_ACCESS_TOKEN_SECRET_KEY = "instagram_access_token"


class InstagramTokenExchangeRequest(BaseModel):
    """Payload for exchanging a short-lived Instagram token."""

    short_lived_token: str


class InstagramTokenSaveRequest(BaseModel):
    """Payload for validating and storing an Instagram access token directly."""

    access_token: str


class InstagramTokenStatusResponse(ApiResponseV1):
    """Expose whether an Instagram access token is already stored."""

    configured: bool
    secret_key: str
    description: str | None = None
    updated_at: datetime | None = None


class InstagramTokenExchangeResponse(ApiResponseV1):
    """Response returned after storing a long-lived Instagram token."""

    token_type: str | None = None
    expires_in: int | None = None
    stored_secret_key: str
    updated_at: datetime


class InstagramTokenSaveResponse(ApiResponseV1):
    """Response returned after validating and storing an Instagram token."""

    stored_secret_key: str
    updated_at: datetime
    instagram_user_id: str | None = None
    username: str | None = None


class InstagramTokenRefreshResponse(ApiResponseV1):
    """Response returned after refreshing the stored Instagram token."""

    token_type: str | None = None
    expires_in: int | None = None
    stored_secret_key: str
    updated_at: datetime


def _meta_app_secret() -> str:
    """Load Meta app secret used by Instagram token exchange."""
    app_secret = env("IG_APP_SECRET", default="", cast=str).strip()
    if not app_secret:
        app_secret = env("META_APP_SECRET", default="", cast=str).strip()
    if not app_secret:
        raise HTTPException(
            status_code=500,
            detail="Instagram app config tidak ditemukan. Set `IG_APP_SECRET` atau fallback `META_APP_SECRET`.",
        )
    return app_secret


def _require_superadmin(current_user: TfUser) -> None:
    """Restrict token management to superadmin users."""
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error


async def _store_instagram_token(
    *,
    session: AsyncSession,
    access_token: str,
    description: str,
) -> datetime:
    """Persist an encrypted Instagram token in managed secret storage."""
    now = datetime.now()
    existing_secret = await session.get(ManagedSecret, INSTAGRAM_ACCESS_TOKEN_SECRET_KEY)
    if existing_secret is None:
        session.add(
            ManagedSecret(
                secret_key=INSTAGRAM_ACCESS_TOKEN_SECRET_KEY,
                secret_value=encrypt_secret(access_token),
                description=description,
                created_at=now,
                updated_at=now,
            )
        )
    else:
        existing_secret.secret_value = encrypt_secret(access_token)
        existing_secret.description = description
        existing_secret.updated_at = now
    await session.commit()
    return now


def _extract_meta_error(response_data) -> str:
    """Return a readable Meta API error message from a response payload."""
    if isinstance(response_data, dict):
        error_payload = response_data.get("error", {})
        if isinstance(error_payload, dict):
            return str(error_payload.get("message") or response_data)
    return str(response_data or "Instagram token request failed.")


@router.get("/api/instagram/token/status", response_model=InstagramTokenStatusResponse)
async def instagram_token_status(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Return whether a reusable long-lived Instagram token is stored."""
    _require_superadmin(current_user)

    stored_secret = await session.get(ManagedSecret, INSTAGRAM_ACCESS_TOKEN_SECRET_KEY)
    return InstagramTokenStatusResponse(
        success=True,
        message="Instagram token status loaded.",
        configured=stored_secret is not None,
        secret_key=INSTAGRAM_ACCESS_TOKEN_SECRET_KEY,
        description=stored_secret.description if stored_secret else None,
        updated_at=stored_secret.updated_at if stored_secret else None,
    )


@router.get("/api/instagram/analytics", response_model=AnalyticsResponse)
async def instagram_analytics(
    start_date: date = Query(...),
    end_date: date = Query(...),
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(require_roles_dep(*SOCMED_ANALYTICS_ROLES)),  # noqa: ARG001
):
    """Generate Instagram analytics payload for dashboard rendering."""
    validate_date_range(start_date, end_date)
    return await build_analytics_response(
        loader=lambda: fetch_instagram_analytics_payload(
            session=session,
            start_date=start_date,
            end_date=end_date,
        ),
        success_message="Instagram analytics generated.",
        logger=logger,
        failure_log_message="Failed to generate Instagram analytics payload",
        failure_detail_message="An internal error occurred while generating Instagram analytics.",
    )


@router.post("/api/instagram/token/exchange", response_model=InstagramTokenExchangeResponse)
async def exchange_instagram_token(
    payload: InstagramTokenExchangeRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Exchange and persist a long-lived Instagram token for backend ETL usage."""
    _require_superadmin(current_user)

    short_lived_token = payload.short_lived_token.strip()
    if not short_lived_token:
        raise HTTPException(status_code=400, detail="Short-lived Instagram token wajib diisi.")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                "https://graph.instagram.com/access_token",
                params={
                    "grant_type": "ig_exchange_token",
                    "client_secret": _meta_app_secret(),
                    "access_token": short_lived_token,
                },
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"Failed to reach Instagram token endpoint: {error}") from error

    try:
        response_data = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="Instagram token endpoint returned invalid JSON.") from error

    if response.status_code >= 400:
        raise HTTPException(
            status_code=400,
            detail=f"Instagram token exchange failed: {_extract_meta_error(response_data)}",
        )

    access_token = str(response_data.get("access_token") or "").strip()
    token_type = str(response_data.get("token_type") or "").strip()
    expires_in = response_data.get("expires_in")
    if not access_token:
        raise HTTPException(
            status_code=502,
            detail="Instagram token exchange succeeded but access_token is missing.",
        )

    updated_at = await _store_instagram_token(
        session=session,
        access_token=access_token,
        description="Instagram long-lived access token",
    )

    return InstagramTokenExchangeResponse(
        success=True,
        message="Instagram long-lived access token berhasil ditukar dan disimpan di database.",
        token_type=token_type or None,
        expires_in=expires_in,
        stored_secret_key=INSTAGRAM_ACCESS_TOKEN_SECRET_KEY,
        updated_at=updated_at,
    )


@router.post("/api/instagram/token/save", response_model=InstagramTokenSaveResponse)
async def save_instagram_token(
    payload: InstagramTokenSaveRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Validate and persist an Instagram access token that is already usable."""
    _require_superadmin(current_user)

    access_token = payload.access_token.strip()
    if not access_token:
        raise HTTPException(status_code=400, detail="Instagram access token wajib diisi.")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                "https://graph.instagram.com/me",
                params={
                    "fields": "id,username,account_type",
                    "access_token": access_token,
                },
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"Failed to validate Instagram token: {error}") from error

    try:
        response_data = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="Instagram token validation returned invalid JSON.") from error

    if response.status_code >= 400:
        raise HTTPException(
            status_code=400,
            detail=f"Instagram token validation failed: {_extract_meta_error(response_data)}",
        )

    updated_at = await _store_instagram_token(
        session=session,
        access_token=access_token,
        description="Instagram access token",
    )

    return InstagramTokenSaveResponse(
        success=True,
        message="Instagram access token berhasil divalidasi dan disimpan di database.",
        stored_secret_key=INSTAGRAM_ACCESS_TOKEN_SECRET_KEY,
        updated_at=updated_at,
        instagram_user_id=str(response_data.get("id") or "") or None,
        username=str(response_data.get("username") or "") or None,
    )


@router.post("/api/instagram/token/refresh", response_model=InstagramTokenRefreshResponse)
async def refresh_instagram_token(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Refresh the stored Instagram long-lived access token."""
    _require_superadmin(current_user)

    stored_secret = await session.get(ManagedSecret, INSTAGRAM_ACCESS_TOKEN_SECRET_KEY)
    if stored_secret is None:
        raise HTTPException(status_code=404, detail="Belum ada Instagram token yang tersimpan.")

    long_lived_token = decrypt_secret(stored_secret.secret_value).strip()
    if not long_lived_token:
        raise HTTPException(status_code=400, detail="Instagram token tersimpan kosong.")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                "https://graph.instagram.com/refresh_access_token",
                params={
                    "grant_type": "ig_refresh_token",
                    "access_token": long_lived_token,
                },
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"Failed to reach Instagram refresh endpoint: {error}") from error

    try:
        response_data = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="Instagram refresh endpoint returned invalid JSON.") from error

    if response.status_code >= 400:
        raise HTTPException(
            status_code=400,
            detail=f"Instagram token refresh failed: {_extract_meta_error(response_data)}",
        )

    access_token = str(response_data.get("access_token") or "").strip()
    token_type = str(response_data.get("token_type") or "").strip()
    expires_in = response_data.get("expires_in")
    if not access_token:
        raise HTTPException(
            status_code=502,
            detail="Instagram token refresh succeeded but access_token is missing.",
        )

    updated_at = await _store_instagram_token(
        session=session,
        access_token=access_token,
        description="Instagram refreshed long-lived access token",
    )

    return InstagramTokenRefreshResponse(
        success=True,
        message="Instagram long-lived access token berhasil di-refresh dan disimpan.",
        token_type=token_type or None,
        expires_in=expires_in,
        stored_secret_key=INSTAGRAM_ACCESS_TOKEN_SECRET_KEY,
        updated_at=updated_at,
    )
