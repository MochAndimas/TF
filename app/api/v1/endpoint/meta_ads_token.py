"""Meta Ads token exchange and storage endpoints."""

from __future__ import annotations

from datetime import datetime

import httpx
from decouple import config as env
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.user_utils import get_current_user, require_roles

router = APIRouter()


class MetaAdsTokenExchangeRequest(BaseModel):
    """Payload for exchanging a short-lived Meta user token."""

    short_lived_token: str


class MetaAdsTokenStatusResponse(BaseModel):
    """Expose whether a Meta access token is already stored."""

    configured: bool
    secret_key: str
    description: str | None = None
    updated_at: datetime | None = None


def _meta_app_config() -> tuple[str, str]:
    """Load Meta application credentials required for token exchange calls.

    Returns:
        tuple[str, str]: ``(app_id, app_secret)`` pair read from environment
        configuration.

    Raises:
        HTTPException: When required Meta credentials are not configured.
    """
    app_id = env("META_APP_ID", default="", cast=str).strip()
    app_secret = env("META_APP_SECRET", default="", cast=str).strip()
    if not app_id or not app_secret:
        raise HTTPException(
            status_code=500,
            detail="Meta app config tidak ditemukan. Set `META_APP_ID` dan `META_APP_SECRET`.",
        )
    return app_id, app_secret


def _meta_api_version() -> str:
    """Resolve the Graph API version used for outbound Meta token requests.

    Returns:
        str: Configured API version, defaulting to ``v22.0`` when unset.
    """
    return env("META_API_VERSION", default="v22.0", cast=str).strip() or "v22.0"


@router.get("/api/meta-ads/token/status", response_model=MetaAdsTokenStatusResponse)
async def meta_ads_token_status(
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Return whether a reusable long-lived Meta Ads token is stored.

    Returns:
        MetaAdsTokenStatusResponse: Status payload describing whether the
        managed secret exists and when it was last updated.
    """
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error

    stored_secret = await session.get(ManagedSecret, "meta_ads_access_token")
    return MetaAdsTokenStatusResponse(
        configured=stored_secret is not None,
        secret_key="meta_ads_access_token",
        description=stored_secret.description if stored_secret else None,
        updated_at=stored_secret.updated_at if stored_secret else None,
    )


@router.post("/api/meta-ads/token/exchange")
async def exchange_meta_ads_token(
    payload: MetaAdsTokenExchangeRequest,
    session: AsyncSession = Depends(get_db),
    current_user: TfUser = Depends(get_current_user),
):
    """Exchange and persist a long-lived Meta Ads token for backend ETL usage.

    Args:
        payload (MetaAdsTokenExchangeRequest): Incoming short-lived token
            payload from the admin UI.

    Returns:
        JSONResponse-compatible dict: Success payload confirming that the
        exchanged long-lived token has been stored.
    """
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error

    short_lived_token = payload.short_lived_token.strip()
    if not short_lived_token:
        raise HTTPException(status_code=400, detail="Short-lived token wajib diisi.")

    app_id, app_secret = _meta_app_config()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"https://graph.facebook.com/{_meta_api_version()}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": app_id,
                    "client_secret": app_secret,
                    "fb_exchange_token": short_lived_token,
                },
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"Failed to reach Meta token endpoint: {error}") from error

    try:
        response_data = response.json()
    except ValueError as error:
        raise HTTPException(status_code=502, detail="Meta token endpoint returned invalid JSON.") from error

    if response.status_code >= 400:
        error_payload = response_data.get("error", {}) if isinstance(response_data, dict) else {}
        message = error_payload.get("message") or response_data or "Meta token exchange failed."
        raise HTTPException(status_code=400, detail=f"Meta token exchange failed: {message}")

    access_token = str(response_data.get("access_token") or "").strip()
    token_type = str(response_data.get("token_type") or "").strip()
    expires_in = response_data.get("expires_in")
    if not access_token:
        raise HTTPException(status_code=502, detail="Meta token exchange succeeded but access_token is missing.")

    existing_secret = await session.get(ManagedSecret, "meta_ads_access_token")
    now = datetime.now()
    if existing_secret is None:
        session.add(
            ManagedSecret(
                secret_key="meta_ads_access_token",
                secret_value=encrypt_secret(access_token),
                description="Meta Ads long-lived access token",
                created_at=now,
                updated_at=now,
            )
        )
    else:
        existing_secret.secret_value = encrypt_secret(access_token)
        existing_secret.description = "Meta Ads long-lived access token"
        existing_secret.updated_at = now
    await session.commit()

    return {
        "message": "Meta long-lived access token berhasil ditukar dan disimpan di database.",
        "token_type": token_type or None,
        "expires_in": expires_in,
        "stored_secret_key": "meta_ads_access_token",
        "updated_at": now.isoformat(),
    }
