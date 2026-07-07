"""TikTok OAuth endpoints for securely storing user access credentials."""

from __future__ import annotations

import html
import base64
import hashlib
import logging
import secrets
from datetime import datetime, timedelta
from urllib.parse import urlencode

import httpx
from decouple import config as env
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from jose import JWTError, jwt

from app.core.config import settings
from app.core.security import decrypt_secret, encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.models.user import TfUser
from app.db.session import sqlite_async_session
from app.schemas.responses import ApiResponseV1
from app.utils.user_utils import get_current_user, require_roles

router = APIRouter()
logger = logging.getLogger(__name__)

TIKTOK_OAUTH_STATE_PURPOSE = "tiktok_oauth"
DEFAULT_SCOPES = [
    "user.info.basic",
    "user.info.profile",
    "user.info.stats",
    "video.list",
]
TIKTOK_AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TIKTOK_TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
TIKTOK_USER_INFO_URL = "https://open.tiktokapis.com/v2/user/info/"


class TikTokOAuthStartResponse(ApiResponseV1):
    """JSON response containing the TikTok OAuth consent URL."""

    authorization_url: str
    redirect_uri: str
    scopes: list[str]


def _client_key() -> str:
    value = env("TIKTOK_CLIENT_KEY", default="", cast=str).strip()
    if not value:
        raise HTTPException(status_code=500, detail="TIKTOK_CLIENT_KEY belum dikonfigurasi.")
    return value


def _client_secret() -> str:
    value = env("TIKTOK_CLIENT_SECRET", default="", cast=str).strip()
    if not value:
        raise HTTPException(status_code=500, detail="TIKTOK_CLIENT_SECRET belum dikonfigurasi.")
    return value


def _scopes() -> list[str]:
    configured = env("TIKTOK_SCOPES", default="", cast=str).strip()
    if not configured:
        return DEFAULT_SCOPES
    return [scope.strip() for scope in configured.replace(",", " ").split() if scope.strip()]


def _callback_redirect_uri() -> str:
    configured = env("TIKTOK_REDIRECT_URI", default="", cast=str).strip()
    if configured:
        return configured
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/tiktok/oauth/callback"

    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/tiktok/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/tiktok/oauth/callback"


def _dashboard_return_url() -> str:
    frontend_url = env("FRONTEND_URL", default="", cast=str).strip()
    if frontend_url:
        return frontend_url.split(",", maxsplit=1)[0].strip().rstrip("/")
    return "http://localhost:5504"


def _html_page(title: str, body: str) -> HTMLResponse:
    response = HTMLResponse(
        f"""
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>{html.escape(title)}</title>
          <style>
            body {{ margin: 0; font-family: Arial, sans-serif; background: #101418; color: #f5f7fa; }}
            main {{ max-width: 780px; margin: 48px auto; padding: 24px; }}
            .card {{ border: 1px solid #2a3139; border-radius: 16px; background: #171d23; padding: 24px; }}
            code {{ background: #0d1117; border: 1px solid #2a3139; border-radius: 8px; padding: 3px 6px; }}
            a {{ color: #7cc4ff; }}
          </style>
        </head>
        <body><main><div class="card"><h1>{html.escape(title)}</h1>{body}</div></main></body>
        </html>
        """
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


def _create_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _create_oauth_state(user_id: str, pkce_key: str) -> str:
    payload = {
        "sub": user_id,
        "type": TIKTOK_OAUTH_STATE_PURPOSE,
        "jti": pkce_key,
        "exp": (datetime.now() + timedelta(minutes=10)).timestamp(),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.ALGORITHM)


async def _authorized_oauth_actor_from_state(state: str | None) -> tuple[TfUser, str] | None:
    if not state:
        return None
    try:
        payload = jwt.decode(state, settings.JWT_SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None
    pkce_key = str(payload.get("jti") or "").strip()
    if payload.get("type") != TIKTOK_OAUTH_STATE_PURPOSE or not payload.get("sub") or not pkce_key:
        return None

    async with sqlite_async_session() as session:
        user = await session.get(TfUser, payload["sub"])
        if user is None or user.deleted_at is not None:
            return None
        try:
            require_roles(user, "superadmin")
        except PermissionError:
            return None
        return user, pkce_key


async def _store_pkce_verifier(pkce_key: str, code_verifier: str) -> None:
    await _store_secret(
        f"tiktok_pkce_{pkce_key}",
        code_verifier,
        "Temporary TikTok OAuth PKCE verifier",
    )


async def _take_pkce_verifier(pkce_key: str) -> str | None:
    async with sqlite_async_session() as session:
        stored_secret = await session.get(ManagedSecret, f"tiktok_pkce_{pkce_key}")
        if stored_secret is None:
            return None
        code_verifier = decrypt_secret(stored_secret.secret_value).strip()
        await session.delete(stored_secret)
        await session.commit()
        return code_verifier


async def _prepare_authorization_url(current_user: TfUser) -> tuple[str, str, list[str]]:
    require_roles(current_user, "superadmin")
    redirect_uri = _callback_redirect_uri()
    scopes = _scopes()
    pkce_key = secrets.token_urlsafe(16)
    code_verifier, code_challenge = _create_pkce_pair()
    await _store_pkce_verifier(pkce_key, code_verifier)
    query = urlencode(
        {
            "client_key": _client_key(),
            "scope": ",".join(scopes),
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "state": _create_oauth_state(current_user.user_id, pkce_key),
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{TIKTOK_AUTH_URL}?{query}", redirect_uri, scopes


async def _fetch_tiktok_user(access_token: str) -> dict[str, object]:
    fields = "open_id,union_id,display_name,username,avatar_url,follower_count,following_count,likes_count,video_count"
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            TIKTOK_USER_INFO_URL,
            params={"fields": fields},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    payload = response.json()
    if response.status_code >= 400 or (payload.get("error") or {}).get("code") not in {None, "ok"}:
        detail = (payload.get("error") or {}).get("message") or payload
        raise ValueError(f"TikTok user validation failed: {detail}")
    return (payload.get("data") or {}).get("user") or {}


async def _store_secret(secret_key: str, secret_value: str, description: str) -> None:
    async with sqlite_async_session() as session:
        existing_secret = await session.get(ManagedSecret, secret_key)
        now = datetime.now()
        encrypted_value = encrypt_secret(secret_value)
        if existing_secret is None:
            session.add(
                ManagedSecret(
                    secret_key=secret_key,
                    secret_value=encrypted_value,
                    description=description,
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            existing_secret.secret_value = encrypted_value
            existing_secret.description = description
            existing_secret.updated_at = now
        await session.commit()


@router.get("/api/tiktok/oauth/callback", response_class=HTMLResponse)
async def tiktok_oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
):
    """Exchange a TikTok authorization code and store access credentials."""
    if error:
        detail = error_description or error
        return _html_page("TikTok OAuth Error", f"<p>{html.escape(detail)}</p>")
    if not code:
        return _html_page("TikTok OAuth", "<p>Authorization code was not provided.</p>")
    oauth_actor = await _authorized_oauth_actor_from_state(state)
    if oauth_actor is None:
        return _html_page("TikTok OAuth Error", "<p>OAuth state validation failed.</p>")
    _, pkce_key = oauth_actor
    code_verifier = await _take_pkce_verifier(pkce_key)
    if not code_verifier:
        return _html_page("TikTok OAuth Error", "<p>OAuth PKCE verifier expired or missing.</p>")

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                TIKTOK_TOKEN_URL,
                data={
                    "client_key": _client_key(),
                    "client_secret": _client_secret(),
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": _callback_redirect_uri(),
                    "code_verifier": code_verifier,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        payload = response.json()
        if response.status_code >= 400 or "access_token" not in payload:
            detail = payload.get("error_description") or payload.get("message") or payload
            raise ValueError(f"TikTok token exchange failed: {detail}")

        access_token = str(payload.get("access_token") or "").strip()
        refresh_token = str(payload.get("refresh_token") or "").strip()
        open_id = str(payload.get("open_id") or "").strip()
        if not access_token or not refresh_token:
            raise ValueError("TikTok token exchange succeeded but access or refresh token is missing.")

        user = await _fetch_tiktok_user(access_token)
        display_name = str(user.get("display_name") or user.get("username") or open_id or "TikTok account")
        resolved_open_id = str(user.get("open_id") or open_id).strip()
        description_suffix = f" for {resolved_open_id or display_name}"
        await _store_secret("tiktok_access_token", access_token, f"TikTok access token{description_suffix}")
        await _store_secret("tiktok_refresh_token", refresh_token, f"TikTok refresh token{description_suffix}")
        if resolved_open_id:
            await _store_secret("tiktok_open_id", resolved_open_id, "TikTok authorized open_id")
    except (httpx.HTTPError, ValueError, HTTPException):
        logger.exception("TikTok OAuth exchange failed")
        return _html_page(
            "TikTok OAuth Error",
            "<p>OAuth exchange gagal. Cek backend log, redirect URI, scopes, dan TikTok app credentials.</p>",
        )

    return _html_page(
        "TikTok OAuth",
        (
            f"<p>OAuth berhasil untuk <strong>{html.escape(display_name)}</strong>.</p>"
            "<p>Access token dan refresh token sudah disimpan terenkripsi dan tidak ditampilkan ke UI.</p>"
            f'<p><a href="{html.escape(_dashboard_return_url())}?page=tiktok-token">Back to dashboard</a></p>'
        ),
    )


@router.post("/api/tiktok/oauth/start", response_model=TikTokOAuthStartResponse)
async def tiktok_oauth_start_payload(
    current_user: TfUser = Depends(get_current_user),
):
    """Return the TikTok consent URL for an authenticated superadmin."""
    try:
        authorization_url, redirect_uri, scopes = await _prepare_authorization_url(current_user)
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to prepare TikTok OAuth authorization URL")
        raise HTTPException(status_code=500, detail="Unable to start TikTok OAuth flow.") from exc

    return TikTokOAuthStartResponse(
        success=True,
        message="TikTok OAuth authorization URL generated.",
        authorization_url=authorization_url,
        redirect_uri=redirect_uri,
        scopes=scopes,
    )
