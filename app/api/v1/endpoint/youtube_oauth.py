"""YouTube OAuth endpoints for securely storing a channel refresh token."""

from __future__ import annotations

import html
import logging
import secrets
from datetime import datetime, timedelta

import httpx
from decouple import config as env
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from google.auth.exceptions import GoogleAuthError
from google_auth_oauthlib.flow import Flow
from jose import JWTError, jwt
from requests import RequestException

from app.core.config import settings
from app.core.security import encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.models.user import TfUser
from app.db.session import sqlite_async_session
from app.schemas.responses import ApiResponseV1
from app.utils.user_utils import get_current_user, require_roles

router = APIRouter()
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]
YOUTUBE_OAUTH_STATE_PURPOSE = "youtube_oauth"
YOUTUBE_OAUTH_FLOW_ERRORS = (
    GoogleAuthError,
    RequestException,
    httpx.HTTPError,
    ValueError,
    HTTPException,
)


class YouTubeOAuthStartResponse(ApiResponseV1):
    """JSON response containing the YouTube OAuth consent URL."""

    authorization_url: str


def _load_client_config() -> dict:
    """Build a Google OAuth web-client configuration from environment values."""
    client_id = env("YOUTUBE_CLIENT_ID", default="", cast=str).strip()
    client_secret = env("YOUTUBE_CLIENT_SECRET", default="", cast=str).strip()
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="YouTube OAuth client config is missing.")

    return {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [],
        }
    }


def _callback_redirect_uri() -> str:
    """Resolve the backend callback URL registered in Google Cloud."""
    configured = env("YOUTUBE_REDIRECT_URI", default="", cast=str).strip()
    if configured:
        return configured
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/youtube/oauth/callback"

    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/youtube/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/youtube/oauth/callback"


def _dashboard_return_url() -> str:
    """Resolve the dashboard URL used by OAuth completion pages."""
    frontend_url = env("FRONTEND_URL", default="", cast=str).strip()
    if frontend_url:
        return frontend_url.split(",", maxsplit=1)[0].strip().rstrip("/")
    return "http://localhost:5504"


def _build_flow(state: str | None = None) -> Flow:
    """Build a configured YouTube OAuth flow."""
    flow = Flow.from_client_config(_load_client_config(), scopes=SCOPES, state=state)
    flow.redirect_uri = _callback_redirect_uri()
    return flow


def _html_page(title: str, body: str) -> HTMLResponse:
    """Render a minimal no-store OAuth result page."""
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


def _create_oauth_state(user_id: str) -> str:
    """Create a signed, short-lived state token bound to one superadmin."""
    payload = {
        "sub": user_id,
        "type": YOUTUBE_OAUTH_STATE_PURPOSE,
        "jti": secrets.token_urlsafe(16),
        "exp": (datetime.now() + timedelta(minutes=10)).timestamp(),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.ALGORITHM)


async def _authorized_oauth_actor_from_state(state: str | None) -> TfUser | None:
    """Resolve the active superadmin encoded in an OAuth state token."""
    if not state:
        return None
    try:
        payload = jwt.decode(state, settings.JWT_SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None
    if payload.get("type") != YOUTUBE_OAUTH_STATE_PURPOSE or not payload.get("sub"):
        return None

    async with sqlite_async_session() as session:
        user = await session.get(TfUser, payload["sub"])
        if user is None or user.deleted_at is not None:
            return None
        try:
            require_roles(user, "superadmin")
        except PermissionError:
            return None
        return user


def _prepare_authorization_url(current_user: TfUser) -> str:
    """Create a YouTube authorization URL for an authenticated superadmin."""
    require_roles(current_user, "superadmin")
    flow = _build_flow(state=_create_oauth_state(current_user.user_id))
    authorization_url, _state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )
    return authorization_url


async def _authorized_channel(access_token: str) -> tuple[str, str]:
    """Validate that OAuth resolved to the configured YouTube channel."""
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            "https://www.googleapis.com/youtube/v3/channels",
            params={"part": "id,snippet", "mine": "true"},
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if response.status_code >= 400:
        payload = response.json()
        detail = (payload.get("error") or {}).get("message") or "unknown error"
        raise ValueError(f"Unable to validate the YouTube channel: {detail}")

    items = response.json().get("items") or []
    if not items:
        raise ValueError("OAuth account did not return an accessible YouTube channel.")

    expected_channel_id = env("YOUTUBE_CHANNEL_ID", default="", cast=str).strip()
    matched = next(
        (item for item in items if not expected_channel_id or item.get("id") == expected_channel_id),
        None,
    )
    if matched is None:
        raise ValueError("OAuth selected a different channel than YOUTUBE_CHANNEL_ID.")
    return matched.get("id", ""), (matched.get("snippet") or {}).get("title", "YouTube channel")


@router.get("/api/youtube/oauth/callback", response_class=HTMLResponse)
async def youtube_oauth_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
):
    """Exchange a Google authorization code and store the refresh token."""
    if error:
        return _html_page("YouTube OAuth Error", f"<p>{html.escape(error)}</p>")
    if not code:
        return _html_page("YouTube OAuth", "<p>Authorization code was not provided.</p>")
    if await _authorized_oauth_actor_from_state(state) is None:
        return _html_page("YouTube OAuth Error", "<p>OAuth state validation failed.</p>")

    try:
        flow = _build_flow(state=state)
        flow.fetch_token(code=code)
        credentials = flow.credentials
        channel_id, channel_title = await _authorized_channel(credentials.token)
    except YOUTUBE_OAUTH_FLOW_ERRORS:
        logger.exception("YouTube OAuth exchange or channel validation failed")
        return _html_page(
            "YouTube OAuth Error",
            "<p>OAuth exchange gagal. Cek backend log, channel yang dipilih, dan konfigurasi OAuth.</p>",
        )

    refresh_token = credentials.refresh_token
    if not refresh_token:
        return _html_page(
            "YouTube OAuth",
            "<p>Refresh token tidak dikembalikan. Revoke app access lalu ulangi consent.</p>",
        )

    async with sqlite_async_session() as session:
        existing_secret = await session.get(ManagedSecret, "youtube_refresh_token")
        now = datetime.now()
        if existing_secret is None:
            session.add(
                ManagedSecret(
                    secret_key="youtube_refresh_token",
                    secret_value=encrypt_secret(refresh_token),
                    description=f"YouTube OAuth refresh token for {channel_id}",
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            existing_secret.secret_value = encrypt_secret(refresh_token)
            existing_secret.description = f"YouTube OAuth refresh token for {channel_id}"
            existing_secret.updated_at = now
        await session.commit()

    return _html_page(
        "YouTube OAuth",
        (
            f"<p>OAuth berhasil untuk <strong>{html.escape(channel_title)}</strong>.</p>"
            "<p>Refresh token sudah disimpan terenkripsi dan tidak ditampilkan ke UI.</p>"
            f'<p><a href="{html.escape(_dashboard_return_url())}">Back to dashboard</a></p>'
        ),
    )


@router.post("/api/youtube/oauth/start", response_model=YouTubeOAuthStartResponse)
async def youtube_oauth_start_payload(
    current_user: TfUser = Depends(get_current_user),
):
    """Return the Google consent URL for an authenticated superadmin."""
    try:
        authorization_url = _prepare_authorization_url(current_user)
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except YOUTUBE_OAUTH_FLOW_ERRORS as exc:
        logger.exception("Failed to prepare YouTube OAuth authorization URL")
        raise HTTPException(status_code=500, detail="Unable to start YouTube OAuth flow.") from exc

    return YouTubeOAuthStartResponse(
        success=True,
        message="YouTube OAuth authorization URL generated.",
        authorization_url=authorization_url,
    )
