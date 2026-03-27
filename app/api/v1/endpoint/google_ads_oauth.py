"""Google Ads OAuth callback helpers."""

from __future__ import annotations

import html
import logging
from datetime import datetime, timedelta

from decouple import config as env
from jose import JWTError, jwt
import secrets

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google_auth_oauthlib.flow import Flow

from app.core.config import settings
from app.core.security import encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.models.user import TfUser
from app.db.session import sqlite_async_session
from app.utils.user_utils import get_current_user, require_roles

router = APIRouter()
logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/adwords"]
GOOGLE_ADS_OAUTH_STATE_PURPOSE = "google_ads_oauth"


def _load_client_config() -> dict:
    """Load Google OAuth client configuration from environment variables.

    Returns:
        dict: Client configuration payload shaped for
        ``google_auth_oauthlib.flow.Flow`` construction.
    """
    client_id = env("GOOGLE_ADS_CLIENT_ID", default="", cast=str).strip()
    client_secret = env("GOOGLE_ADS_CLIENT_SECRET", default="", cast=str).strip()
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Google Ads OAuth client config is missing.")

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
    """Resolve the backend callback URL registered for Google OAuth redirects.

    Returns:
        str: Fully qualified callback URL consumed by the OAuth flow.
    """
    configured = env("GOOGLE_ADS_REDIRECT_URI", default="", cast=str).strip()
    if configured:
        return configured
    backend_public_url = env("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return f"{backend_public_url.rstrip('/')}/api/google-ads/oauth/callback"

    backend_host = env("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = env("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return f"{backend_host.rstrip('/')}/api/google-ads/oauth/callback"
    return f"http://{backend_host}:{backend_port}/api/google-ads/oauth/callback"


def _build_flow(state: str | None = None) -> Flow:
    """Build a configured Google OAuth flow object for redirects and callbacks.

    Args:
        state (str | None): Optional signed state token bound to the flow.

    Returns:
        Flow: Configured OAuth flow ready to create authorization URLs or
        exchange callback codes.
    """
    flow = Flow.from_client_config(_load_client_config(), scopes=SCOPES, state=state)
    flow.redirect_uri = _callback_redirect_uri()
    return flow


def _html_page(title: str, body: str) -> HTMLResponse:
    """Render a minimal standalone HTML page for OAuth callback outcomes.

    Args:
        title (str): Browser-page title text.
        body (str): HTML body fragment rendered inside the result card.

    Returns:
        HTMLResponse: Small HTML page used for OAuth success and error states.
    """
    response = HTMLResponse(
        f"""
        <!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>{html.escape(title)}</title>
          <style>
            body {{
              margin: 0;
              font-family: Arial, sans-serif;
              background: #101418;
              color: #f5f7fa;
            }}
            main {{
              max-width: 780px;
              margin: 48px auto;
              padding: 24px;
            }}
            .card {{
              border: 1px solid #2a3139;
              border-radius: 16px;
              background: #171d23;
              padding: 24px;
            }}
            code, pre {{
              background: #0d1117;
              border: 1px solid #2a3139;
              border-radius: 10px;
              padding: 12px;
              overflow-x: auto;
              white-space: pre-wrap;
              word-break: break-all;
            }}
            a {{
              color: #7cc4ff;
            }}
          </style>
        </head>
        <body>
          <main>
            <div class="card">
              <h1>{html.escape(title)}</h1>
              {body}
            </div>
          </main>
        </body>
        </html>
        """
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    return response


def _create_google_ads_oauth_state(user_id: str) -> str:
    """Create a signed short-lived OAuth state token tied to one superadmin.

    Args:
        user_id (str): Superadmin user ID initiating the OAuth flow.

    Returns:
        str: Signed state token validated during the callback exchange.
    """
    payload = {
        "sub": user_id,
        "type": GOOGLE_ADS_OAUTH_STATE_PURPOSE,
        "jti": str(secrets.token_urlsafe(16)),
        "exp": (datetime.now() + timedelta(minutes=10)).timestamp(),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.ALGORITHM)


async def _authorized_oauth_actor_from_state(state: str | None) -> TfUser | None:
    """Resolve the superadmin encoded inside a signed OAuth state token.

    Args:
        state (str | None): Signed state token returned by Google.

    Returns:
        TfUser | None: Authorized superadmin when the state token is valid and
        still maps to an active account, otherwise ``None``.
    """
    if not state:
        return None

    try:
        payload = jwt.decode(state, settings.JWT_SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None

    if payload.get("type") != GOOGLE_ADS_OAUTH_STATE_PURPOSE:
        return None

    user_id = payload.get("sub")
    if not user_id:
        return None

    async with sqlite_async_session() as session:
        user = await session.get(TfUser, user_id)
        if user is None or user.deleted_at is not None:
            return None
        try:
            require_roles(user, "superadmin")
        except PermissionError:
            return None
        return user


def _prepare_google_ads_authorization_url(current_user: TfUser) -> str:
    """Create the Google authorization URL for a validated superadmin session.

    Args:
        current_user (TfUser): Authenticated superadmin starting the OAuth flow.

    Returns:
        str: Browser redirect URL pointing at Google consent screens.
    """
    require_roles(current_user, "superadmin")

    oauth_state = _create_google_ads_oauth_state(current_user.user_id)

    flow = _build_flow(state=oauth_state)
    authorization_url, _state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
    )
    return authorization_url


@router.get("/api/google-ads/oauth/callback", response_class=HTMLResponse)
async def google_ads_oauth_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
):
    """Handle Google OAuth callback and exchange the code for a refresh token.

    Returns:
        HTMLResponse: Result page describing callback success or failure.
    """
    if error:
        return _html_page("Google Ads OAuth Error", f"<p>{html.escape(error)}</p>")

    if not code:
        return _html_page("Google Ads OAuth", "<p>Authorization code was not provided.</p>")

    oauth_actor = await _authorized_oauth_actor_from_state(state)
    if oauth_actor is None:
        return _html_page(
            "Google Ads OAuth Error",
            "<p>OAuth state validation failed.</p>",
        )

    try:
        flow = _build_flow(state=state)
        flow.fetch_token(code=code)
        credentials = flow.credentials
    except Exception:
        logger.exception("Google Ads OAuth token exchange failed")
        return _html_page(
            "Google Ads OAuth Error",
            "<p>OAuth exchange gagal. Cek backend log dan konfigurasi OAuth.</p>",
        )

    refresh_token = credentials.refresh_token
    if not refresh_token:
        return _html_page(
            "Google Ads OAuth",
            (
                "<p>Refresh token tidak dikembalikan.</p>"
                "<p>Revoke app access di akun Google lalu ulangi consent dengan <code>prompt=consent</code>.</p>"
            ),
        )

    async with sqlite_async_session() as session:
        existing_secret = await session.get(ManagedSecret, "google_ads_refresh_token")
        now = datetime.now()
        if existing_secret is None:
            session.add(
                ManagedSecret(
                    secret_key="google_ads_refresh_token",
                    secret_value=encrypt_secret(refresh_token),
                    description="Google Ads OAuth refresh token",
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            existing_secret.secret_value = encrypt_secret(refresh_token)
            existing_secret.updated_at = now
        await session.commit()

    return _html_page(
        "Google Ads OAuth",
        (
            "<p>OAuth berhasil. Refresh token sudah disimpan aman di backend storage.</p>"
            "<p>UI ini sengaja tidak menampilkan token mentah.</p>"
            '<p><a href="http://localhost:5504">Back to dashboard</a></p>'
        ),
    )


@router.get("/api/google-ads/oauth/start")
async def google_ads_oauth_start(
    request: Request,
    current_user: TfUser = Depends(get_current_user),
):
    """Start Google Ads OAuth flow and redirect browser to Google consent."""
    try:
        require_roles(current_user, "superadmin")
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error

    try:
        authorization_url = _prepare_google_ads_authorization_url(current_user)
    except Exception as exc:
        logger.exception("Failed to prepare Google Ads OAuth redirect")
        raise HTTPException(status_code=500, detail="Unable to start Google Ads OAuth flow.") from exc

    return RedirectResponse(url=authorization_url, status_code=302)


@router.post("/api/google-ads/oauth/start")
async def google_ads_oauth_start_payload(
    request: Request,
    current_user: TfUser = Depends(get_current_user),
):
    """Return the Google OAuth consent URL for authenticated frontend clients."""
    try:
        authorization_url = _prepare_google_ads_authorization_url(current_user)
    except PermissionError as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except Exception as exc:
        logger.exception("Failed to prepare Google Ads OAuth authorization URL")
        raise HTTPException(status_code=500, detail="Unable to start Google Ads OAuth flow.") from exc

    return {"authorization_url": authorization_url, "success": True}
