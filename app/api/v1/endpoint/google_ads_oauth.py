"""Google Ads OAuth callback helpers."""

from __future__ import annotations

import html
from datetime import datetime

from decouple import config as env
import secrets

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google_auth_oauthlib.flow import Flow

from app.core.security import encrypt_secret
from app.db.models.external_api import ManagedSecret
from app.db.session import sqlite_async_session

router = APIRouter()

SCOPES = ["https://www.googleapis.com/auth/adwords"]


def _load_client_config() -> dict:
    """Load OAuth client configuration from environment variables only."""
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
    """Resolve backend callback URL used for Google OAuth."""
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
    """Build configured Google OAuth flow for callback exchange."""
    flow = Flow.from_client_config(_load_client_config(), scopes=SCOPES, state=state)
    flow.redirect_uri = _callback_redirect_uri()
    return flow


def _html_page(title: str, body: str) -> HTMLResponse:
    """Render a minimal HTML result page."""
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


@router.get("/api/google-ads/oauth/callback", response_class=HTMLResponse)
async def google_ads_oauth_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
):
    """Exchange Google OAuth authorization code for a refresh token."""
    if error:
        return _html_page("Google Ads OAuth Error", f"<p>{html.escape(error)}</p>")

    if not code:
        return _html_page("Google Ads OAuth", "<p>Authorization code was not provided.</p>")

    expected_state = request.session.get("google_ads_oauth_state")
    if not state or not expected_state or not secrets.compare_digest(state, expected_state):
        return _html_page("Google Ads OAuth Error", "<p>OAuth state validation failed.</p>")

    try:
        flow = _build_flow(state=state)
        flow.fetch_token(code=code)
        credentials = flow.credentials
    except Exception as exc:
        return _html_page("Google Ads OAuth Error", f"<p>{html.escape(str(exc))}</p>")
    finally:
        request.session.pop("google_ads_oauth_state", None)

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
async def google_ads_oauth_start(request: Request):
    """Start Google Ads OAuth flow and redirect browser to Google consent."""
    try:
        oauth_state = secrets.token_urlsafe(32)
        request.session["google_ads_oauth_state"] = oauth_state

        flow = _build_flow(state=oauth_state)
        authorization_url, _state = flow.authorization_url(
            access_type="offline",
            prompt="consent",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return RedirectResponse(url=authorization_url, status_code=302)
