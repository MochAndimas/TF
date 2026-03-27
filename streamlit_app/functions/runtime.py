"""Runtime helpers shared across Streamlit pages."""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.parse import urlparse

import httpx
import streamlit as st
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from streamlit_cookies_controller import CookieController

cookie_controller = CookieController()

streamlit_engine = create_engine(
    st.secrets["db"]["DB_DEV"] if config("ENV") == "development" else st.secrets["db"]["DB"],
    echo=False,
    poolclass=StaticPool,
    pool_pre_ping=True,
)
streamlit_session = sessionmaker(
    bind=streamlit_engine,
    expire_on_commit=False,
    class_=Session,
)


def get_access_token() -> str | None:
    """Read the current bearer token from Streamlit session storage."""
    return st.session_state.get("access_token")


def resolve_backend_base_url(*, prefer_internal: bool = True) -> str:
    """Resolve the backend base URL used by Streamlit pages and auth helpers."""
    if prefer_internal:
        internal_api_host = config("STREAMLIT_API_HOST", default="", cast=str).strip()
        if internal_api_host:
            return internal_api_host.rstrip("/")

    backend_public_url = config("BACKEND_PUBLIC_URL", default="", cast=str).strip()
    if backend_public_url:
        return backend_public_url.rstrip("/")

    try:
        api_secrets = st.secrets.get("api", {})
    except Exception:
        api_secrets = {}

    env_name = config("ENV", default="development", cast=str).lower()
    if env_name == "production":
        configured_host = str(api_secrets.get("HOST", "")).strip()
        if configured_host:
            return configured_host.rstrip("/")
    else:
        configured_dev_host = str(api_secrets.get("DEV_HOST", "")).strip()
        if configured_dev_host:
            return configured_dev_host.rstrip("/")

    backend_host = config("DEV_HOST", default="localhost", cast=str).strip() or "localhost"
    backend_port = config("DEV_PORT", default=8000, cast=int)
    if backend_host.startswith("http://") or backend_host.startswith("https://"):
        return backend_host.rstrip("/")
    return f"http://{backend_host}:{backend_port}"


def refresh_cookie_options(host_url: str) -> dict[str, object]:
    """Build cookie options that behave correctly for localhost and HTTPS hosts."""
    parsed = urlparse(host_url)
    hostname = parsed.hostname
    secure = parsed.scheme == "https"
    options: dict[str, object] = {
        "path": "/",
        "same_site": "strict",
        "secure": secure,
    }
    if hostname and hostname not in {"localhost", "127.0.0.1"}:
        options["domain"] = hostname
    return options


def sync_refresh_cookie(host: str, refresh_token: str | None) -> None:
    """Keep the browser refresh-token cookie aligned with the latest rotation."""
    if not refresh_token or not cookie_controller.get("refresh_token"):
        return

    cookie_controller.set(
        name="refresh_token",
        value=refresh_token,
        expires=datetime.now() + timedelta(days=7),
        **refresh_cookie_options(host),
    )


async def restore_backend_session(host: str, refresh_token: str) -> dict | None:
    """Attempt silent login restoration with a persisted refresh token."""
    if not refresh_token:
        return None

    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            f"{host}/api/token/refresh",
            json={"refresh_token": refresh_token},
        )
    if response.status_code >= 400:
        return None
    return response.json() if response.content else None


async def refresh_backend_tokens(host: str, refresh_token: str) -> dict | None:
    """Request a rotated bearer-token pair from the backend auth service."""
    return await restore_backend_session(host=host, refresh_token=refresh_token)


def get_streamlit():
    """Yield a synchronous SQLAlchemy session for Streamlit components."""
    with streamlit_session() as session:
        try:
            yield session
        finally:
            session.close()
