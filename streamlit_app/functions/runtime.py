"""Runtime helpers shared across Streamlit pages."""

from __future__ import annotations

import uuid

import httpx
import streamlit as st
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from streamlit_app.components.auth_bridge import auth_bridge

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


def apply_auth_payload(payload: dict) -> None:
    """Project a successful auth payload into Streamlit session state."""
    st.session_state.role = payload.get("role")
    st.session_state.logged_in = True
    st.session_state.page = st.session_state.get("page", "home")
    st.session_state._user_id = payload.get("user_id")
    st.session_state.access_token = payload.get("access_token")


def clear_auth_state() -> None:
    """Reset local Streamlit auth/session markers."""
    st.session_state.logged_in = False
    st.session_state.page = "home"
    st.session_state.role = None
    st.session_state._user_id = None
    st.session_state.access_token = None
    st.session_state.auth_bridge_request = None


def start_auth_bridge_request(action: str, payload: dict | None = None) -> str:
    """Queue one browser-side auth request handled by the custom component."""
    request_id = str(uuid.uuid4())
    st.session_state.auth_bridge_request = {
        "id": request_id,
        "action": action,
        "payload": payload or {},
    }
    return request_id


def consume_auth_bridge_response(host: str, *, component_key: str) -> dict | None:
    """Render the browser auth bridge and return a settled response, if any."""
    pending_request = st.session_state.get("auth_bridge_request")
    if not pending_request:
        return None

    response = auth_bridge(
        action=pending_request["action"],
        host=host,
        request_id=pending_request["id"],
        payload=pending_request.get("payload", {}),
        key=component_key,
    )
    if not response:
        return None

    if response.get("request_id") != pending_request["id"]:
        return None

    st.session_state.auth_bridge_request = None
    return response


async def refresh_backend_tokens(host: str) -> dict | None:
    """Request a new access token using the HttpOnly persisted session cookie."""
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            f"{host}/api/token/refresh",
            json={},
        )
    if response.status_code >= 400:
        return None
    return response.json() if response.content else None


def get_streamlit():
    """Yield a synchronous SQLAlchemy session for Streamlit components."""
    with streamlit_session() as session:
        try:
            yield session
        finally:
            session.close()
