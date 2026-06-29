"""Backend API helpers for Streamlit pages."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import httpx
import streamlit as stlib

from streamlit_app.functions.runtime import (
    apply_auth_payload,
    clear_auth_state,
    consume_auth_bridge_response,
    get_access_token,
    refresh_backend_tokens,
    start_auth_bridge_request,
)


@dataclass(slots=True)
class ApiClientResult:
    """Standard Streamlit-to-FastAPI API client result."""

    ok: bool
    data: Any = None
    message: str = ""
    status_code: int | None = None
    raw: dict[str, Any] | None = None


def _result_from_payload(payload: dict[str, Any], *, status_code: int) -> ApiClientResult:
    """Convert a backend JSON payload into the standard API client shape."""
    explicit_success = payload.get("success")
    ok = bool(explicit_success) if explicit_success is not None else 200 <= status_code < 400
    data = payload.get("data", payload)
    return ApiClientResult(
        ok=ok,
        data=data,
        message=str(payload.get("message") or ""),
        status_code=status_code,
        raw=payload,
    )


def _error_result(message: str, *, status_code: int | None = None) -> ApiClientResult:
    """Build a standard failed API client result."""
    return ApiClientResult(
        ok=False,
        data=None,
        message=message,
        status_code=status_code,
        raw={"success": False, "message": message},
    )


async def fetch_api_result(
    st,
    host,
    uri,
    params=None,
    method: str = "GET",
    json_payload: dict | None = None,
) -> ApiClientResult:
    """Fetch a protected API endpoint and return a stable result wrapper."""
    st_module = st if st is not None else stlib
    try:
        access_token = get_access_token()
        if not access_token:
            return _error_result("Session invalid. Please log in again.", status_code=401)
        url = f"{host}/api/{uri}"
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
            )
            if response.status_code == 401:
                refreshed_payload = await refresh_backend_tokens(host=host)
                if refreshed_payload and refreshed_payload.get("success"):
                    apply_auth_payload(refreshed_payload)
                    headers["Authorization"] = f"Bearer {st_module.session_state.access_token}"
                    response = await client.request(
                        method=method.upper(),
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_payload,
                    )
                else:
                    clear_auth_state()
            response.raise_for_status()
            return _result_from_payload(response.json(), status_code=response.status_code)
    except httpx.HTTPStatusError as http_error:
        logging.error("HTTP error occurred: %s", http_error)
        return _error_result(
            f"HTTP error: {http_error}",
            status_code=http_error.response.status_code,
        )
    except (httpx.ConnectError, httpx.TimeoutException) as conn_error:
        logging.error("Connection error or timeout: %s", conn_error)
        return _error_result(f"Connection error or timeout: {conn_error}")
    except httpx.RequestError as req_error:
        logging.error("Request failed: %s", req_error)
        return _error_result(f"Request failed: {req_error}")
    except Exception as error:
        logging.error("An unexpected error occurred: %s", error)
        return _error_result(f"An unexpected error occurred: {error}")


async def fetch_data(st, host, uri, params=None, method: str = "GET", json_payload: dict | None = None):
    """Fetch data from a protected API endpoint with legacy dict compatibility.

    Deprecated:
        New Streamlit code should call :func:`fetch_api_result` and consume the
        standardized ``ApiClientResult`` shape. This wrapper remains only for
        backward-compatible imports and older tests.
    """
    result = await fetch_api_result(
        st=st,
        host=host,
        uri=uri,
        params=params,
        method=method,
        json_payload=json_payload,
    )
    if result.raw is not None:
        return result.raw
    return {"success": result.ok, "message": result.message}


async def logout(st, host):
    """Handle logout button action and clear client/session state."""
    if st.button("Log Out", type="secondary", width="stretch"):
        start_auth_bridge_request(
            "logout",
            {"access_token": get_access_token()},
        )
        st.rerun()

    pending_request = st.session_state.get("auth_bridge_request")
    if not pending_request or pending_request.get("action") != "logout":
        return

    bridge_response = consume_auth_bridge_response(host, component_key="auth_logout_bridge")
    if bridge_response is None:
        st.caption("Logging out...")
        return

    payload = bridge_response.get("payload", {})
    if bridge_response.get("ok") and payload.get("success"):
        clear_auth_state()
        st.session_state.auth_restore_completed = True
        st.success("Logged out successfully!")
        st.rerun()

    st.error(payload.get("message") or bridge_response.get("error") or "Logout failed")
