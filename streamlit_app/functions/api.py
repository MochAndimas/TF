"""Backend API helpers for Streamlit pages."""

from __future__ import annotations

import logging

import httpx
import streamlit as st
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from streamlit_app.functions.runtime import (
    clear_auth_state,
    get_access_token,
    refresh_backend_tokens,
    start_auth_bridge_request,
    consume_auth_bridge_response,
)


async def fetch_data(st, host, uri, params=None, method: str = "GET", json_payload: dict | None = None):
    """Fetch data from a protected API endpoint with token refresh support."""
    try:
        access_token = get_access_token()
        if not access_token:
            return {"message": "Session invalid. Please log in again."}
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
                    st.session_state.access_token = refreshed_payload.get("access_token")
                    headers["Authorization"] = f"Bearer {st.session_state.access_token}"
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
            return response.json()
    except HTTPError as http_error:
        logging.error("HTTP error occurred: %s", http_error)
        return {"message": f"HTTP error: {http_error}"}
    except (ConnectionError, Timeout) as conn_error:
        logging.error("Connection error or timeout: %s", conn_error)
        return {"message": f"Connection error or timeout: {conn_error}"}
    except RequestException as req_error:
        logging.error("Request failed: %s", req_error)
        return {"message": f"Request failed: {req_error}"}
    except Exception as error:
        logging.error("An unexpected error occurred: %s", error)
        return {"message": f"An unexpected error occurred: {error}"}


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
