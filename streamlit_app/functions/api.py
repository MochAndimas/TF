"""Backend API helpers for Streamlit pages."""

from __future__ import annotations

import logging

import httpx
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from streamlit_app.functions.runtime import (
    cookie_controller,
    get_access_token,
    refresh_backend_tokens,
    sync_refresh_cookie,
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
            if response.status_code == 401 and st.session_state.get("refresh_token"):
                refreshed_payload = await refresh_backend_tokens(
                    host=host,
                    refresh_token=st.session_state["refresh_token"],
                )
                if refreshed_payload and refreshed_payload.get("success"):
                    st.session_state.access_token = refreshed_payload.get("access_token")
                    st.session_state.refresh_token = refreshed_payload.get("refresh_token")
                    st.session_state.session_id = refreshed_payload.get("session_id", st.session_state.get("session_id"))
                    sync_refresh_cookie(host, st.session_state.refresh_token)
                    headers["Authorization"] = f"Bearer {st.session_state.access_token}"
                    response = await client.request(
                        method=method.upper(),
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_payload,
                    )
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
        with st.spinner("Logging out..."):
            try:
                access_token = get_access_token()
                if not access_token:
                    st.error("Session invalid. Please log in again.")
                    return
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(
                        f"{host}/api/logout",
                        headers={"Content-Type": "application/json", "Authorization": f"Bearer {access_token}"},
                    )
                    response.raise_for_status()
                    data = response.json()

                if data.get("success"):
                    cookie_controller.set("refresh_token", "", max_age=0)
                    del st.session_state.logged_in
                    del st.session_state.page
                    del st.session_state._user_id
                    del st.session_state.role
                    if "access_token" in st.session_state:
                        del st.session_state.access_token
                    if "refresh_token" in st.session_state:
                        del st.session_state.refresh_token
                    if "session_id" in st.session_state:
                        del st.session_state.session_id

                    st.success("Logged out successfully!")
                    st.rerun()
                else:
                    st.error(data.get("message", "Logout failed"))
            except RequestException as error:
                st.error(f"An error occurred during logout: {error}. Please try again later.")
