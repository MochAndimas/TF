"""Update Data module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import asyncio
import datetime as dt
import httpx
import streamlit as st
from streamlit_app.functions.utils import get_date_range, get_user


DATA_SOURCE_OPTIONS = {
    "Unique Campaign": "unique_campaign",
    "Google Ads (GSheet)": "google_ads",
    "Facebook Ads (GSheet)": "facebook_ads",
    "TikTok Ads (GSheet)": "tiktok_ads",
    "Data Depo": "data_depo",
    "GA4 Daily Users (App/Web)": "ga4_daily_metrics",
}


def _date_presets(today: dt.date) -> dict[str, tuple[dt.date, dt.date]]:
    """Build preset date ranges for update form."""
    yesterday = today - dt.timedelta(days=1)
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start - dt.timedelta(days=1)).replace(day=1)
    last_month_end = this_month_start - dt.timedelta(days=1)

    return {
        "Yesterday": (yesterday, yesterday),
        "Last 7 Days": (today - dt.timedelta(days=7), yesterday),
        "This Month": (this_month_start, today),
        "Last Month": (last_month_start, last_month_end),
        "Custom Range": None,
    }


def _resolve_date_input(
    mode: str,
    preset_key: str,
    presets: dict[str, tuple[dt.date, dt.date]],
) -> tuple[dt.date, dt.date]:
    """Resolve date range from selected mode and preset option."""
    if mode == "auto":
        yesterday = dt.date.today() - dt.timedelta(days=1)
        return yesterday, yesterday

    if preset_key != "Custom Range":
        return presets[preset_key]

    selected_range = st.date_input(
        "Select Date Range",
        value=get_date_range(days=7, period="days"),
        min_value=dt.date(2022, 1, 1),
        max_value=get_date_range(days=2, period="days")[1],
        key="update_date_range",
    )

    if not isinstance(selected_range, tuple) or len(selected_range) != 2:
        raise ValueError("Please select a start and end date.")

    from_date, to_date = selected_range
    if from_date > to_date:
        raise ValueError("Start date cannot be after end date.")
    return from_date, to_date


async def show_update_page(host):
    """Render update form and execute external data synchronization.

    Args:
        host (str): Base URL for backend API requests.

    Returns:
        None: UI side effects only.
    """
    st.markdown("""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)
    st.caption("Trigger manual or automatic synchronization for campaign and deposit datasets.")

    presets = _date_presets(dt.date.today())
    from_date = None
    to_date = None

    with st.container(border=True):
        left_col, right_col = st.columns(2)
        with left_col:
            source_label = st.selectbox(
                "Data Source",
                options=list(DATA_SOURCE_OPTIONS.keys()),
                index=None,
                placeholder="Select a data source",
                key="update_data_source",
            )
            mode = st.radio(
                "Update Mode",
                options=["manual", "auto"],
                horizontal=True,
                key="update_mode",
            )
        with right_col:
            preset_key = st.selectbox(
                "Date Preset",
                options=list(presets.keys()),
                index=0 if mode == "auto" else None,
                disabled=(mode == "auto"),
                placeholder="Select date range preset",
                key="update_period",
            )
            if mode == "auto":
                auto_date = dt.date.today() - dt.timedelta(days=1)
                st.info(f"Auto mode uses date: `{auto_date.isoformat()}`")
            elif preset_key:
                try:
                    from_date, to_date = _resolve_date_input(mode, preset_key, presets)
                except ValueError as error:
                    st.warning(str(error))

        submitted = st.button("Run Update", type="primary", width="stretch", key="update_submit")

    if not submitted:
        return

    if not source_label:
        st.warning("Please select a data source before running update.")
        return

    if mode == "auto":
        from_date, to_date = _resolve_date_input(mode, "Yesterday", presets)
    elif not from_date or not to_date:
        st.warning("Please provide a valid date range.")
        return

    user = get_user(st.session_state._user_id)
    if not user or not getattr(user, "access_token", None):
        st.error("Session is invalid. Please log in again.")
        return

    payload = {
        "types": mode,
        "start_date": from_date.isoformat(),
        "end_date": to_date.isoformat(),
        "data": DATA_SOURCE_OPTIONS[source_label],
    }

    with st.spinner("Updating data..."):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                response = await client.post(
                    f"{host}/api/feature-data/update-external-api",
                    headers={"Authorization": f"Bearer {user.access_token}"},
                    json=payload,
                )

            data = response.json() if response.content else {}
            if response.status_code >= 400:
                message = data.get("detail") or data.get("message") or "Update request failed."
                st.error(message)
                return

            run_id = data.get("run_id")
            if not run_id:
                st.success(data.get("message", "Update request accepted."))
                return

            status_url = f"{host}/api/feature-data/update-external-api/{run_id}"
            st.info(f"Job accepted. Run ID: `{run_id}`")
            status_placeholder = st.empty()
            progress_placeholder = st.empty()

            max_wait_seconds = 300
            poll_interval_seconds = 2
            elapsed = 0
            final_status = None
            final_message = None
            final_error = None

            async with httpx.AsyncClient(timeout=120) as client:
                while elapsed < max_wait_seconds:
                    status_response = await client.get(
                        status_url,
                        headers={"Authorization": f"Bearer {user.access_token}"},
                    )
                    status_data = status_response.json() if status_response.content else {}
                    if status_response.status_code >= 400:
                        message = (
                            status_data.get("detail")
                            or status_data.get("message")
                            or "Failed to fetch update status."
                        )
                        st.error(message)
                        return

                    final_status = status_data.get("status")
                    final_message = status_data.get("message")
                    final_error = status_data.get("error_detail")
                    status_placeholder.info(
                        f"Current status: `{final_status}` | Elapsed: {elapsed}s / {max_wait_seconds}s"
                    )
                    progress_placeholder.progress(min(elapsed / max_wait_seconds, 1.0))

                    if final_status in {"success", "failed"}:
                        break

                    await asyncio.sleep(poll_interval_seconds)
                    elapsed += poll_interval_seconds

            if final_status == "success":
                status_placeholder.empty()
                progress_placeholder.empty()
                st.success(final_message or "Update completed successfully.")
                return

            if final_status == "failed":
                status_placeholder.empty()
                progress_placeholder.empty()
                st.error(final_error or final_message or "Update failed.")
                return

            status_placeholder.warning("Current status: `running`")
            progress_placeholder.progress(1.0)
            st.warning(
                "Update still running. Please check again later using this run_id: "
                f"`{run_id}`"
            )
        except httpx.RequestError as error:
            st.error(f"Network error while updating data: {error}")
        except Exception as error:
            st.error(f"Unexpected error while updating data: {error}")
