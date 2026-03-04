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

        submitted = st.button("Run Update", type="primary", use_container_width=True, key="update_submit")

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

            st.success(data.get("message", "Update completed successfully."))
        except httpx.RequestError as error:
            st.error(f"Network error while updating data: {error}")
        except Exception as error:
            st.error(f"Unexpected error while updating data: {error}")
