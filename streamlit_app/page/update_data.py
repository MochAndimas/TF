import streamlit as st
import datetime
import httpx
from streamlit_app.functions.utils import fetch_data, get_date_range, get_user

async def show_update_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)

    with st.container(border=True):
        update_data = {
            None : "",
            "GSheet Google Ads": "google_ads",
            "Gsheet Facebook Ads": "facebook_ads",
            "Gsheet Tiktok Ads": "tiktok_ads",
            "Data Depo": "data_depo"
        }
        update_data_options = st.selectbox("Data to update", list(update_data.keys()), placeholder="Choose a data to update!", index=None, key="update-data-api")
        today = datetime.datetime.now().date()
        yesterday = today - datetime.timedelta(1)
        last_7days_start = today - datetime.timedelta(days=7)
        last_7days_end  = today - datetime.timedelta(days=1)
        this_month_start = today.replace(day=1)
        last_month_start = (this_month_start - datetime.timedelta(days=1)).replace(day=1)
        last_month_end = this_month_start - datetime.timedelta(days=1)
        preset_date = {
            None : (yesterday, yesterday),
            "Yesterday" : (yesterday, yesterday),
            "Last 7 Days": (last_7days_start, last_7days_end),
            "This Month" : (this_month_start, today),
            "Last Month" : (last_month_start, last_month_end),
            "Custom Range" : "custom_range"
        }
        period_options = st.selectbox("Periods", list(preset_date.keys()), placeholder="Choose a Periods", index=None, key=f"period_new_install")
        if preset_date[period_options] != "custom_range":
            from_date, to_date = preset_date[period_options]
        else : 
            try:
                from_date, to_date = st.date_input(
                    "Select Date Range",
                    value=(get_date_range(days=7, period='days')),
                    min_value=datetime.date(2025, 11, 1),
                    max_value=get_date_range(days=2, period='days')[1],
                    key="new_install_date_range")
            except ValueError:
                st.warning("Please Select A Range of date!")
        submit_button = st.button(label="Update data!", disabled=False, key="update_button")
    
    # Data Fetching with Loading State
    if submit_button:
        user = get_user(st.session_state._user_id)
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(
                        f"{host}/api/feature-data/update-external-api",
                        headers={
                            "Authorization" : f"Bearer {user.access_token}"},
                        json={
                            "types": "manual",
                            "start_date": str(from_date),
                            "end_date": str(to_date),
                            "data": update_data[update_data_options]
                        }
                    )
                    data = response.json()
                
                # card_style(st)
                if data:
                    st.info(data["message"])

            except Exception as e:
                st.error(f"Error fetching data: {e}") 