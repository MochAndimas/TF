import streamlit as st
import requests
import datetime
from streamlit_app.functions.utils import fetch_data
import plotly.io as pio

async def show_logger_page(host):
    """
    This function creates a Streamlit page to display revenue metrics,
    with a loading state during data fetching.

    Args:
        host: The base URL for the data API.
        source: The source of data ('app' or 'web')
    """
    # Form Input and Submission
    st.markdown(f"""<h1 align="center">Update Data</h1>""", unsafe_allow_html=True)

    with st.form("logger_form"):
        update_data = {
            None : "",
            "GSheet Google Ads": "google_ads",
            "Gsheet Facebook Ads": "facebook_ads",
            "Gsheet Tiktok Ads": "tiktok_ads",
            "Data Depo": "data_depo"
        }
        update_data_options = st.selectbox("Data to update", list(update_data.keys()), placeholder="Choose a data to update!", index=None, key="update-data-api")
        submit_button = st.form_submit_button(label="Apply Filters", disabled=False)
    
    # Data Fetching with Loading State
    if submit_button:
        with st.spinner('Fetching data...'):  # Display loading spinner
            try:
                params = {
                        "data": update_data[update_data_options]
                    }
                
                data = await fetch_data(st, host=host, uri=f'feature-data/update-external-api', params=params)
                
                # card_style(st)
                if data:
                    st.info(data["message"])

            except Exception as e:
                st.error(f"Error fetching data: {e}") 