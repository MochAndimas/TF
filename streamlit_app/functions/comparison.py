"""Keep dashboard requests and cached responses tied to the selected period."""
import streamlit as st


def select_comparison(period):
    st.session_state["analytics_comparison"] = {
        "This Month": "month_to_date", "Last Month": "previous_month",
    }.get(period, "previous_period")


def comparison_cache_key():
    return ("comparison-v2", st.session_state.get("analytics_comparison", "previous_period"))
