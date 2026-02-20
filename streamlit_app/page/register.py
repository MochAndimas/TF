import httpx
import asyncio
import streamlit as st
from datetime import datetime, timedelta
from app.db.models.user import TfUser
from streamlit_app.functions.utils import get_accounts, add_account_modal
from streamlit_app.functions.utils import get_user, edit_account_modal


async def create_account(host):
    """
    Docstring for create_account
    """
    st.set_page_config(layout="wide")
    users = get_accounts()
    token = get_user(st.session_state._user_id)
    col1, col99, col98, col2 = st.columns([4,2,2,4], gap="xxlarge", vertical_alignment="center")
    with col1:
        st.title("Accounts")
    with col2:
        if st.button("Create Account", type="primary"):
            add_account_modal(host, token)

    # -------- STYLE --------
    st.markdown("""
    <style>
    .table-divider {
        border-bottom: 1px solid #1F2937;
        margin-top: 12px;
        margin-bottom: 12px;
    }
    .header-text {
        font-weight: 600;
        opacity: 0.8;
    }
    </style>
    """, unsafe_allow_html=True)

    # -------- HEADER --------
    col3, col4, col5, col6 = st.columns([4, 3, 3, 3], vertical_alignment="center")

    col3.markdown('<div class="header-text">Email</div>', unsafe_allow_html=True)
    col4.markdown('<div class="header-text">Fullname</div>', unsafe_allow_html=True)
    col5.markdown('<div class="header-text">Role</div>', unsafe_allow_html=True)
    col6.markdown('<div class="header-text" style="text-align:left;">Action</div>', unsafe_allow_html=True)

    st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)

    # -------- ROWS --------
    for user in users.itertuples():
        col3, col4, col5, col6 = st.columns([4, 3, 3, 3], vertical_alignment="center")

        col3.markdown(
            f'<div class="header-text">{user.email}</div>',
            unsafe_allow_html=True
        )

        col4.markdown(
            f'<div class="header-text">{user.fullname}</div>',
            unsafe_allow_html=True
        )

        col5.markdown(
            f'<div class="header-text">{user.role}</div>',
            unsafe_allow_html=True
        )

        with col6:
            if st.button("Manage", key=f"manage_{user.user_id}", type="primary"):
                edit_account_modal(host, user, token)

        st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)
