import httpx
import asyncio
import streamlit as st
from datetime import datetime, timedelta
from app.db.models.user import TfUser
from streamlit_app.functions.utils import is_valid_email, get_accounts
from streamlit_app.functions.utils import get_user, get_streamlit


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
            @st.dialog("Create Account")
            def add_account_modal():
                with st.form("register", border=False, clear_on_submit=True):
                    # Input Box
                    fullname = st.text_input("Fullname", width="stretch")
                    email = st.text_input("Email", width="stretch")
                    password = st.text_input("Password", type="password", width="stretch")
                    confirm_password = st.text_input("Confirm Password", type="password", width="stretch")
                    role_options = {
                        "Super Admin": "superadmin",
                        "Admin": "admin",
                        "Digital Marketing" : "digital_marketing",
                        "Sales": "sales",

                    }
                    role = st.selectbox("Role", list(role_options.keys()))
                    submit = st.form_submit_button("Create Account")

                    if submit:
                        email_valid = is_valid_email(email)
                        password_match = password == confirm_password and password != ""

                        if not email_valid:
                            st.warning("Please input a real format email!")
                                
                        elif not password_match:
                            st.warning("Please check if passwords are the same!")
                                
                        else:
                            with st.spinner("Creating account!"):
                                try:
                                    with httpx.Client(timeout=120) as client:
                                        response = client.post(
                                            f"{host}/api/register",
                                            json={
                                                "fullname": fullname,
                                                "email": email,
                                                "role": role_options[role],
                                                "password": password,
                                                "confirm_password": confirm_password
                                            }
                                        )
                                        response_data = response.json()

                                    if response_data["success"]:
                                        st.info("Successfully created an account!")
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error creating account: {response_data.get("detail", "Something error, please try again!")}")

            add_account_modal()

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
            if st.button("Manage", key=f"manage_{user.user_id}"):
                @st.dialog("Manage Account")
                def edit_account_modal():
                    with st.form("edit", border=False, clear_on_submit=True):
                        # Input Box
                        fullname = st.text_input("Fullname", placeholder=user.fullname, width="stretch")
                        email = st.text_input("Email", placeholder=user.email, width="stretch")
                        role_options = {
                            "":"",
                            "Super Admin": "superadmin",
                            "Admin": "admin",
                            "Digital Marketing" : "digital_marketing",
                            "Sales": "sales"
                        }
                        role = st.selectbox("Role", placeholder=user.role, options=list(role_options.keys()))
                        submit = st.form_submit_button("Edit")
                        
                        if submit:
                            acc = get_accounts(data="one", user_id=user.user_id)
                            session_gen = get_streamlit()
                            session = next(session_gen)
                            acc.fullname = fullname if  fullname else user.fullname
                            acc.email = email if email else user.email
                            acc.role = role_options[role] if role else user.role
                            session.add(acc)
                            session.commit()
                            st.rerun()

                    if st.button("Delete User", type="primary"):
                        with httpx.Client(timeout=120) as client:
                            client.delete(
                                f"{host}/api/delete_account/{user.user_id}",
                                headers = {
                                    "Authorization": f"Bearer {token.access_token}"
                                }
                            )
                        st.rerun()

                edit_account_modal()

        st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)
