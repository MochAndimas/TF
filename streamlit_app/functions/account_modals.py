"""Account-management dialogs for Streamlit admin pages."""

from __future__ import annotations

import httpx
import streamlit as st

from streamlit_app.functions.accounts import get_accounts, is_valid_email
from streamlit_app.functions.runtime import get_streamlit


@st.dialog("Create Account")
def add_account_modal(host, token):
    """Render and process create-account modal form."""
    with st.form("register", border=False, clear_on_submit=True):
        fullname = st.text_input("Fullname", width="stretch")
        email = st.text_input("Email", width="stretch")
        password = st.text_input("Password", type="password", width="stretch")
        confirm_password = st.text_input("Confirm Password", type="password", width="stretch")

        role_options = {
            "Super Admin": "superadmin",
            "Admin": "admin",
            "Digital Marketing": "digital_marketing",
            "Sales": "sales",
        }

        role = st.selectbox("Role", list(role_options.keys()))
        submit = st.form_submit_button("Create Account")

        if submit:
            if not is_valid_email(email):
                st.warning("Please input a real format email!")
            elif password != confirm_password or password == "":
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
                                    "confirm_password": confirm_password,
                                },
                                headers={"Authorization": f"Bearer {token}"},
                            )
                            response_data = response.json()

                        if response_data["success"]:
                            st.info("Successfully created an account!")
                            st.rerun()
                    except Exception:
                        st.error(f"Error creating account: {response_data.get('detail', 'Something error, please try again!')}")


@st.dialog("Manage Account")
def edit_account_modal(host, user, token):
    """Render and process account edit/delete modal actions."""
    with st.form("edit", border=False, clear_on_submit=True):
        fullname = st.text_input("Fullname", placeholder=user.fullname, width="stretch")
        email = st.text_input("Email", placeholder=user.email, width="stretch")
        role_options = {
            "": "",
            "Super Admin": "superadmin",
            "Admin": "admin",
            "Digital Marketing": "digital_marketing",
            "Sales": "sales",
        }
        role = st.selectbox("Role", placeholder=user.role, options=list(role_options.keys()))
        submit = st.form_submit_button("Edit")

        if submit:
            acc = get_accounts(data="one", user_id=user.user_id)
            session_gen = get_streamlit()
            session = next(session_gen)
            acc.fullname = fullname if fullname else user.fullname
            acc.email = email if email else user.email
            acc.role = role_options[role] if role else user.role
            session.add(acc)
            session.commit()
            st.rerun()

    if st.button("Delete User", type="primary"):
        with httpx.Client(timeout=120) as client:
            client.delete(
                f"{host}/api/delete_account/{user.user_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            st.rerun()
