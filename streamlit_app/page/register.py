import httpx
import streamlit as st
from datetime import datetime, timedelta
from streamlit_app.functions.utils import is_valid_email


async def create_account(host):
    """
    Docstring for create_account
    """
    st.title("Create Account", width="stretch", text_alignment="center")

    with st.form("register", border=False):
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
                    async with httpx.AsyncClient(timeout=120) as client:
                        response = await client.post(
                            f"{host}/api/register",
                            data={
                                "fullname": fullname,
                                "email": email,
                                "role": role_options[role],
                                "password": password
                            }
                        )
                        response_data = response.json()

                    if response_data["success"]:
                        st.info("Successfully created an account!")
                except Exception as e:
                    st.error(f"Error creating account: {e}")
