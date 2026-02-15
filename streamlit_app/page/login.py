import httpx
import streamlit as st
import requests
from datetime import datetime, timedelta
from streamlit_app.functions.utils import get_user
from streamlit_app.functions.utils import cookie_controller



async def show_login_page(host):
    st.image("./streamlit_app/page/logotf.png", width=100)
    st.title('Traders Family Dashboard')
    
    with st.form("log-in", border=False):
        email = st.text_input('Email')
        password = st.text_input('Password', type='password')
        remember = st.checkbox('Remember Me!')
        submit = st.form_submit_button("Login")
        
    if submit:
        with st.spinner("Loggin in!"):
            async with httpx.AsyncClient(timeout=120) as client:
                # Make a GET request to initialize the CSRF token
                csrf_response = await client.post(
                    f"{host}/api/login/csrf-token",
                    data={"username": email, "password": password})
                
                # Check if the CSRF cookie is set
                csrf_token = csrf_response.cookies.get("csrf_token", None)

                if csrf_token is None:
                    return st.error("CSRF token initialization failed. Please try again!")
                
                response = await client.post(
                    f"{host}/api/login",
                    data={"username": email, "password": password},
                    cookies={"csrf_token": csrf_token},
                    headers={"X-CSRF-Token": csrf_token}
                )
                response_data = response.json()

            if response_data.get("success", False):
                user = get_user(response.headers["Authentication"])
                st.session_state.role = response_data['role']
                st.session_state.logged_in = True
                st.session_state.page = 'home'
                st.session_state._user_id = response.headers['Authentication']
                st.session_state.csrf_token = csrf_token

                if remember:
                    cookie_controller.set(
                        name="session_id", 
                        value=user.session_id,
                        path="/",
                        expires=datetime.now()+timedelta(days=7),
                        domain=st.secrets["api"]["HOST"],
                        same_site="strict",
                        secure=True
                    )
                st.rerun()
            else:
                st.error(f"{response_data.get("detail", "Invalid email or password! Please try again!")}")
