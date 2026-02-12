import streamlit as st
# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Traders Family Dashboard",
    page_icon="./streamlit_app/page/logotf.png",
    layout="wide",  # Optional: Use "wide" for full-width layout
    initial_sidebar_state="collapsed"
)
import asyncio
from decouple import config
from streamlit_app.functions.utils import cookie_controller, get_session, footer, logout
from streamlit_app.page import login, overall, register


# --- App Settings ---
session_id = get_session(cookie_controller.get("session_id"))
HOST = st.secrets["api"]["HOST"] if config("ENV") == "production" else st.secrets["api"]["DEV_HOST"]
footer(st)


# --- Initialize session state ---
if 'page' not in st.session_state:
    st.session_state.page = 'login'
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'role' not in st.session_state:
    st.session_state.role = None

# Login Page
login_page = st.Page(page=login, title="Login")

# Overview data page
overall_page = st.Page(page=overall, title="Overall Data", url_path="/overall-page", icon="🗃")

# Settings Page
register_page = st.Page(page=register, title="Create Account", url_path="/create-account")

# show menu only for spesific role
if st.session_state.role in ['developer', 'superadmin']:
    menu_options = {
        "🗂 Overview Data" : [
            overall_page
        ],
        "⚙️ Settings" : [
            register_page
        ]
    }

# --- NAVIGATION ---
with st.sidebar:
    if st.session_state["logged_in"]:
        st.image("./streamlit_app/page/logotf.png", width=True)
        page = st.navigation(
            menu_options,
            position='sidebar',
            expanded=False
        )
        asyncio.run(logout(st, HOST, session_id))
    else:
        page = st.navigation(
            [login_page],
            position='sidebar',
            expanded=False
        )

# --- PAGE CONTENT ---
try:
    if not st.session_state['logged_in']:
        asyncio.run(login.show_login_page(HOST))
    else:
        page_handlers = {
            overall_page: lambda: overall.show_overall_page(HOST),
            register_page: lambda: register.create_account()
        }
        if page in page_handlers:
            asyncio.run(page_handlers[page]())  # Call the appropriate function based on the page
            st.session_state.page = page.url_path
except Exception as e:
    st.error(f"Error Fetching Data! {e}")
