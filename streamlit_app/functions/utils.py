import streamlit as st
import httpx
import pandas as pd
import re
from decouple import config
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import Session, sessionmaker
from app.db.models.user import UserToken, TfUser
from streamlit_cookies_controller import CookieController
from requests.exceptions import RequestException
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
cookie_controller = CookieController()

# streamlit engine and sessionmaker
streamlit_engine = create_engine(
    st.secrets["db"]["DB_DEV"] if config("ENV") == "development" else st.secrets["db"]["DB"],
    echo=False,
    poolclass=StaticPool,
    pool_pre_ping=True
)
streamlit_session = sessionmaker(
    bind=streamlit_engine,
    expire_on_commit=False,
    class_=Session
)
def get_streamlit():
    """ """
    with streamlit_session() as session:
        try:
            yield session
        finally:
            session.close()


def get_user(user_id):
    """ """
    session_gen = get_streamlit()
    session = next(session_gen)
    with session.begin():
        query = select(UserToken).filter_by(user_id=user_id)
        data = session.execute(query).scalars().first()
    session.close()
    return data


def get_accounts(
        data: str ="all",
        user_id: str = None
    ):
    """
    Docstring for get_accounts
    """
    session_gen = get_streamlit()
    session = next(session_gen)
    with session.begin():
        if data == "all":
            query = select(
                TfUser.user_id, 
                TfUser.fullname, 
                TfUser.email, 
                TfUser.role
            ).where(
                TfUser.deleted_at == None,
            )
        else:
            query = select(
                TfUser
            ).where(
                TfUser.user_id == user_id,
                TfUser.deleted_at == None,
            )
        result = session.execute(query)

    df = pd.DataFrame(result.fetchall()) if data == "all" else result.scalar_one_or_none()

    return df


def get_session(session_id):
    """Retrieve session details from the SQLite database."""
    session_generator = get_streamlit()
    session = next(session_generator)
    with session.begin():
        query = select(UserToken).filter_by(session_id=session_id)
        existing_data = session.execute(query)
        user = existing_data.scalars().first()
        if user != None:
            if datetime.now() <= user.expiry and not user.is_revoked:
                st.session_state.role = user.role
                st.session_state.logged_in = user.logged_in
                st.session_state._user_id = user.user_id
                st.session_state.page = user.page
            else:
                user.is_revoked = True
                user.logged_in = False
                session.commit()
                
                cookie_controller.set("session_id", "", max_age=0)
                del st.session_state.logged_in
                del st.session_state.page
                del st.session_state._user_id
                del st.session_state.role
                st.toast("Session is expired! Please Re Log In.")
            session.close()
        return user
    

def footer(st):
    """Renders a styled footer at the bottom of the Streamlit app."""

    # Using a template string for better readability
    footer_html = f"""
    <style>
        .footer {{
            position: fixed;
            left: 0;
            bottom: 0;
            width: 100%;
            padding: 10px;
            text-align: right;
            font-size: 14px;
            color: #666; /* Slightly darker text */
        }}
    </style>
    <div class="footer">
        <p>© {datetime.now().year}, made with 💰</p> 
    </div>
    """

    st.markdown(footer_html, unsafe_allow_html=True)


async def logout(st, host, session_id):
    """
    Handles the logout process, clearing session state and redirecting the user.

    Args:
        st: Streamlit object for interacting with the app.
        host (str): Base URL of the API.
    """
    
    if st.button("Log Out", use_container_width=True):
        with st.spinner("Logging out..."):
            try:
                user = get_user(st.session_state._user_id)
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {user.access_token}"
                }
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(f"{host}/api/logout", headers=headers)
                    response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
                    data =  response.json()
                
                if data.get('success'):
                    # Clear session state
                    cookie_controller.set("session_id", "", max_age=0)
                    del st.session_state.logged_in
                    del st.session_state.page
                    del st.session_state._user_id
                    del st.session_state.role
                        
                    st.success("Logged out successfully!")
                    st.rerun()  # Redirect to login page (or home page)
                else:
                    error_message = data.get('message', "Logout failed")
                    st.error(error_message)

            except RequestException as e:
                st.error(f"An error occurred during logout: {e}. Please try again later.")


def get_date_range(days, period='days', months=3):
    """
    Returns the date range from today minus the specified number of days to yesterday, or from the start of the month
    a specified number of months ago to the last day of the previous month.

    Args:
        days (int): The number of days to go back from today to determine the start date when period is 'days'.
        period (str): The period type, either 'days' or 'months'. Default is 'days'.
        months (int): The number of months to go back from the current month to determine the start date when period is 'months'. Default is 3.

    Returns:
        tuple: A tuple containing the start date and end date (both in datetime.date format).
    """
    if period == 'days':
        # Calculate the end date as yesterday
        end_date = datetime.today() - timedelta(days=1)
        # Calculate the start date based on the number of days specified
        start_date = datetime.today() - timedelta(days=days)
    elif period == 'months':
        # Get the first day of the current month
        end_date = datetime.today().replace(day=1)
        # Calculate the start date based on the number of months specified
        start_date = end_date - relativedelta(months=months)
        # Adjust the end date to be the last day of the previous month
        end_date = end_date - relativedelta(days=1)
        
    return start_date.date(), end_date.date()


def is_valid_email(email):
    """
    Docstring for is_valid_email
    
    :param email: email account of an user
    """
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

    return re.match(pattern,email)


@st.dialog("Create Account")
def add_account_modal(host, token):
    with st.form("register", border=False, clear_on_submit=True):
        # Input Box
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
                                    "confirm_password": confirm_password,
                                },
                                headers={
                                    "Authorization": f"Bearer {token.access_token}",
                                    "X-CSRF-Token": st.session_state.csrf_token,
                                },
                            )
                            response_data = response.json()

                        if response_data["success"]:
                            st.info("Successfully created an account!")
                            st.rerun()

                    except Exception as e:
                        st.error(
                            f"Error creating account: {response_data.get('detail', 'Something error, please try again!')}"
                        )


@st.dialog("Manage Account")
def edit_account_modal(
    host, 
    user, 
    token
):
    """
    """
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

