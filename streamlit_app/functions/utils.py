import streamlit as st
import httpx
import re
from decouple import config
from datetime import datetime
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import Session, sessionmaker
from app.db.models.user import UserToken
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



