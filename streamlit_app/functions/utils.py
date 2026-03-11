"""Utils module.

This module is part of `streamlit_app.functions` and contains runtime logic used by the
Traders Family application.
"""

import logging
import re
import httpx
import pandas as pd
import streamlit as st
from decouple import config
from datetime import date, datetime, timedelta
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from jose import jwt
from app.db.models.user import UserToken, TfUser
from jose.exceptions import ExpiredSignatureError, JWTError
from sqlalchemy.orm import Session, sessionmaker
from streamlit_cookies_controller import CookieController
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
from dateutil.relativedelta import relativedelta
import plotly.io as pio
import plotly.graph_objects as go
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


def get_user(user_id):
    """Fetch persisted token/session row by user ID.

    Args:
        user_id: User identifier used to query token table.

    Returns:
        UserToken | None: Matching token/session row if available.
    """
    session_gen = get_streamlit()
    session = next(session_gen)
    with session.begin():
        query = select(UserToken).filter_by(user_id=user_id)
        data = session.execute(query).scalars().first()
    session.close()
    return data


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


async def fetch_data(
        st,
        host,
        uri,
        params=None,
        method: str = "GET",
        json_payload: dict | None = None
):
    """
    Fetches data from a protected API endpoint, handling token refresh and errors gracefully.
    
    Args:
        st: Streamlit module/state object.
        host (str): The base URL of the API host.
        uri (str): The specific endpoint URI.
        params (dict | None): Query parameters for request.
        method (str): HTTP method (`GET`/`POST`/etc).
        json_payload (dict | None): JSON payload for non-GET request.
    
    Returns:
        dict: JSON payload from API, or error payload on failure.
    """
    try:
        user = get_user(st.session_state._user_id)
        url = f"{host}/api/{uri}"
        headers = {
            "Authorization": f"Bearer {user.access_token}",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
            )
            response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
            return response.json()
        
    except HTTPError as http_error:
        logging.error(f"HTTP error occurred: {http_error}")
        return {"message": f"HTTP error: {http_error}"}
    except (ConnectionError, Timeout) as conn_error:
        logging.error(f"Connection error or timeout: {conn_error}")
        return {"message": f"Connection error or timeout: {conn_error}"}
    except RequestException as req_error:
        logging.error(f"Request failed: {req_error}")
        return {"message": f"Request failed: {req_error}"}
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return {"message": f"An unexpected error occurred: {e}"}


def campaign_preset_ranges(today: date) -> dict[str, tuple[date, date] | None]:
    """Build reusable preset date windows for campaign-facing dashboard pages.

    Args:
        today (date): Reference date used to calculate rolling and month-based
            preset windows.

    Returns:
        dict[str, tuple[date, date] | None]: Mapping of preset labels to
        inclusive date ranges. ``Custom Range`` intentionally maps to ``None``
        so the caller can render a date picker.
    """
    yesterday = today - timedelta(days=1)
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
    last_month_end = this_month_start - timedelta(days=1)
    return {
        "Last 7 Day": (today - timedelta(days=7), yesterday),
        "Last 30 Day": (today - timedelta(days=30), yesterday),
        "This Month": (this_month_start, today),
        "Last Month": (last_month_start, last_month_end),
        "Custom Range": None,
    }


def campaign_figure_from_payload(payload: dict | None, title: str) -> go.Figure:
    """Convert serialized Plotly payload into a display-ready figure object.

    Besides rehydrating standard chart payloads, this helper also normalizes
    table figures so numeric columns are formatted consistently in the FE and
    empty states degrade into a readable placeholder figure.

    Args:
        payload (dict | None): Plotly JSON payload returned by backend APIs.
        title (str): Fallback title used when the payload is empty or invalid.

    Returns:
        go.Figure: Ready-to-render Plotly figure instance with FE formatting
        adjustments applied when relevant.
    """
    if payload and isinstance(payload, dict):
        figure = go.Figure(payload)
        if figure.data and isinstance(figure.data[0], go.Table):
            table_trace = figure.data[0]
            header_values = [str(value).strip().lower() for value in (table_trace.header.values or [])]
            text_only_headers = {"ad name", "ad group", "campaign", "campaign_name"}
            formatted_columns = []
            for column_index, column_values in enumerate(table_trace.cells.values or []):
                header_name = header_values[column_index] if column_index < len(header_values) else ""
                formatted_values = []
                for value in column_values:
                    if header_name in text_only_headers:
                        formatted_values.append(value)
                        continue
                    try:
                        number = float(value)
                        if "share" in header_name:
                            formatted_values.append(f"{number:,.2f}".rstrip("0").rstrip("."))
                        else:
                            formatted_values.append(f"{int(number):,}" if number.is_integer() else f"{number:,.2f}")
                    except (TypeError, ValueError):
                        formatted_values.append(value)
                formatted_columns.append(formatted_values)

            figure.update_traces(
                header=dict(
                    fill_color="#1f2937",
                    font=dict(color="#f8fafc", size=14),
                    align="center",
                ),
                cells=dict(
                    values=formatted_columns,
                    fill_color="#0f172a",
                    font=dict(color="#e2e8f0", size=13),
                    align="center",
                    height=34,
                ),
                domain=dict(y=[0.24, 0.76]),
                selector=dict(type="table"),
            )
            figure.update_layout(margin=dict(l=0, r=0, t=56, b=0), height=430)
        return figure

    figure = go.Figure()
    figure.update_layout(
        title=title,
        annotations=[
            {
                "text": "No data available",
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
            }
        ],
    )
    return figure


def _campaign_format_number(value: float | int) -> str:
    """Format numeric metric values for compact, human-readable card display.

    Args:
        value (float | int): Raw metric value that may already be numeric or a
            numeric-like object.

    Returns:
        str: Formatted integer or decimal string with thousand separators.
        Invalid inputs degrade to ``"0"`` instead of raising UI errors.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "0"

    if number.is_integer():
        return f"{int(number):,}"
    return f"{number:,.2f}"


def _campaign_format_currency(value: float | int, compact: bool = False) -> str:
    """Format an IDR-denominated currency metric for cards and tables.

    Args:
        value (float | int): Currency value already expressed in IDR.
        compact (bool): When ``True``, abbreviate large numbers into
            ``K/M/B``-style notation for tight UI surfaces.

    Returns:
        str: Formatted IDR currency string prefixed with ``Rp.``.
    """
    full_currency = f"Rp. {_campaign_format_number(value)}"
    if not compact and len(full_currency) <= 12:
        return full_currency

    number = float(value)
    absolute = abs(number)
    if absolute >= 1_000_000_000:
        compact_value = f"{number / 1_000_000_000:.1f}B"
    elif absolute >= 1_000_000:
        compact_value = f"{number / 1_000_000:.1f}M"
    elif absolute >= 1_000:
        compact_value = f"{number / 1_000:.1f}K"
    else:
        compact_value = _campaign_format_number(number)

    return f"Rp. {compact_value}"


def _campaign_format_usd(value: float | int, compact: bool = False) -> str:
    """Format a USD-denominated currency metric for cards and tables.

    Args:
        value (float | int): Currency value already expressed in USD.
        compact (bool): When ``True``, abbreviate large numbers into
            ``K/M/B``-style notation for tight UI surfaces.

    Returns:
        str: Formatted USD currency string prefixed with ``$``.
    """
    full_currency = f"$ {_campaign_format_number(value)}"
    if not compact and len(full_currency) <= 12:
        return full_currency

    number = float(value)
    absolute = abs(number)
    if absolute >= 1_000_000_000:
        compact_value = f"{number / 1_000_000_000:.1f}B"
    elif absolute >= 1_000_000:
        compact_value = f"{number / 1_000_000:.1f}M"
    elif absolute >= 1_000:
        compact_value = f"{number / 1_000:.1f}K"
    else:
        compact_value = _campaign_format_number(number)

    return f"$ {compact_value}"


def _campaign_convert_idr_to_usd(value: float | int) -> float:
    """Convert an IDR-denominated metric value into USD.

    Args:
        value (float | int): Source monetary value represented in IDR.

    Returns:
        float: Converted USD value based on ``USD_TO_IDR_RATE`` from env.
        If the configured rate is invalid or zero, the original numeric value
        is returned as a defensive fallback.
    """
    rate = float(config("USD_TO_IDR_RATE", default=16968, cast=float))
    if rate == 0:
        return float(value)
    return float(value) / rate


def _campaign_format_growth(growth: float | None) -> str:
    """Format growth percentage text used by dashboard metric cards.

    Args:
        growth (float | None): Growth percentage value, typically already
            calculated against the previous comparison window.

    Returns:
        str: Human-readable delta string such as ``+12.40% from last period``.
        ``None`` values degrade to ``N/A``.
    """
    if growth is None:
        return "N/A"
    sign = "+" if growth > 0 else ""
    return f"{sign}{growth:.2f}% from last period"


def _campaign_metric_value(metrics: dict[str, float], key: str) -> float:
    """Safely extract one metric value as ``float`` from a payload mapping.

    Args:
        metrics (dict[str, float]): Metrics mapping returned by API payload.
        key (str): Metric identifier to extract.

    Returns:
        float: Numeric metric value, defaulting to ``0.0`` when missing or not
        parseable.
    """
    value = metrics.get(key, 0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _campaign_growth_from_periods(source_metrics: dict[str, object], key: str) -> float:
    """Calculate metric growth using current and previous period payloads.

    This helper is used as a frontend fallback when the backend payload does
    not explicitly include a growth value for a specific metric.

    Args:
        source_metrics (dict[str, object]): Summary payload containing
            ``current_period`` and ``previous_period`` metric blocks.
        key (str): Metric identifier to compare across periods.

    Returns:
        float: Growth percentage rounded to 2 decimals.
    """
    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    previous_metrics = source_metrics.get("previous_period", {}).get("metrics", {})
    current_value = _campaign_metric_value(current_metrics, key)
    previous_value = _campaign_metric_value(previous_metrics, key)

    if previous_value == 0:
        if current_value == 0:
            return 0.0
        return 100.0
    return round(((current_value - previous_value) / previous_value) * 100, 2)


def render_campaign_metric_cards(st, source_metrics: dict[str, object], source_label: str) -> None:
    """Render the primary five-card KPI row for campaign performance pages.

    Args:
        st: Streamlit module/object used to write UI components.
        source_metrics (dict[str, object]): API summary payload containing
            current-period metrics and optional growth values.
        source_label (str): Human-readable source name shown in the section
            title, for example ``Google Ads``.

    Returns:
        None: Renders metric cards as Streamlit side effects.
    """
    st.markdown(f'<div class="metric-section-title">{source_label} Performance</div>', unsafe_allow_html=True)

    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = source_metrics.get("growth_percentage", {})
    cards = [
        ("Cost Spend", "cost"),
        ("Impressions", "impressions"),
        ("Clicks", "clicks"),
        ("Leads", "leads"),
        ("Cost/Leads", "cost_leads"),
    ]

    columns = st.columns(5, gap="small")
    for column, (label, key) in zip(columns, cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key == "cost":
                    metric_value = _campaign_format_currency(raw_value)
                elif key == "cost_leads":
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                else:
                    metric_value = _campaign_format_number(raw_value)

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                growth_text = _campaign_format_growth(growth_value)
                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )


def render_brand_awareness_metric_cards(st, source_metrics: dict[str, object], source_label: str) -> None:
    """Render six KPI cards for selected Brand Awareness source."""
    if source_label:
        st.markdown(f'<div class="metric-section-title">{source_label} - Brand Awareness</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <style>
            div[data-testid="stMetricLabel"] > div {
                white-space: nowrap !important;
                overflow: hidden !important;
                text-overflow: ellipsis !important;
                font-size: 1.05rem !important;
            }
            div[data-testid="stMetricValue"] > div {
                font-size: 1.8rem !important;
                line-height: 1.15 !important;
            }
            div[data-testid="stMetricDelta"] > div {
                font-size: 0.92rem !important;
                white-space: nowrap !important;
                overflow: hidden !important;
                text-overflow: ellipsis !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    current_metrics = source_metrics.get("current_period", {}).get("metrics", {})
    growth_metrics = source_metrics.get("growth_percentage", {})
    cards = [
        ("Spend", "cost"),
        ("Impr.", "impressions"),
        ("Clicks", "clicks"),
        ("CTR", "ctr"),
        ("CPM", "cpm"),
        ("CPC", "cpc"),
    ]

    columns = st.columns(6, gap="small")
    for column, (label, key) in zip(columns, cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key in ("cost", "cpm", "cpc"):
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                elif key == "ctr":
                    metric_value = f"{raw_value:.2f}%"
                else:
                    metric_value = _campaign_format_number(raw_value)

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                if growth_value is None:
                    growth_text = "N/A"
                else:
                    sign = "+" if growth_value > 0 else ""
                    growth_text = f"{sign}{growth_value:.2f}%"
                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )


def render_overview_metric_cards(st, summary_payload: dict[str, object]) -> None:
    """Render active-user overview metric cards.

    Args:
        st: Streamlit module/object used to render components.
        summary_payload: ``stickiness_with_growth`` payload from
            ``/api/overview/active-users``.
    """
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    cards = [
        ("Last Day Stickiness", "last_day_stickiness"),
        ("Average Stickiness", "average_stickiness"),
        ("Active User", "active_user"),
    ]

    columns = st.columns(3, gap="small")
    for column, (label, key) in zip(columns, cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key == "active_user":
                    metric_value = _campaign_format_number(raw_value)
                else:
                    metric_value = f"{raw_value:.2f}%"

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)

                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )


def render_overview_cost_metric_cards(st, summary_payload: dict[str, object]) -> None:
    """Render ad-cost spend metric cards for Overview.

    Args:
        st: Streamlit module/object used to render components.
        summary_payload: ``cost_metrics_with_growth`` payload from
            ``/api/overview/campaign-cost``.
    """
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    cards = [
        ("Total Ad Cost", "total_ad_cost"),
        ("Google Ad Cost", "google_ad_cost"),
        ("Facebook Ad Cost", "facebook_ad_cost"),
        ("Tiktok Ad Cost", "tiktok_ad_cost"),
    ]

    columns = st.columns(4, gap="small")
    for column, (label, key) in zip(columns, cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                metric_value = _campaign_format_currency(raw_value, compact=True)

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)

                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )


def render_overview_leads_metric_cards(
    st,
    summary_payload: dict[str, object],
    currency_unit: str = "IDR",
) -> None:
    """Render user-acquisition metric cards for Overview.

    Args:
        st: Streamlit module/object used to render components.
        summary_payload: ``metrics_with_growth`` payload from
            ``/api/overview/leads-acquisition``.
    """
    current_metrics = summary_payload.get("current_period", {}).get("metrics", {})
    growth_metrics = summary_payload.get("growth_percentage", {})
    primary_cards = [
        ("Cost", "cost"),
        ("Impressions", "impressions"),
        ("Clicks", "clicks"),
        ("Leads", "leads"),
        ("Cost/Leads", "cost_leads"),
    ]
    secondary_cards = [
        ("First Deposit", "first_deposit"),
        ("Cost to First Deposit", "cost_to_first_deposit"),
    ]

    columns = st.columns(5, gap="small")
    for column, (label, key) in zip(columns, primary_cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key in ("cost", "cost_leads"):
                    if currency_unit == "USD":
                        metric_value = _campaign_format_usd(
                            _campaign_convert_idr_to_usd(raw_value),
                            compact=True,
                        )
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                else:
                    metric_value = _campaign_format_number(raw_value)

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)

                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )

    secondary_columns = st.columns(2, gap="small")
    for column, (label, key) in zip(secondary_columns, secondary_cards):
        with column:
            with st.container(border=True):
                raw_value = _campaign_metric_value(current_metrics, key)
                if key == "first_deposit":
                    if currency_unit == "USD":
                        metric_value = _campaign_format_usd(
                            _campaign_convert_idr_to_usd(raw_value),
                            compact=True,
                        )
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                else:
                    metric_value = f"{raw_value:.2f}%"

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)

                st.metric(
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                )


def get_streamlit():
    """Yield synchronous SQLAlchemy session for Streamlit components.

    Yields:
        Session: Active SQLAlchemy session bound to Streamlit DB engine.
    """
    with streamlit_session() as session:
        try:
            yield session
        finally:
            session.close()


def get_accounts(
        data: str ="all",
        user_id: str = None
    ):
    """Retrieve account records for admin pages.

    Args:
        data (str): Retrieval mode, `all` for list or any other value for single user.
        user_id (str | None): User identifier used when fetching single account.

    Returns:
        pd.DataFrame | TfUser | None: All users as DataFrame or single user entity.
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
    """Restore Streamlit state from persisted backend session.

    Args:
        session_id: Session identifier stored in browser cookie.

    Returns:
        UserToken | None: Matching session row if present.
    """
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
                if "server_session" in st.session_state:
                    del st.session_state.server_session
                st.toast("Session is expired! Please Re Log In.")
            session.close()
        return user
    

def footer(st):
    """Render fixed footer element at the bottom of Streamlit page.

    Args:
        st: Streamlit module instance used to render markdown/html.
    """

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
    Handle logout button action and clear client/session state.

    Args:
        st: Streamlit module instance.
        host (str): Base URL of the API.
        session_id: Persisted session identifier (currently unused in request payload).
    """
    
    if st.button("Log Out", type="secondary", width="stretch"):
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
                    if "server_session" in st.session_state:
                        del st.session_state.server_session
                        
                    st.success("Logged out successfully!")
                    st.rerun()  # Redirect to login page (or home page)
                else:
                    error_message = data.get('message', "Logout failed")
                    st.error(error_message)

            except RequestException as e:
                st.error(f"An error occurred during logout: {e}. Please try again later.")
def is_valid_email(email):
    """Validate email string against a basic regex pattern.

    Args:
        email: Email value to validate.

    Returns:
        Match[str] | None: Regex match object when valid, otherwise `None`.
    """
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

    return re.match(pattern,email)


@st.dialog("Create Account")
def add_account_modal(host, token):
    """Render and process create-account modal form.

    Args:
        host (str): Base URL of backend API.
        token: Current authenticated token object for authorization header.
    """
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
                                cookies={
                                    "csrf_token": st.session_state.get("csrf_token", ""),
                                    "session": st.session_state.get("server_session", ""),
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
    """Render and process account edit/delete modal actions.

    Args:
        host (str): Base URL of backend API.
        user: Selected user row for edit/delete operations.
        token: Current authenticated token object for authorization header.
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
