"""Utils module.

This module is part of `streamlit_app.functions` and contains runtime logic used by the
Traders Family application.
"""

import logging
import re
from urllib.parse import urlparse
import httpx
import pandas as pd
import streamlit as st
from decouple import config
from datetime import date, datetime, timedelta
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from app.db.models.user import TfUser
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


def get_access_token() -> str | None:
    """Read the current bearer token from Streamlit session storage.

    Returns:
        str | None: Access token used for authenticated backend API calls, or
        ``None`` when the current browser session is not authenticated.
    """
    return st.session_state.get("access_token")


def refresh_cookie_options(host_url: str) -> dict[str, object]:
    """Build cookie options that behave correctly for localhost and HTTPS hosts."""
    parsed = urlparse(host_url)
    hostname = parsed.hostname
    secure = parsed.scheme == "https"
    options: dict[str, object] = {
        "path": "/",
        "same_site": "strict",
        "secure": secure,
    }
    if hostname and hostname not in {"localhost", "127.0.0.1"}:
        options["domain"] = hostname
    return options


def sync_refresh_cookie(host: str, refresh_token: str | None) -> None:
    """Keep the browser refresh-token cookie aligned with the latest rotation."""
    if not refresh_token:
        return

    if not cookie_controller.get("refresh_token"):
        return

    cookie_controller.set(
        name="refresh_token",
        value=refresh_token,
        expires=datetime.now() + timedelta(days=7),
        **refresh_cookie_options(host),
    )


async def restore_backend_session(host: str, refresh_token: str) -> dict | None:
    """Attempt silent login restoration with a persisted refresh token.

    Args:
        host (str): Backend base URL that exposes the refresh endpoint.
        refresh_token (str): Refresh token recovered from cookie or session
            storage.

    Returns:
        dict | None: Parsed backend response when rotation succeeds, otherwise
        ``None`` when the token is missing, rejected, or the response body is
        empty.
    """
    if not refresh_token:
        return None

    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            f"{host}/api/token/refresh",
            json={"refresh_token": refresh_token},
        )
    if response.status_code >= 400:
        return None
    return response.json() if response.content else None


async def refresh_backend_tokens(host: str, refresh_token: str) -> dict | None:
    """Request a rotated bearer-token pair from the backend auth service.

    Args:
        host (str): Backend API base URL used to call the refresh endpoint.
        refresh_token (str): Persisted refresh token currently stored in
            Streamlit session state or browser cookie.

    Returns:
        dict | None: Parsed refresh payload when rotation succeeds, otherwise
        ``None`` if the backend rejects or cannot return a valid response.
    """
    return await restore_backend_session(host=host, refresh_token=refresh_token)


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
        access_token = get_access_token()
        if not access_token:
            return {"message": "Session invalid. Please log in again."}
        url = f"{host}/api/{uri}"
        headers = {
            "Authorization": f"Bearer {access_token}",
        }
        async with httpx.AsyncClient(timeout=120) as client:
            response = await client.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                json=json_payload,
            )
            if response.status_code == 401 and st.session_state.get("refresh_token"):
                refreshed_payload = await refresh_backend_tokens(
                    host=host,
                    refresh_token=st.session_state["refresh_token"],
                )
                if refreshed_payload and refreshed_payload.get("success"):
                    st.session_state.access_token = refreshed_payload.get("access_token")
                    st.session_state.refresh_token = refreshed_payload.get("refresh_token")
                    st.session_state.session_id = refreshed_payload.get(
                        "session_id",
                        st.session_state.get("session_id"),
                    )
                    sync_refresh_cookie(host, st.session_state.refresh_token)
                    headers["Authorization"] = f"Bearer {st.session_state.access_token}"
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


def _campaign_format_compact_number(value: float | int, suffix: str = "") -> str:
    """Format a numeric value into compact K/M/B/T notation for narrow cards.

    Args:
        value (float | int): Raw numeric value to abbreviate.
        suffix (str): Optional string appended after the compact number, such
            as a unit symbol.

    Returns:
        str: Compact representation that preserves up to roughly three
        significant digits, for example ``8.93M`` or ``12.4K``.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return f"0{suffix}"

    absolute = abs(number)
    if absolute >= 1_000_000_000_000:
        scaled = number / 1_000_000_000_000
        unit = "T"
    elif absolute >= 1_000_000_000:
        scaled = number / 1_000_000_000
        unit = "B"
    elif absolute >= 1_000_000:
        scaled = number / 1_000_000
        unit = "M"
    elif absolute >= 1_000:
        scaled = number / 1_000
        unit = "K"
    else:
        scaled = number
        unit = ""

    if not unit:
        return f"{_campaign_format_number(scaled)}{suffix}"

    integer_digits = len(str(int(abs(scaled)))) if scaled else 1
    decimal_places = max(0, 3 - integer_digits)
    compact_value = f"{scaled:.{decimal_places}f}".rstrip("0").rstrip(".")
    return f"{compact_value}{unit}{suffix}"


def _campaign_format_currency(value: float | int, compact: bool = False) -> str:
    """Format an IDR-denominated currency metric for cards and tables.

    Args:
        value (float | int): Currency value already expressed in IDR.
        compact (bool): When ``True``, abbreviate large numbers into
            ``K/M/B``-style notation for tight UI surfaces.

    Returns:
        str: Formatted IDR currency string prefixed with ``Rp.``.
    """
    if not compact:
        return f"Rp. {_campaign_format_number(value)}"
    return f"Rp. {_campaign_format_compact_number(value)}"


def _campaign_format_usd(value: float | int, compact: bool = False) -> str:
    """Format a USD-denominated currency metric for cards and tables.

    Args:
        value (float | int): Currency value already expressed in USD.
        compact (bool): When ``True``, abbreviate large numbers into
            ``K/M/B``-style notation for tight UI surfaces.

    Returns:
        str: Formatted USD currency string prefixed with ``$``.
    """
    if not compact:
        return f"$ {_campaign_format_number(value)}"
    return f"$ {_campaign_format_compact_number(value)}"


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


def _render_hover_metric_card(
    st,
    label: str,
    value: str,
    delta: str,
    growth_value: float | None = None,
    tooltip: str | None = None,
) -> None:
    """Render one metric card using Streamlit defaults plus optional help text.

    Args:
        st: Streamlit module/object used to render the metric widget.
        label (str): Metric label shown above the value.
        value (str): Preformatted metric value string.
        delta (str): Preformatted growth or delta text shown below the value.
        growth_value (float | None): Raw numeric delta used to decide whether
            neutral coloring should be applied for zero growth.
        tooltip (str | None): Optional help text exposed through Streamlit's
            built-in metric help affordance.

    Returns:
        None: Writes the metric widget into the active Streamlit container.
    """
    delta_color = "normal"
    if growth_value == 0:
        delta_color = "off"

    st.metric(
        label=label,
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=tooltip,
    )


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
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                    tooltip_value = _campaign_format_currency(raw_value, compact=False)
                elif key == "cost_leads":
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                    tooltip_value = _campaign_format_currency(raw_value, compact=False)
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                growth_text = _campaign_format_growth(growth_value)
                if key in {"cost", "cost_leads"}:
                    _render_hover_metric_card(
                        st,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
                    st.metric(
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                    )


def render_brand_awareness_metric_cards(st, source_metrics: dict[str, object], source_label: str) -> None:
    """Render the six-card KPI row for Brand Awareness dashboard sections.

    Args:
        st: Streamlit module/object used to output UI components.
        source_metrics (dict[str, object]): Summary payload containing current,
            previous, and growth metrics for the selected source.
        source_label (str): Optional source label shown in the section title.

    Returns:
        None: Produces Streamlit metric cards as UI side effects.
    """
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
                font-size: 0.84rem !important;
                white-space: normal !important;
                overflow: visible !important;
                text-overflow: unset !important;
                line-height: 1.2 !important;
                word-break: break-word !important;
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
                if key == "cpc":
                    metric_value = f"Rp. {round(raw_value):,}"
                    tooltip_value = f"Rp. {round(raw_value):,}"
                elif key in ("cost", "cpm"):
                    metric_value = _campaign_format_currency(raw_value, compact=True)
                    tooltip_value = _campaign_format_currency(raw_value, compact=False)
                elif key == "ctr":
                    metric_value = f"{raw_value:.2f}%"
                    tooltip_value = None
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = _campaign_growth_from_periods(source_metrics, key)
                growth_text = _campaign_format_growth(growth_value)
                if key in {"cost", "cpm", "cpc"}:
                    _render_hover_metric_card(
                        st,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
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
                tooltip_value = _campaign_format_currency(raw_value, compact=False)

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)
                _render_hover_metric_card(
                    st,
                    label=label,
                    value=metric_value,
                    delta=growth_text,
                    growth_value=growth_value,
                    tooltip=tooltip_value,
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
                        converted_value = _campaign_convert_idr_to_usd(raw_value)
                        metric_value = _campaign_format_usd(
                            converted_value,
                            compact=True,
                        )
                        tooltip_value = _campaign_format_usd(converted_value, compact=False)
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                        tooltip_value = _campaign_format_currency(raw_value, compact=False)
                else:
                    metric_value = _campaign_format_number(raw_value)
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)
                if key in {"cost", "cost_leads"}:
                    _render_hover_metric_card(
                        st,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
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
                        converted_value = _campaign_convert_idr_to_usd(raw_value)
                        metric_value = _campaign_format_usd(
                            converted_value,
                            compact=True,
                        )
                        tooltip_value = _campaign_format_usd(converted_value, compact=False)
                    else:
                        metric_value = _campaign_format_currency(raw_value, compact=True)
                        tooltip_value = _campaign_format_currency(raw_value, compact=False)
                else:
                    metric_value = f"{raw_value:.2f}%"
                    tooltip_value = None

                growth_value = growth_metrics.get(key)
                if growth_value is None:
                    growth_value = 0.0
                growth_text = _campaign_format_growth(growth_value)
                if key == "first_deposit":
                    _render_hover_metric_card(
                        st,
                        label=label,
                        value=metric_value,
                        delta=growth_text,
                        growth_value=growth_value,
                        tooltip=tooltip_value,
                    )
                else:
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


async def logout(st, host):
    """
    Handle logout button action and clear client/session state.

    Args:
        st: Streamlit module instance.
        host (str): Base URL of the API.
    """
    if st.button("Log Out", type="secondary", width="stretch"):
        with st.spinner("Logging out..."):
            try:
                access_token = get_access_token()
                if not access_token:
                    st.error("Session invalid. Please log in again.")
                    return
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {access_token}"
                }
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(f"{host}/api/logout", headers=headers)
                    response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
                    data =  response.json()
                
                if data.get('success'):
                    # Clear session state
                    cookie_controller.set("refresh_token", "", max_age=0)
                    del st.session_state.logged_in
                    del st.session_state.page
                    del st.session_state._user_id
                    del st.session_state.role
                    if "access_token" in st.session_state:
                        del st.session_state.access_token
                    if "refresh_token" in st.session_state:
                        del st.session_state.refresh_token
                    if "session_id" in st.session_state:
                        del st.session_state.session_id
                        
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
                                headers={
                                    "Authorization": f"Bearer {token}",
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
                    "Authorization": f"Bearer {token}"
                }
            )
            st.rerun()
