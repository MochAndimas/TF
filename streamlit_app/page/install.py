"""Streamlit page for Google Play Console install analytics."""

from __future__ import annotations

import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _campaign_format_growth
from streamlit_app.page.campaign_components.common import PAGE_STYLE, set_transparent_chart_background


def _fmt_int(value) -> str:
    return f"{int(float(value or 0)):,.0f}"


def _fmt_float(value) -> str:
    return f"{float(value or 0):,.2f}"


def _render_period_filter() -> tuple[dt.date | None, dt.date | None]:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Install</div>', unsafe_allow_html=True)

    presets = campaign_preset_ranges(dt.date.today())
    if "install_date_range" not in st.session_state:
        st.session_state["install_date_range"] = presets["This Month"]
    if "install_period" not in st.session_state:
        st.session_state["install_period"] = "This Month"

    with st.container(border=True):
        selected_period = st.selectbox("Periods", options=list(presets.keys()), key="install_period")
        if selected_period == "Custom Range":
            selected = st.date_input("Select Date Range", key="install_date_range")
            if not isinstance(selected, tuple) or len(selected) != 2:
                st.warning("Please select a valid date range.")
                return None, None
            return selected

        start_date, end_date = presets[selected_period]
        if st.session_state.get("install_date_range") != (start_date, end_date):
            st.session_state["install_date_range"] = (start_date, end_date)
        return start_date, end_date


def _render_dimension_filters(filter_options: dict[str, object]) -> tuple[str, str]:
    packages = ["All Packages", *filter_options.get("packages", [])]
    countries = ["All Countries", *filter_options.get("countries", [])]

    selected_package = st.session_state.get("install_package_label", "All Packages")
    selected_country = st.session_state.get("install_country_label", "All Countries")
    if selected_package not in packages:
        st.session_state["install_package_label"] = "All Packages"
    if selected_country not in countries:
        st.session_state["install_country_label"] = "All Countries"

    with st.container(border=True):
        package_col, country_col = st.columns([2, 2], gap="small")
        with package_col:
            selected_package = st.selectbox("Package", options=packages, key="install_package_label")
        with country_col:
            selected_country = st.selectbox("Country", options=countries, key="install_country_label")

    package_key = "all" if selected_package == "All Packages" else selected_package
    country_key = "all" if selected_country == "All Countries" else selected_country
    return package_key, country_key


def _render_metrics(metrics: dict[str, object]) -> None:
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    specs = [
        ("Installers", "installers", _fmt_int(current.get("installers"))),
        ("Uninstallers", "uninstallers", _fmt_int(current.get("uninstallers"))),
        ("Net Installs", "net_installs", _fmt_int(current.get("net_installs"))),
        ("Active Devices", "active_devices", _fmt_int(current.get("active_devices"))),
        ("Churn Rate", "churn_rate", f"{_fmt_float(current.get('churn_rate'))}%"),
    ]
    for column, (label, key, value) in zip(st.columns(5, gap="small"), specs):
        with column:
            with st.container(border=True):
                growth_value = growth.get(key, 0.0)
                delta_color = "inverse" if key in {"uninstallers", "churn_rate"} else "normal"
                if growth_value == 0:
                    delta_color = "off"
                st.metric(
                    label,
                    value,
                    delta=_campaign_format_growth(growth_value),
                    delta_color=delta_color,
                )


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in ("installers", "uninstallers", "net_installs", "active_devices"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")


def _dimension_dataframe(rows: list[dict[str, object]], label_column: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for column in ("installers", "uninstallers", "net_installs", "active_devices"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    df["share_pct"] = pd.to_numeric(df.get("share_pct", 0), errors="coerce").fillna(0.0).astype(float)
    df[label_column] = df[label_column].fillna("").astype(str)
    return df.sort_values("installers", ascending=False)


def _details_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.rename(
        columns={
            "date": "Date",
            "installers": "Installers",
            "uninstallers": "Uninstallers",
            "net_installs": "Net Installs",
            "active_devices": "Active Devices",
            "churn_rate": "Churn Rate",
        }
    )
    columns = [
        "Date",
        "Installers",
        "Uninstallers",
        "Net Installs",
        "Active Devices",
        "Churn Rate",
    ]
    return df[[column for column in columns if column in df.columns]]


def _date_labels(series: pd.Series) -> list[str]:
    return pd.to_datetime(series).dt.strftime("%b %d\n%Y").tolist()


def _build_daily_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Daily Installs",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    labels = _date_labels(df["date"])
    for column, label in [("installers", "Installers"), ("uninstallers", "Uninstallers")]:
        figure.add_trace(
            go.Bar(
                x=labels,
                y=df[column],
                name=label,
                hovertemplate=f"<b>%{{x}}</b><br>{label}: %{{y:,}}<extra></extra>",
            )
        )
    figure.add_trace(
        go.Scatter(
            x=labels,
            y=df["active_devices"],
            name="Active Devices",
            mode="lines+markers",
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Active Devices: %{y:,}<extra></extra>",
        )
    )
    figure.update_layout(
        title="Daily Installs",
        xaxis_title="Date",
        yaxis_title="Installs",
        yaxis2=dict(title="Active Devices", overlaying="y", side="right", showgrid=False),
        xaxis=dict(type="category"),
        barmode="group",
        legend=dict(orientation="h", y=1.14, x=0),
    )
    return figure


def _build_dimension_figure(df: pd.DataFrame, *, label_column: str, title: str) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title=title,
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    top_df = df.head(12).sort_values("installers", ascending=True)
    figure.add_trace(
        go.Bar(
            y=top_df[label_column],
            x=top_df["installers"],
            name="Installers",
            orientation="h",
            customdata=top_df[["uninstallers", "net_installs", "share_pct"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                + "Installers: %{x:,}<br>"
                + "Uninstallers: %{customdata[0]:,}<br>"
                + "Net Installs: %{customdata[1]:,}<br>"
                + "Share: %{customdata[2]:.2f}%<extra></extra>"
            ),
        )
    )
    figure.update_layout(title=title, xaxis_title="Installers", yaxis_title=None, margin=dict(l=18, r=18, t=70, b=36))
    return figure


async def _fetch_install_payload(host: str, start_date: dt.date, end_date: dt.date, package_name: str, country: str) -> dict[str, object] | None:
    result = await fetch_api_result(
        st=st,
        host=host,
        uri="install/analytics",
        method="GET",
        params={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "package_name": package_name,
            "country": country,
        },
    )
    if result.ok and isinstance(result.raw, dict):
        return result.raw
    st.error(result.message or "Failed to fetch install analytics.")
    return None


async def show_install_page(host: str) -> None:
    start_date, end_date = _render_period_filter()
    if not start_date:
        return
    if start_date > end_date:
        st.warning("Start date cannot be after end date.")
        return

    last_payload = st.session_state.get("install_payload", {})
    filter_options = last_payload.get("data", {}).get("filters", {}) if isinstance(last_payload, dict) else {}
    package_name, country = _render_dimension_filters(filter_options)

    selected_range = (start_date, end_date, package_name, country)
    should_fetch = "install_payload" not in st.session_state or st.session_state.get("install_range") != selected_range
    if should_fetch:
        if not st.session_state.get("access_token"):
            st.error("Session invalid. Please log in again.")
            return
        with st.spinner("Fetching install analytics..."):
            response = await _fetch_install_payload(host, start_date, end_date, package_name, country)
        if response is None:
            return
        st.session_state["install_payload"] = response
        st.session_state["install_range"] = selected_range

    data = st.session_state.get("install_payload", {}).get("data", {})
    _render_metrics(data.get("metrics", {}))

    daily_df = _daily_dataframe(data.get("daily_rows", []))
    package_df = _dimension_dataframe(data.get("package_rows", []), "package_name")
    country_df = _dimension_dataframe(data.get("country_rows", []), "country")

    daily_figure = set_transparent_chart_background(_build_daily_figure(daily_df))
    daily_figure.update_layout(height=440)
    with st.container(border=True):
        st.plotly_chart(daily_figure, width="stretch")

    package_figure = set_transparent_chart_background(
        _build_dimension_figure(package_df, label_column="package_name", title="Installers by Package")
    )
    country_figure = set_transparent_chart_background(
        _build_dimension_figure(country_df, label_column="country", title="Installers by Country")
    )
    package_figure.update_layout(height=460)
    country_figure.update_layout(height=460)
    for column, figure in zip(st.columns(2, gap="small"), [package_figure, country_figure]):
        with column:
            with st.container(border=True):
                st.plotly_chart(figure, width="stretch")

    st.markdown("### Install Details")
    details_df = _details_dataframe(data.get("details", []))
    if details_df.empty:
        st.info("No install data for selected date range.")
        return
    st.dataframe(
        details_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Installers": st.column_config.NumberColumn("Installers", format="%d"),
            "Uninstallers": st.column_config.NumberColumn("Uninstallers", format="%d"),
            "Net Installs": st.column_config.NumberColumn("Net Installs", format="%d"),
            "Active Devices": st.column_config.NumberColumn("Active Devices", format="%d"),
            "Churn Rate": st.column_config.NumberColumn("Churn Rate", format="%.2f%%"),
        },
    )
