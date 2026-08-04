"""Streamlit page for Google Play Console and Apple App Store analytics."""

from __future__ import annotations

import datetime as dt
from html import escape

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


INSTALL_METRIC_CARD_STYLE = """
<style>
.install-metric-card {
    border: 1px solid rgba(250, 250, 250, 0.20);
    border-radius: 8px;
    padding: 17px 16px 16px;
    min-height: 108px;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
    background: rgba(0, 0, 0, 0);
}
.install-metric-card .metric-label {
    font-size: 14px;
    line-height: 1.25;
    color: rgba(250, 250, 250, 0.95);
    margin-bottom: 8px;
}
.install-metric-card .metric-value {
    font-size: 34px;
    line-height: 1.1;
    color: rgb(250, 250, 250);
    margin-bottom: 10px;
}
.install-metric-card .metric-delta {
    width: fit-content;
    min-height: 22px;
    border-radius: 999px;
    padding: 3px 9px;
    font-size: 14px;
    line-height: 16px;
}
.install-metric-card .metric-delta.positive {
    color: rgb(72, 207, 121);
    background: rgba(35, 134, 73, 0.38);
}
.install-metric-card .metric-delta.negative {
    color: rgb(255, 112, 112);
    background: rgba(248, 81, 73, 0.28);
}
.install-metric-card .metric-delta.neutral {
    color: rgba(250, 250, 250, 0.55);
    background: rgba(250, 250, 250, 0.10);
}
.install-metric-card .metric-delta.placeholder {
    visibility: hidden;
}
</style>
"""


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


def _render_metric_card(
    *,
    label: str,
    value: str,
    growth_value: float | None = None,
    delta_color: str = "normal",
) -> None:
    if growth_value is None:
        delta_class = "placeholder"
        delta_text = "&nbsp;"
    else:
        display_growth = -growth_value if delta_color == "inverse" else growth_value
        delta_class = "positive" if display_growth > 0 else "negative" if display_growth < 0 else "neutral"
        arrow = "&uarr;" if display_growth > 0 else "&darr;" if display_growth < 0 else "&rarr;"
        delta_text = f"{arrow} {escape(_campaign_format_growth(growth_value))}"

    st.markdown(
        f"""
        <div class="install-metric-card">
            <div class="metric-label">{escape(label)}</div>
            <div class="metric-value">{escape(value)}</div>
            <div class="metric-delta {delta_class}">{delta_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metrics(metrics: dict[str, object], all_time: dict[str, object] | None = None) -> None:
    st.markdown(INSTALL_METRIC_CARD_STYLE, unsafe_allow_html=True)
    current = metrics.get("current_period", {}).get("metrics", {})
    growth = metrics.get("growth_percentage", {})
    all_time = all_time or {}
    top_specs = [
        (
            "All-time Installers",
            _fmt_int(all_time.get("installers")),
            None,
            "normal",
        ),
        (
            "Active Devices",
            _fmt_int(current.get("active_devices")),
            growth.get("active_devices", 0.0),
            "normal",
        ),
    ]
    bottom_specs = [
        ("Installers", "installers", _fmt_int(current.get("installers"))),
        ("Uninstallers", "uninstallers", _fmt_int(current.get("uninstallers"))),
        ("Net Installs", "net_installs", _fmt_int(current.get("net_installs"))),
        ("Churn Rate", "churn_rate", f"{_fmt_float(current.get('churn_rate'))}%"),
    ]

    for column, (label, value, growth_value, delta_color) in zip(st.columns(2, gap="small"), top_specs):
        with column:
            _render_metric_card(
                label=label,
                value=value,
                growth_value=growth_value,
                delta_color=delta_color,
            )

    for column, (label, key, value) in zip(st.columns(4, gap="small"), bottom_specs):
        with column:
            _render_metric_card(
                label=label,
                value=value,
                growth_value=growth.get(key, 0.0),
                delta_color="inverse" if key in {"uninstallers", "churn_rate"} else "normal",
            )


def _daily_dataframe(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for column in ("installers", "uninstallers", "net_installs", "active_devices"):
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values("date")


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


def _growth_percentage(current: float, previous: float) -> float:
    if previous == 0:
        return 100.0 if current else 0.0
    return round(((current - previous) / previous) * 100, 2)


def _install_values(installers: int, uninstallers: int, active_devices: int) -> dict[str, float]:
    return {
        "installers": installers,
        "uninstallers": uninstallers,
        "net_installs": installers - uninstallers,
        "active_devices": active_devices,
        "churn_rate": round((uninstallers / installers) * 100, 2) if installers else 0.0,
    }


def _normalize_apple_data(apple: dict[str, object]) -> dict[str, object]:
    source_metrics = apple.get("metrics", {})

    def normalize_period(period: dict[str, object]) -> dict[str, object]:
        values = period.get("metrics", {})
        normalized = _install_values(
            int(values.get("total_downloads", 0)),
            int(values.get("deletions", 0)),
            int(values.get("active_devices", 0)),
        )
        return {**period, "metrics": normalized}

    current = normalize_period(source_metrics.get("current_period", {}))
    previous = normalize_period(source_metrics.get("previous_period", {}))
    growth = {
        key: _growth_percentage(float(current["metrics"][key]), float(previous["metrics"][key]))
        for key in current["metrics"]
    }
    daily = pd.DataFrame(apple.get("daily_rows", []))
    if not daily.empty:
        daily = daily.rename(columns={"total_downloads": "installers", "deletions": "uninstallers"})
        daily["net_installs"] = daily["installers"] - daily["uninstallers"]
        daily["churn_rate"] = daily.apply(
            lambda row: round((row["uninstallers"] / row["installers"]) * 100, 2) if row["installers"] else 0.0,
            axis=1,
        )
    rows = daily.to_dict(orient="records")
    return {
        "metrics": {"current_period": current, "previous_period": previous, "growth_percentage": growth},
        "all_time": {"installers": apple.get("all_time", {}).get("total_downloads", 0)},
        "daily_rows": rows,
        "details": rows,
    }


def _combine_install_data(play: dict[str, object], apple: dict[str, object]) -> dict[str, object]:
    normalized_apple = _normalize_apple_data(apple)

    def combined_period(period_key: str) -> dict[str, object]:
        play_period = play.get("metrics", {}).get(period_key, {})
        apple_period = normalized_apple.get("metrics", {}).get(period_key, {})
        play_values = play_period.get("metrics", {})
        apple_values = apple_period.get("metrics", {})
        metrics = _install_values(
            int(play_values.get("installers", 0)) + int(apple_values.get("installers", 0)),
            int(play_values.get("uninstallers", 0)) + int(apple_values.get("uninstallers", 0)),
            int(play_values.get("active_devices", 0)) + int(apple_values.get("active_devices", 0)),
        )
        return {**play_period, "metrics": metrics}

    current = combined_period("current_period")
    previous = combined_period("previous_period")
    growth = {
        key: _growth_percentage(float(current["metrics"][key]), float(previous["metrics"][key]))
        for key in current["metrics"]
    }
    frames = []
    for rows in (play.get("daily_rows", []), normalized_apple.get("daily_rows", [])):
        if rows:
            frames.append(pd.DataFrame(rows))
    if frames:
        daily = pd.concat(frames, ignore_index=True)
        daily = daily.groupby("date", as_index=False).agg(
            installers=("installers", "sum"),
            uninstallers=("uninstallers", "sum"),
            active_devices=("active_devices", "sum"),
        )
        daily["net_installs"] = daily["installers"] - daily["uninstallers"]
        daily["churn_rate"] = daily.apply(
            lambda row: round((row["uninstallers"] / row["installers"]) * 100, 2) if row["installers"] else 0.0,
            axis=1,
        )
        rows = daily.sort_values("date").to_dict(orient="records")
    else:
        rows = []
    return {
        "metrics": {"current_period": current, "previous_period": previous, "growth_percentage": growth},
        "all_time": {
            "installers": int(play.get("all_time", {}).get("installers", 0))
            + int(normalized_apple.get("all_time", {}).get("installers", 0))
        },
        "daily_rows": rows,
        "details": rows,
    }


def _overview_platform_frame(play: dict[str, object], apple: dict[str, object]) -> pd.DataFrame:
    frames = []
    for platform, rows in (
        ("Google Play", play.get("daily_rows", [])),
        ("Apple App Store", _normalize_apple_data(apple).get("daily_rows", [])),
    ):
        if not rows:
            continue
        frame = pd.DataFrame(rows)
        frame["platform"] = platform
        frames.append(frame)
    if not frames:
        return pd.DataFrame(
            columns=["date", "platform", "installers", "uninstallers", "net_installs", "active_devices", "churn_rate"]
        )
    frame = pd.concat(frames, ignore_index=True)
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    for column in ("installers", "uninstallers", "net_installs", "active_devices"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0).astype(int)
    frame["churn_rate"] = pd.to_numeric(frame["churn_rate"], errors="coerce").fillna(0.0)
    return frame.sort_values(["date", "platform"])


def _build_overview_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Daily Installs - All Platforms",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    colors = {"Google Play": "#4C78FF", "Apple App Store": "#FF4B4B"}
    for platform in ("Google Play", "Apple App Store"):
        subset = df[df["platform"] == platform]
        if subset.empty:
            continue
        labels = _date_labels(subset["date"])
        figure.add_trace(
            go.Bar(
                x=labels,
                y=subset["installers"],
                name=f"{platform} Installers",
                marker_color=colors[platform],
                offsetgroup="installers",
                legendgroup=platform,
            )
        )
        figure.add_trace(
            go.Bar(
                x=labels,
                y=subset["uninstallers"],
                name=f"{platform} Uninstallers",
                marker_color=colors[platform],
                marker_pattern_shape="/",
                offsetgroup="uninstallers",
                legendgroup=platform,
            )
        )
    figure.update_layout(
        title="Daily Installers & Uninstallers - All Platforms",
        barmode="relative",
        xaxis=dict(title="Date", type="category"),
        yaxis=dict(title="Installs"),
        legend=dict(orientation="h", y=1.18, x=0),
    )
    return figure


def _build_active_device_trend_figure(df: pd.DataFrame) -> go.Figure:
    figure = go.Figure()
    if df.empty:
        figure.update_layout(
            title="Active Devices Trend (First Available Day = 100)",
            annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
        )
        return figure

    colors = {"Google Play": "#4C78FF", "Apple App Store": "#FF4B4B"}
    for platform in ("Google Play", "Apple App Store"):
        subset = df[df["platform"] == platform].sort_values("date").copy()
        if subset.empty:
            continue
        non_zero = subset.loc[subset["active_devices"] > 0, "active_devices"]
        baseline = float(non_zero.iloc[0]) if not non_zero.empty else 0.0
        subset["active_device_index"] = (
            subset["active_devices"].astype(float).div(baseline).mul(100)
            if baseline
            else 0.0
        )
        figure.add_trace(
            go.Scatter(
                x=_date_labels(subset["date"]),
                y=subset["active_device_index"],
                customdata=subset[["active_devices"]].values,
                name=platform,
                mode="lines+markers",
                line=dict(color=colors[platform], width=3),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    + "Trend Index: %{y:.2f}<br>"
                    + "Active Devices: %{customdata[0]:,}<extra></extra>"
                ),
            )
        )
    figure.add_hline(y=100, line_dash="dot", line_color="rgba(255,255,255,0.35)")
    figure.update_layout(
        title="Active Devices Trend (First Available Day = 100)",
        xaxis=dict(title="Date", type="category"),
        yaxis=dict(title="Trend Index"),
        legend=dict(orientation="h", y=1.15, x=0),
    )
    return figure


def _render_overview(play: dict[str, object], apple: dict[str, object]) -> None:
    normalized_apple = _normalize_apple_data(apple)
    st.markdown("### Google Play")
    _render_metrics(play.get("metrics", {}), play.get("all_time", {}))
    st.markdown("### Apple App Store")
    _render_metrics(normalized_apple.get("metrics", {}), normalized_apple.get("all_time", {}))

    overview_df = _overview_platform_frame(play, apple)
    figure = set_transparent_chart_background(_build_overview_figure(overview_df))
    figure.update_layout(height=460)
    with st.container(border=True):
        st.plotly_chart(figure, width="stretch")

    trend_figure = set_transparent_chart_background(_build_active_device_trend_figure(overview_df))
    trend_figure.update_layout(height=420)
    with st.container(border=True):
        st.plotly_chart(trend_figure, width="stretch")

    st.markdown("### Install Details - All Platforms")
    if overview_df.empty:
        st.info("No install data for selected date range.")
        return
    details = overview_df.rename(
        columns={
            "date": "Date",
            "platform": "Platform",
            "installers": "Installers",
            "uninstallers": "Uninstallers",
            "net_installs": "Net Installs",
            "active_devices": "Active Devices",
            "churn_rate": "Churn Rate",
        }
    )
    details = details[["Date", "Platform", "Installers", "Uninstallers", "Net Installs", "Active Devices", "Churn Rate"]]
    st.dataframe(
        details,
        width="stretch",
        hide_index=True,
        column_config={
            "Date": st.column_config.DateColumn("Date"),
            "Platform": st.column_config.TextColumn("Platform"),
            "Installers": st.column_config.NumberColumn("Installers", format="%d"),
            "Uninstallers": st.column_config.NumberColumn("Uninstallers", format="%d"),
            "Net Installs": st.column_config.NumberColumn("Net Installs", format="%d"),
            "Active Devices": st.column_config.NumberColumn("Active Devices", format="%d"),
            "Churn Rate": st.column_config.NumberColumn("Churn Rate", format="%.2f%%"),
        },
    )


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


def _render_install_analytics(data: dict[str, object], *, title: str) -> None:
    _render_metrics(data.get("metrics", {}), data.get("all_time", {}))
    daily_df = _daily_dataframe(data.get("daily_rows", []))
    daily_figure = set_transparent_chart_background(_build_daily_figure(daily_df))
    daily_figure.update_layout(title=f"Daily Installs - {title}", height=440)
    with st.container(border=True):
        st.plotly_chart(daily_figure, width="stretch")

    st.markdown(f"### Install Details - {title}")
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

    platform = st.segmented_control(
        "Platform view",
        options=["Overview", "Google Play", "Apple App Store"],
        default="Overview",
        key="install_platform_view",
    ) or "Overview"

    package_name, country = "all", "all"

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
    if platform == "Apple App Store":
        _render_install_analytics(_normalize_apple_data(data.get("apple", {})), title="Apple App Store")
    elif platform == "Overview":
        _render_overview(data, data.get("apple", {}))
    else:
        _render_install_analytics(data, title="Google Play")
