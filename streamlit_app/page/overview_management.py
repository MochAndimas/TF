"""Overview Management analytics page."""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from streamlit_app.page.overview_components.management_compare import comparison_rows, add_comparison_line
from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.metrics import (
    _campaign_format_currency,
    _campaign_format_growth,
    _campaign_format_usd,
    _render_metric_with_growth,
)
from streamlit_app.page.overview_components.charts import (
    build_cost_to_deposit_ratio_figure,
    build_cost_vs_deposit_figure,
)
from streamlit_app.page.overview_components.formatting import convert_idr_to_usd, render_currency_toggle
from streamlit_app.page.campaign_components.common import (
    PAGE_STYLE,
    render_period_filter,
    set_transparent_chart_background,
)


async def show_overview_management_page(host: str) -> None:
    st.markdown(PAGE_STYLE, unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Overview Management</div>', unsafe_allow_html=True)
    dates = render_period_filter("overview_management")
    if dates is None:
        return
    start_date, end_date = dates
    with st.spinner("Fetching register data..."):
        result = await fetch_api_result(
            st=st,
            host=host,
            uri="deposit-revenue/register-total",
            params={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        )
    if not result.ok:
        st.error(result.message or "Failed to fetch the register total.")
        return

    with st.container(border=True, key="management_section_register"):
        st.markdown('<div class="metric-section-title">Register</div>', unsafe_allow_html=True)
        cards = (
            ("Total Register", "total_register", "growth_percentage"),
            ("Ads Register", "ads_register", "ads_growth_percentage"),
            ("Socmed Register", "socmed_register", "socmed_growth_percentage"),
            ("Organic Register", "organic_register", "organic_growth_percentage"),
        )
        for card, (label, total_key, growth_key) in zip(st.columns(len(cards), gap="small"), cards):
            total = result.data[total_key]
            growth = result.data[growth_key]
            with card, st.container(border=True):
                _render_metric_with_growth(
                    st,
                    label,
                    f"{total:,}" if total is not None else "—",
                    delta=_campaign_format_growth(growth, result.data) if growth is not None else None,
                )

        await _render_daily_register(result.data["daily_register"], start_date, end_date, host)
        _render_register_breakdown(result.data)

    with st.container(border=True, key="management_section_all_deposit"):
        st.markdown('<div class="metric-section-title">All Deposit</div>', unsafe_allow_html=True)
        _render_amount_qty_cards(result.data, "all_deposit", "Total Deposit", "Average Deposit")
        with st.container(border=True):
            comparison = await comparison_rows(host, dates, "all_deposit", ("daily_all_deposit",))
            _render_amount_qty_chart(result.data["daily_all_deposit"], "Daily All Deposit", comparison, dates)

    with st.container(border=True, key="management_section_all_subscription"):
        st.markdown('<div class="metric-section-title">All Subscription</div>', unsafe_allow_html=True)
        _render_amount_qty_cards(result.data, "all_subscription", "Total Subscription", "Average Subscription")
        with st.container(border=True):
            comparison = await comparison_rows(host, dates, "all_subscription", ("daily_all_subscription",))
            _render_amount_qty_chart(result.data["daily_all_subscription"], "Daily All Subscription", comparison, dates)

    with st.container(border=True, key="management_section_first_deposit"):
        _render_first_deposit(result.data)
        await _render_first_deposit_chart(result.data["daily_first_deposit"], host, dates)
        _render_first_deposit_breakdown(result.data)

    with st.container(border=True, key="management_section_first_subscription"):
        st.markdown('<div class="metric-section-title">First Subscription</div>', unsafe_allow_html=True)
        _render_amount_qty_cards(result.data, "first_subscription", "Total First Subscription", "Average First Subscription")
        with st.container(border=True):
            comparison = await comparison_rows(host, dates, "first_subscription", ("daily_first_subscription",))
            _render_amount_qty_chart(result.data["daily_first_subscription"], "Daily First Subscription", comparison, dates)

    with st.container(border=True, key="management_section_topup"):
        st.markdown('<div class="metric-section-title">Topup</div>', unsafe_allow_html=True)
        _render_amount_qty_cards(result.data, "topup", "Total Topup", "Average Topup")
        with st.container(border=True):
            comparison = await comparison_rows(host, dates, "topup", ("daily_topup",))
            _render_amount_qty_chart(result.data["daily_topup"], "Daily Topup", comparison, dates)

    with st.container(border=True, key="management_section_cost_to_revenue"):
        await _render_cost_to_revenue(result.data["cost_to_revenue"], host, dates)

async def _render_cost_to_revenue(data: dict, host, dates) -> None:
    st.markdown('<div class="metric-section-title">Cost to Revenue</div>', unsafe_allow_html=True)
    with st.container(horizontal=True, horizontal_alignment="center"):
        currency = render_currency_toggle("management_cost_revenue_currency")
    metrics = data["current_period"]["metrics"]
    cards = (
        ("Overall Cost", "cost_to_revenue_cost", "Ads spend plus daily allocations of monthly Tools Fixed Cost."),
        ("Overall Revenue", "revenue", "All Deposit Amount plus All Subscription Amount."),
        ("Cost to Revenue", "cost_to_revenue_pct", "Overall Cost ÷ Overall Revenue × 100%"),
    )
    for column, (label, key, help_text) in zip(st.columns(len(cards), gap="small"), cards):
        value = metrics[key]
        growth = data["growth_percentage"][key]
        display = "—"
        if key == "cost_to_revenue_pct":
            if value is not None:
                display = f"{value:,.2f}%"
        else:
            if currency == "USD":
                value = convert_idr_to_usd(value)
                display = _campaign_format_usd(value, compact=True)
                full = f"$ {value:,.2f}"
            else:
                display = _campaign_format_currency(value, compact=True)
                full = f"Rp {value:,.2f}"
            help_text = f"{full}. {help_text}"
        with column, st.container(border=True):
            _render_metric_with_growth(
                st, label, display, help=help_text,
                delta=_campaign_format_growth(growth, data) if growth is not None else None,
                delta_color="inverse" if key == "cost_to_revenue_pct" else "normal",
            )

    if data["missing_tools_months"]:
        months = ", ".join(data["missing_tools_months"])
        st.info(f"Tools Fixed Cost is unavailable for these months in the selected or comparison period: {months}.")

    rows = data["daily_rows"]
    ratio = build_cost_to_deposit_ratio_figure(rows, "Overall Revenue")
    ratio.update_layout(title="Cost to Revenue (%) Per Hari")
    if rows:
        ratio.data[0].y = [row["cost"] / row["revenue_idr"] * 100 if row["revenue_idr"] else None for row in rows]
        ratio.data[0].name = "Cost to Revenue"
        ratio.data[0].hovertemplate = "<b>%{x}</b><br>Cost to Revenue: %{y:.2f}%<extra></extra>"
    figures = (build_cost_vs_deposit_figure(rows, currency, "Overall Revenue"), ratio)
    for column, figure in zip(st.columns(2, gap="small"), figures):
        with column, st.container(border=True):
            if figure is ratio:
                comparison = await comparison_rows(host, dates, "cost_revenue_ratio", ("cost_to_revenue", "daily_rows"))
                # The existing chart uses date labels. Use actual dates for aligned overlays.
                figure.data[0].x = pd.to_datetime([row["date"] for row in rows])
                figure.update_xaxes(type="date")
                add_comparison_line(figure, comparison, dates, "revenue_idr", "Cost to Revenue", "#ff6248", ratio=True)
            else:
                comparison = await comparison_rows(host, dates, "cost_revenue_bars", ("cost_to_revenue", "daily_rows"))
                if comparison is not None:
                    for trace in figure.data:
                        trace.x = pd.to_datetime([row["date"] for row in rows])
                    figure.update_xaxes(type="date")
                    for field, name, color, axis in (
                        ("cost", "Cost", "#6176ff", "y"),
                        ("revenue_idr", "Overall Revenue", "#13c39c", "y2"),
                    ):
                        values = [row.get(field) for row in comparison]
                        if currency == "USD":
                            values = [convert_idr_to_usd(value) if value is not None else None for value in values]
                        figure.add_trace(go.Bar(
                            x=pd.date_range(dates[0], periods=len(comparison)),
                            y=values,
                            name=f"{name} ({currency}, comparison)",
                            marker_color=color,
                            opacity=0.4,
                            yaxis=axis,
                            offsetgroup=f"{field}_comparison",
                            customdata=[row["date"] for row in comparison],
                            hovertemplate="Comparison: %{customdata}<br>%{y:,.2f}<extra>%{fullData.name}</extra>",
                        ))
            st.plotly_chart(set_transparent_chart_background(figure), width="stretch")


def _render_first_deposit(data: dict) -> None:
    st.markdown('<div class="metric-section-title">First Deposit</div>', unsafe_allow_html=True)
    _render_amount_qty_cards(data, "first_deposit", "Total First Deposit", "Average First Deposit")
    _render_amount_qty_cards(data, "ads_first_deposit", "Ads First Deposit", "Average Ads First Deposit")
    _render_amount_qty_cards(data, "socmed_first_deposit", "Socmed First Deposit", "Average Socmed First Deposit")
    _render_amount_qty_cards(data, "organic_first_deposit", "Organic First Deposit", "Average Organic First Deposit")

    conversion = data["register_to_first_deposit"]
    rate = conversion["metrics"]["rate"]
    growth = conversion["growth_percentage"]["rate"]
    with st.container(border=True, key="register_to_first_deposit_card"):
        st.markdown("""
            <style>
            .st-key-register_to_first_deposit_card [data-testid="stMetric"] {
                align-items: center;
            }
            .st-key-register_to_first_deposit_card [data-testid="stMetricLabel"] {
                display: flex;
                width: fit-content;
                max-width: 100%;
                margin-inline: auto;
                justify-content: center;
            }
            .st-key-register_to_first_deposit_card .tf-growth-footer {
                justify-content: center;
            }
            .st-key-register_to_first_deposit_card [data-testid="stMetricValue"] {
                text-align: center;
            }
            </style>
            """, unsafe_allow_html=True)
        _render_metric_with_growth(
            st,
            "Register to First Deposit",
            f"{rate:.2%}" if rate is not None else "—",
            delta=_campaign_format_growth(growth, data) if growth is not None else None,
            help="Total First Deposit Qty ÷ Total Register",
        )


def _render_amount_qty_cards(data: dict, section: str, label_prefix: str, average_label: str) -> None:
    summary = data[section]
    cards = (
        (f"{label_prefix} Amount", "amount"),
        (f"{label_prefix} Qty", "qty"),
        (average_label, "average"),
    )
    for card, (label, key) in zip(st.columns(len(cards), gap="small"), cards):
        value = summary["metrics"][key]
        growth = summary["growth_percentage"][key]
        display = "—"
        if value is not None:
            display = f"{value:,}" if key == "qty" else _campaign_format_currency(value, compact=True)
        with card, st.container(border=True):
            _render_metric_with_growth(
                st,
                label,
                display,
                delta=_campaign_format_growth(growth, data) if growth is not None else None,
                help=f"Rp {value:,.2f}" if key != "qty" and value is not None else None,
            )


async def _render_first_deposit_chart(daily_deposits: dict, host, dates) -> None:
    with st.container(border=True):
        category = st.selectbox(
            "First Deposit Source",
            list(daily_deposits),
            format_func=lambda source: f"{source} First Deposit",
            key="management_first_deposit_source",
        )
        comparison = await comparison_rows(host, dates, "first_deposit", ("daily_first_deposit", category))
        _render_amount_qty_chart(daily_deposits[category], f"Daily {category} First Deposit", comparison, dates)


def _render_amount_qty_chart(rows: list, title: str, comparison=None, dates=None) -> None:
    if not rows:
        st.info("No data for the selected period.")
        return
    daily = pd.DataFrame(rows)
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(go.Bar(
        x=daily["date"], y=daily["amount"], name="Amount", marker_color="#6176ff",
        offsetgroup="current",
        hovertemplate="Rp %{y:,.2f}<extra>Amount</extra>",
    ), secondary_y=False)
    figure.add_trace(go.Scatter(
        x=daily["date"], y=daily["qty"], name="Qty", mode="lines+markers",
        line={"color": "#2dd4bf"},
        hovertemplate="%{y:,}<extra>Qty</extra>",
    ), secondary_y=True)
    if comparison is not None:
        figure.add_trace(go.Bar(
            x=pd.date_range(dates[0], periods=len(comparison)),
            y=[row.get("amount") for row in comparison],
            name="Amount (comparison)",
            marker_color="#6176ff",
            opacity=0.4,
            offsetgroup="comparison",
            customdata=[row["date"] for row in comparison],
            hovertemplate="Comparison: %{customdata}<br>Rp %{y:,.2f}<extra>Amount (comparison)</extra>",
        ), secondary_y=False)
    add_comparison_line(figure, comparison, dates, "qty", "Qty", "#2dd4bf", secondary_y=True)
    figure.update_layout(title=title, hovermode="x unified", barmode="group")
    figure.update_xaxes(type="date", title_text="Date")
    figure.update_yaxes(title_text="Amount (IDR)", tickprefix="Rp ", secondary_y=False)
    figure.update_yaxes(title_text="Qty", tickformat=",d", secondary_y=True)
    st.plotly_chart(set_transparent_chart_background(figure), width="stretch")


async def _render_daily_register(daily_register, start_date, end_date, host) -> None:
    with st.container(border=True):
        comparison = await comparison_rows(host, (start_date, end_date), "register", ("daily_register",))
        if not daily_register:
            st.info("No total register data for the selected period.")
            return

        daily = pd.DataFrame(daily_register)
        daily["date"] = pd.to_datetime(daily["date"])
        timeline = pd.date_range(start_date, end_date, freq="D")
        counts = daily.set_index("date")["total_register"].reindex(timeline)
        figure = go.Figure(go.Scatter(
            x=timeline,
            y=counts,
            mode="lines+markers",
            line={"color": "#6176ff"},
            hovertemplate="%{x|%d %b %Y}<br>Total Register: %{y:,}<extra></extra>",
        ))
        add_comparison_line(figure, comparison, (start_date, end_date), "total_register", "Total Register", "#6176ff")
        figure.update_layout(
            title="Daily Total Register",
            xaxis_title="Date",
            yaxis_title="Total Register",
            yaxis_rangemode="tozero",
            showlegend=comparison is not None,
        )
        st.plotly_chart(set_transparent_chart_background(figure), width="stretch")


def _render_register_breakdown(data: dict) -> None:
    labels = ["Ads Register", "Socmed Register", "Organic Register"]
    counts = [data["ads_register"], data["socmed_register"], data["organic_register"]]
    total = sum(count for count in counts if count is not None)
    can_plot = total > 0 and all(count is None or count >= 0 for count in counts)

    st.markdown("### Register Breakdown")
    table, chart = st.columns(2, gap="small")
    panel_height = 490
    with table, st.container(border=True, height=panel_height):
        st.dataframe(
            pd.DataFrame({
                "Category": labels,
                "Register": pd.array(counts, dtype="Int64"),
                "Share": [f"{count / total:.1%}" if can_plot and count is not None else "—" for count in counts],
            }),
            hide_index=True,
            width="stretch",
            column_config={"Register": st.column_config.NumberColumn(format="localized")},
        )

    with chart, st.container(border=True, height=panel_height):
        if not can_plot:
            st.info("A pie chart requires positive totals and non-negative register counts.")
            return
        _render_share_pie("Register Share", labels, counts, "Register: %{value:,}")


def _render_first_deposit_breakdown(data: dict) -> None:
    labels = ["Ads First Deposit", "Socmed First Deposit", "Organic First Deposit"]
    metrics = [data[key]["metrics"] for key in ("ads_first_deposit", "socmed_first_deposit", "organic_first_deposit")]
    amounts = [metric["amount"] for metric in metrics]
    total = sum(amount for amount in amounts if amount is not None)
    can_plot = amounts[-1] is not None and total > 0 and all(
        amount is None or amount >= 0 for amount in amounts
    )

    st.markdown("### First Deposit Breakdown")
    table, chart = st.columns(2, gap="small")
    with table, st.container(border=True, height=490):
        st.dataframe(
            pd.DataFrame({
                "Category": labels,
                "Qty": pd.array([metric["qty"] for metric in metrics], dtype="Int64"),
                "Amount (IDR)": amounts,
                "Share Amount": [
                    f"{amount / total:.1%}" if can_plot and amount is not None else "—"
                    for amount in amounts
                ],
            }).style.format({"Amount (IDR)": "Rp {:,.2f}"}, na_rep="—"),
            hide_index=True,
            width="stretch",
            column_config={
                "Qty": st.column_config.NumberColumn(format="localized"),
            },
        )

    with chart, st.container(border=True, height=490):
        if not can_plot:
            st.info("The pie chart needs a positive total and non-negative amounts.")
            return
        _render_share_pie("First Deposit Amount Share", labels, amounts, "Amount: Rp %{value:,.2f}")


def _render_share_pie(title: str, labels: list, values: list, hover_value: str) -> None:
    figure = go.Figure(go.Pie(
        labels=labels,
        values=[value if value is not None else 0 for value in values],
        sort=False,
        marker={"colors": ["#6176ff", "#2dd4bf", "#fbbf24"]},
        textinfo="percent",
        hovertemplate=f"%{{label}}<br>{hover_value}<br>Share: %{{percent}}<extra></extra>",
    ))
    figure.update_layout(title=title, height=450)
    st.plotly_chart(set_transparent_chart_background(figure), width="stretch")
