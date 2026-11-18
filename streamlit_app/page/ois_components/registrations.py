"""Registration-cohort analysis of OIS GSheet records."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.chart_comparison import ChartComparison
from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.metrics import _render_metric_with_growth, _campaign_format_growth, _campaign_format_number
from streamlit_app.page.ois_components.charts import COLORS, style
from streamlit_app.page.ois_components.contacts import render_contacts

FIELDS = ["tf_id", "registration_date", "tgl_regis", "utm_source", "utm_medium", "utm_campaign",
          "user_status", "assign_date", "first_assign_date", "first_subs", "first_subs_idr",
          "last_subs", "last_subs_nominal_idr", "cabang", "status", "pool", "last_activity", "pull_date"]
LABELS = {"registration_date": "Register Date", "utm_source": "Source", "utm_medium": "Medium",
          "utm_campaign": "Campaign", "cabang": "Branch", "user_status": "User Status",
          "register": "Register", "subscribers": "First Subscribers", "conversion": "Register to First Subs (%)",
          "first_revenue": "First Subs Revenue (IDR)", "last_revenue": "Last Subs Snapshot (IDR)",
          "average_first_revenue": "Average First Subs (IDR)"}


def prepare(rows):
    df = pd.DataFrame(rows, columns=FIELDS)
    df["registration_date"] = pd.to_datetime(df.registration_date)
    for field in ("first_subs_idr", "last_subs_nominal_idr"):
        df[field] = pd.to_numeric(df[field], errors="coerce").fillna(0)
    df["subscribed"] = pd.to_datetime(df.first_subs, errors="coerce").notna()
    for field in ("utm_medium", "utm_campaign", "cabang", "user_status"):
        df[field] = df[field].fillna("(not set)")
    return df


def summarize(df):
    registers = int(df.tf_id.nunique())
    subscribers = int(df.loc[df.subscribed, "tf_id"].nunique())
    revenue = float(df.first_subs_idr.sum())
    return dict(register=registers, subscribers=subscribers, conversion=subscribers / registers * 100 if registers else 0,
                first_revenue=revenue, last_revenue=float(df.last_subs_nominal_idr.sum()),
                average_first_revenue=revenue / subscribers if subscribers else 0)


def breakdown(df, fields):
    result = []
    for key, group in df.groupby(fields, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        result.append({**dict(zip(fields, key)), **summarize(group)})
    return pd.DataFrame(result, columns=[*fields, *summarize(df)])


async def chart(figure, key, compare):
    with st.container(border=True):
        await compare.render(figure, width="stretch", key=key)


async def show_register_revenue(host, dates, *, new_users=None):
    with st.spinner("Loading OIS register & revenue..."):
        result = await fetch_api_result(st=st, host=host, uri="ois/registrations", params={
            "start_date": dates[0].isoformat(), "end_date": dates[1].isoformat()})
    if not result.ok:
        st.error(result.message or "Unable to load register & revenue.")
        return
    data = result.data
    current, previous = prepare(data["current_rows"]), prepare(data["previous_rows"])
    if current.empty:
        st.info("No registrations for this period. Sync OIS Registrations (GSheet) in Update Data or choose another period.")
        return
    combined = pd.concat([current, previous], ignore_index=True)
    selections = {}
    with st.container(border=True):
        for column, field in zip(st.columns(2, gap="small"), ("utm_medium", "utm_campaign")):
            options = sorted(combined[field].dropna().unique().tolist())
            key = f"ois_reg_filter_{field}"
            st.session_state[key] = [value for value in st.session_state.get(key, []) if value in options]
            selected = column.multiselect(LABELS[field], options, placeholder="All", key=key)
            selections[field] = selected
            if selected:
                current = current[current[field].isin(selected)]
                previous = previous[previous[field].isin(selected)]
    if current.empty:
        st.info("No registrations match these filters.")
        return
    compare = ChartComparison(host, dates, "ois_registrations", "ois/registrations", data)
    def comparison_figure(payload, period, kind):
        records = prepare(payload["current_rows"])
        for field, values in selections.items():
            if values:
                records = records[records[field].isin(values)]
        return registration_figure(records, period, kind)
    st.markdown("## Register & Revenue Overview")
    now, before = summarize(current), summarize(previous)
    comparison = {"previous_period": {"start_date": data["previous_start"], "end_date": data["previous_end"]}}
    specs = ["register", "subscribers", "conversion",
             "first_revenue", "average_first_revenue"]
    for row in (specs[:3], specs[3:]):
        for column, field in zip(st.columns(len(row), gap="small"), row):
            value = f"{now[field]:.2f}%" if field == "conversion" else (
                f"Rp {now[field]:,.2f}" if 'revenue' in field else _campaign_format_number(now[field]))
            change = (now[field] / before[field] - 1) * 100 if before[field] else None
            with column, st.container(border=True):
                _render_metric_with_growth(st, LABELS[field], value, delta=_campaign_format_growth(change, comparison),
                                           help="Users grouped by registration date; subscription values reflect the latest synced record." if 'revenue' in field else None)
    if new_users is not None:
        st.markdown("## Acquisition Funnel")
        figure = go.Figure(go.Funnel(
            y=["New users", "Register"], x=[new_users, now["register"]],
            textinfo="value", marker=dict(color=["#6875f5", "#22b895"]),
            hovertemplate="%{y}: %{x:,.0f}<extra></extra>"))
        await chart(style(figure, "New users → Register"), "ois_acquisition_funnel", compare)
        st.caption("New users dari GA4 dan register dari data internal pada periode yang dipilih. "
                   "Masing-masing mengikuti filter bagiannya; angka ini bukan pelacakan user yang sama antar tahap.")
    st.markdown("## Registration Trends")
    daily = breakdown(current, ["registration_date", "utm_source"])
    for kind, key in (("trend", "ois_reg_trend"), ("revenue", "ois_reg_revenue"), ("campaign", "ois_reg_utm_campaign")):
        if kind == "revenue":
            st.markdown("## Subscription Revenue")
        elif kind == "campaign":
            st.markdown("## Acquisition Breakdown")
        figure = registration_figure(current, dates, kind)
        object.__setattr__(figure, "_comparison_builder",
                           lambda data, dates, kind=kind: comparison_figure(data, dates, kind))
        await chart(figure, key, compare)
    with st.expander("Daily Register Details"):
        daily = daily.sort_values("registration_date", ascending=False)
        st.dataframe(daily.rename(columns=LABELS), hide_index=True, width="stretch", column_config={
            "Register Date": st.column_config.DatetimeColumn(format="DD MMM YYYY")})

    render_contacts(data.get("contact_rows", []), set(current.tf_id), dates)


def registration_figure(records, dates, kind):
    daily = breakdown(records, ["registration_date", "utm_source"])
    if kind == "trend":
        figure = go.Figure()
        timeline = pd.date_range(*dates)
        for source in records.utm_source.unique():
            series = daily[daily.utm_source == source].set_index("registration_date")["register"].reindex(timeline, fill_value=0)
            figure.add_trace(go.Scatter(x=timeline, y=series, name=source,
                                       mode="lines+markers", line=dict(color=COLORS[source])))
        figure.update_layout(hovermode="x unified", yaxis_title="Register", yaxis_rangemode="tozero")
        return style(figure, "Daily Register")
    if kind == "revenue":
        figure = px.bar(daily, x="registration_date", y="first_revenue",
                        color="utm_source", color_discrete_map=COLORS, labels=LABELS)
        return style(figure, "First Subs Revenue by Register Date")
    grouped = breakdown(records, ["utm_campaign"]).sort_values("register", ascending=False).head(12)
    figure = px.bar(grouped, x="utm_campaign", y="register", labels=LABELS,
                    color_discrete_sequence=["#6875f5"])
    return style(figure, "Register by Campaign")
