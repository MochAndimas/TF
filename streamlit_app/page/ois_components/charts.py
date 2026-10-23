"""Consistent Plotly views for OIS acquisition and engagement."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from streamlit_app.page.ois_components.analysis import aggregate, LABELS

COLORS = {"ois_id": "#6875f5", "ois.id": "#22b895"}


def style(figure, title):
    figure.update_layout(title=title, height=370, margin=dict(l=20, r=20, t=60, b=75),
                         paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         legend_title_text="", legend=dict(orientation="h", y=-.22))
    return figure


def trend(df, field, start, end):
    daily = aggregate(df, ["date"])
    figure = go.Figure()
    timeline = pd.date_range(start, end)
    series = daily.set_index("date")[field].reindex(timeline)
    figure.add_trace(go.Scatter(x=timeline, y=series, name=LABELS[field], mode="lines+markers",
                                connectgaps=False, line=dict(color="#6875f5", width=2.5)))
    figure.update_layout(showlegend=False)
    figure.update_layout(hovermode="x unified", yaxis_title=LABELS[field])
    return style(figure, LABELS[field] + " over time")


def contribution(df, dimension, field, title):
    grouped = aggregate(df, [dimension])
    order = grouped.groupby(dimension)[field].sum().nlargest(12).index
    grouped = grouped[grouped[dimension].isin(order)]
    figure = px.bar(grouped, x=field, y=dimension, orientation="h",
                    color_discrete_sequence=["#6875f5"], labels=LABELS,
                    category_orders={dimension: list(order)})
    return style(figure, title)


def mix(df, field):
    grouped = aggregate(df, ["first_user_campaign_name"])
    figure = px.pie(grouped, names="first_user_campaign_name", values=field,
                    color="first_user_campaign_name", labels=LABELS, hole=.65)
    figure.update_traces(textinfo="percent+label")
    return style(figure, LABELS[field] + " share by campaign")


def heatmap(df, field):
    daily = aggregate(df, ["date", "first_user_source"])
    daily["Weekday"] = daily.date.dt.day_name()
    matrix = daily.pivot_table(index="first_user_source", columns="Weekday", values=field, aggfunc="mean")
    matrix = matrix.reindex(columns=["Monday", "Tuesday", "Wednesday",
                            "Thursday", "Friday", "Saturday", "Sunday"])
    figure = go.Figure(go.Heatmap(x=matrix.columns, y=matrix.index, z=matrix.values,
                                  colorscale="Blues", hoverongaps=False,
                                  hovertemplate="%{y} · %{x}<br>Daily average: %{z:,.1f}<extra></extra>"))
    return style(figure, "Weekday pattern · average on observed days")


def quality(df):
    grouped = aggregate(df, ["first_user_source", "platform"])
    grouped = grouped[grouped.active_users > 0]
    figure = px.scatter(grouped, x="engagement_seconds_per_active_user_day", y="sessions_per_active_user_day",
                        size="active_users", color="first_user_source", hover_name="platform",
                        color_discrete_map=COLORS, labels=LABELS, size_max=50)
    return style(figure, "Engagement quality by source & platform")
