"""Revenue / Deposit: daily aggregate deposit analysis from ALL DEPO."""
import datetime as dt

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from streamlit_app.functions.api import fetch_api_result
from streamlit_app.functions.comparison import select_comparison
from streamlit_app.functions.dates import campaign_preset_ranges
from streamlit_app.functions.metrics import _render_metric_with_growth, _campaign_format_growth, _campaign_format_currency
from streamlit_app.page.campaign_components.common import PAGE_STYLE, set_transparent_chart_background

LABELS = {
    'register_qty': 'Register',
    'total_deposit_qty': 'Total Deposit Qty',
    'total_deposit_user_qty': 'Daily Unique Deposit Users',
    'total_deposit_amount': 'Total Deposit Amount',
    'total_deposit_auto_closing_qty': 'Auto Closing Qty',
    'total_deposit_auto_closing_user_qty': 'Auto Closing Users (Daily Sum)',
    'total_deposit_auto_closing_amount': 'Auto Closing Amount',
    'total_deposit_consultant_qty': 'Consultant Qty',
    'total_deposit_consultant_user_qty': 'Consultant Users (Daily Sum)',
    'total_deposit_consultant_amount': 'Consultant Amount',
    'first_deposit_qty': 'Total First Deposit Qty',
    'first_deposit_amount': 'Total First Deposit Amount',
    'first_deposit_auto_closing_qty': 'First Deposit Auto Closing Qty',
    'first_deposit_auto_closing_amount': 'First Deposit Auto Closing Amount',
    'first_deposit_consultant_qty': 'First Deposit Consultant Qty',
    'first_deposit_consultant_amount': 'First Deposit Consultant Amount',
    'average_deposit': 'Average Deposit',
    'average_first_deposit': 'Average First Deposit',
    'top_up_amount': 'Total Top Up Amount',
    'average_top_up': 'Average Top Up',
    'top_up_qty': 'Total Top Up Qty',
}
COLORS = ['#6875f5', '#22b895', '#e6ae54', '#ef6262']
FIELD_COLORS = {
    'total_deposit_amount': COLORS[0],
    'total_deposit_qty': COLORS[0],
    'average_deposit': COLORS[0],
    'first_deposit_amount': COLORS[1],
    'first_deposit_qty': COLORS[1],
    'average_first_deposit': COLORS[1],
    'top_up_amount': COLORS[2],
    'top_up_qty': COLORS[2],
    'average_top_up': COLORS[2],
    'total_deposit_user_qty': COLORS[3],
}

DEPOSIT_REVENUE_STYLE = """
<style>
[data-testid="stMetricLabel"] {
    align-items: flex-start !important;
    min-height: 2.4em !important;
}
[data-testid="stMetricLabel"] > div,
[data-testid="stMetricLabel"] [data-testid="stMarkdownContainer"] {
    width: 100% !important;
    overflow: hidden !important;
    white-space: normal !important;
    text-overflow: clip !important;
}
[data-testid="stMetricLabel"] p {
    display: -webkit-box !important;
    margin: 0 !important;
    overflow: hidden !important;
    white-space: normal !important;
    text-overflow: clip !important;
    line-height: 1.2 !important;
    overflow-wrap: anywhere !important;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 2;
}
</style>
"""


def render_filters():
    presets = campaign_preset_ranges(dt.date.today())
    st.session_state.setdefault('deposit_revenue_period', 'This Month')
    st.session_state.setdefault('deposit_revenue_dates', presets['This Month'])
    with st.container(border=True):
        selected = st.selectbox('Periods', list(presets), key='deposit_revenue_period')
        select_comparison(selected)
        if selected == 'Custom Range':
            dates = st.date_input('Select Date Range', key='deposit_revenue_dates')
            if not isinstance(dates, tuple) or len(dates) != 2:
                st.info('Select both a start and end date.')
                return None
        else:
            dates = presets[selected]
            st.session_state['deposit_revenue_dates'] = dates
    if dates[0] > dates[1]:
        st.warning('Start date cannot be after end date.')
        return None
    return dates


def money(value):
    return '—' if value is None else f'Rp {value:,.2f}'


def daily_frame(data):
    frame = pd.DataFrame(data['daily_rows'])
    frame['date'] = pd.to_datetime(frame['date'])
    period = data['current_period']
    frame = frame.set_index('date').reindex(pd.date_range(period['start_date'], period['end_date'])).rename_axis('date').reset_index()
    frame['top_up_amount'] = frame['total_deposit_amount'] - frame['first_deposit_amount']
    frame['top_up_qty'] = frame['total_deposit_qty'] - frame['first_deposit_qty']
    for prefix, name in [('total_deposit', 'average_deposit'), ('first_deposit', 'average_first_deposit')]:
        frame[name] = frame[f'{prefix}_amount'] / frame[f'{prefix}_qty'].replace(0, float('nan'))
    frame['average_top_up'] = frame['top_up_amount'] / frame['top_up_qty'].replace(0, float('nan'))
    return frame


def trend_figure(frame, fields, title, *, bars=False, amount=False):
    figure = go.Figure()
    for field in fields:
        color = FIELD_COLORS.get(field, COLORS[0])
        label = 'Deposit Users ( Unique )' if field == 'total_deposit_user_qty' else LABELS[field]
        values = dict(x=frame['date'], y=frame[field], name=label,
                      hovertemplate='%{x|%d %b %Y}<br>' + ('Rp %{y:,.2f}' if amount else '%{y:,.0f}') + '<extra>%{fullData.name}</extra>')
        if bars:
            figure.add_trace(go.Bar(**values, marker_color=color))
        else:
            figure.add_trace(go.Scatter(**values, mode='lines+markers', line=dict(color=color, width=2), connectgaps=False))
    figure.update_layout(title=title, height=370, barmode='group', hovermode='x unified',
        margin=dict(l=20,r=20,t=60,b=75), legend=dict(orientation='h',y=-.22),
        yaxis=dict(title='Amount (IDR)' if amount else 'Count', rangemode='tozero'), xaxis_title=None)
    return set_transparent_chart_background(figure)


def closing_breakdown(metrics, prefix, measure='amount'):
    total = metrics[f'{prefix}_{measure}']
    amounts = [metrics[f'{prefix}_auto_closing_{measure}'], metrics[f'{prefix}_consultant_{measure}']]
    labels = ['Auto Closing', 'Consultant']
    if total is None or any(value is None for value in amounts):
        return None
    remainder = total - sum(amounts)
    if remainder < -0.01:
        return None
    if remainder > 0.01:
        labels.append('Unclassified')
        amounts.append(remainder)
    return labels, amounts


def render_report(data):
    if not data.get('daily_rows'):
        st.info('No deposit data for this period. Run All Depo from Update Data to populate this report.')
        return
    totals = data['current_period']['metrics']
    st.markdown('## Deposit Overview')
    card_rows = [
        ['total_deposit_amount', 'first_deposit_amount', 'top_up_amount'],
        ['average_deposit', 'average_first_deposit', 'average_top_up'],
        ['total_deposit_qty', 'first_deposit_qty', 'top_up_qty'],
    ]
    for cards in card_rows:
        for col, key in zip(st.columns(len(cards), gap='small'), cards):
            with col, st.container(border=True):
                value = totals[key]
                amount = key.endswith('_amount') or key.startswith('average_')
                _render_metric_with_growth(st, LABELS[key], (_campaign_format_currency(value, compact=True) if value is not None else '—') if amount else ('—' if value is None else f'{value:,.0f}'),
                    delta=_campaign_format_growth(data['growth_percentage'].get(key), data),
                    help='Sum of daily depositing-user counts. A returning user can count on multiple days.' if key=='total_deposit_user_qty' else money(value) if amount else None)
    frame = daily_frame(data)
    st.markdown('## Deposit Trends')
    with st.container(border=True):
        st.plotly_chart(trend_figure(frame, ('total_deposit_amount','first_deposit_amount','top_up_amount'),
            'Daily Deposit Amount', bars=True, amount=True), width='stretch')
    cols = st.columns(2, gap='small')
    for col, fields, title, amount in [
        (cols[0], ('total_deposit_user_qty','total_deposit_qty','first_deposit_qty','top_up_qty'), 'Daily Deposit Activity', False),
        (cols[1], ('average_deposit','average_first_deposit','average_top_up'), 'Average Amount per Deposit', True),
    ]:
        with col, st.container(border=True):
            st.plotly_chart(trend_figure(frame,fields,title,amount=amount),width='stretch')
    st.markdown('## Closing Method')
    closing_cards = [
        [
            ('total_deposit_auto_closing_qty', 'Auto Closing Deposit Qty'),
            ('total_deposit_consultant_qty', 'Close With Consultant Deposit Qty'),
            ('total_deposit_auto_closing_user_qty', 'Auto Closing Deposit Daily Unique Users'),
            ('total_deposit_consultant_user_qty', 'Close With Consultant Deposit Daily Unique Users'),
        ],
        [
            ('first_deposit_auto_closing_qty', 'Auto Closing First Deposit Qty'),
            ('first_deposit_consultant_qty', 'Close With Consultant Deposit First Deposit Qty'),
        ],
    ]
    for row in closing_cards:
        for col, (key, label) in zip(st.columns(len(row), gap='small'), row):
            with col, st.container(border=True):
                value = totals[key]
                amount = key.endswith('_amount')
                _render_metric_with_growth(
                    st, label,
                    _campaign_format_currency(value, compact=True) if amount else ('—' if value is None else f'{value:,.0f}'),
                    delta=_campaign_format_growth(data['growth_percentage'].get(key), data),
                    help=money(value) if amount else None,
                )
    pie_specs = [
        ('total_deposit', 'amount', 'Total Deposit Amount'),
        ('first_deposit', 'amount', 'First Deposit Amount'),
    ]
    for col, (prefix, measure, title) in zip(st.columns(2,gap='small'), pie_specs):
        with col, st.container(border=True):
            st.markdown(f'#### {title}')
            breakdown = closing_breakdown(totals,prefix,measure)
            if breakdown is None:
                st.warning('Closing-method data is missing or exceeds the reported total. Update both All Depo and First Depo Revenue.')
            elif sum(breakdown[1]) == 0:
                st.info('No deposit data available for this period.')
            else:
                fig = go.Figure(go.Pie(labels=breakdown[0],values=breakdown[1],hole=.65,marker_colors=COLORS,
                    textinfo='percent',hovertemplate='%{label}<br>Rp %{value:,.2f}<br>%{percent}<extra></extra>'))
                fig.update_layout(height=310,margin=dict(l=10,r=10,t=15,b=45),legend=dict(orientation='h',y=-.1))
                st.plotly_chart(set_transparent_chart_background(fig),width='stretch',key=f'deposit_closing_{prefix}_{measure}')
    methods = []
    for key,label in [('auto_closing','Auto Closing'),('consultant','Consultant')]:
        methods.append({'Method':label,'Total Qty':totals[f'total_deposit_{key}_qty'],
            'Daily User Sum':totals[f'total_deposit_{key}_user_qty'],
            'Total Amount (IDR)':totals[f'total_deposit_{key}_amount'],
            'First Deposit Qty':totals[f'first_deposit_{key}_qty'],
            'First Deposit Amount (IDR)':totals[f'first_deposit_{key}_amount']})
    st.dataframe(pd.DataFrame(methods).style.format({'Total Amount (IDR)':'Rp {:,.2f}','First Deposit Amount (IDR)':'Rp {:,.2f}'}, na_rep='—'),hide_index=True,width='stretch')
    st.markdown('## Daily Details')
    details = pd.DataFrame(data['daily_rows']).sort_values('date',ascending=False)
    details['date'] = pd.to_datetime(details['date']).dt.date
    display = details.rename(columns={'date':'Date','pull_date':'Last Updated',**LABELS})
    with st.container(border=True):
        st.dataframe(display.style.format({LABELS[key]:'Rp {:,.2f}' for key in details.columns if key.endswith('_amount')}, na_rep='—'),
            hide_index=True,width='stretch',column_config={'Date':st.column_config.DateColumn(format='DD MMM YYYY')})


async def show_deposit_revenue_page(host: str):
    st.markdown(PAGE_STYLE + DEPOSIT_REVENUE_STYLE,unsafe_allow_html=True)
    st.markdown('<div class="campaign-title">Deposit</div>',unsafe_allow_html=True)
    dates = render_filters()
    if dates is None:
        return
    with st.spinner('Fetching deposit analytics...'):
        result = await fetch_api_result(st=st,host=host,uri='deposit-revenue/analytics',
            params={'start_date':dates[0].isoformat(),'end_date':dates[1].isoformat()})
    if not result.ok:
        st.error(result.message or 'Failed to fetch deposit analytics.')
        return
    render_report(result.data)
