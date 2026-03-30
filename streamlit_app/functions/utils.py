"""Backward-compatible utility barrel for Streamlit pages."""

from streamlit_app.functions.account_modals import add_account_modal, edit_account_modal
from streamlit_app.functions.accounts import get_accounts, is_valid_email
from streamlit_app.functions.api import fetch_data, logout
from streamlit_app.functions.charting import campaign_figure_from_payload
from streamlit_app.functions.dates import campaign_preset_ranges, get_date_range
from streamlit_app.functions.metrics import (
    _campaign_convert_idr_to_usd,
    _campaign_format_compact_number,
    _campaign_format_currency,
    _campaign_format_growth,
    _campaign_format_number,
    _campaign_format_usd,
    _campaign_growth_from_periods,
    _campaign_metric_value,
    _render_hover_metric_card,
    render_brand_awareness_metric_cards,
    render_campaign_metric_cards,
    render_overview_cost_metric_cards,
    render_overview_leads_metric_cards,
    render_overview_metric_cards,
)
from streamlit_app.functions.runtime import (
    apply_auth_payload,
    clear_auth_state,
    consume_auth_bridge_response,
    get_access_token,
    refresh_backend_tokens,
    resolve_backend_base_url,
    start_auth_bridge_request,
)
from streamlit_app.functions.ui import footer
