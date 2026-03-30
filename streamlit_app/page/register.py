"""Register module.

This module is part of `streamlit_app.page` and contains runtime logic used by the
Traders Family application.
"""

import streamlit as st

from streamlit_app.functions.account_modals import add_account_modal, edit_account_modal
from streamlit_app.functions.accounts import get_accounts


TABLE_STYLE = """
<style>
.tf-admin-title {
    font-size: 1.45rem;
    font-weight: 700;
    margin: 0;
    color: var(--text-color);
}
.tf-admin-subtitle {
    color: var(--text-color);
    opacity: 0.82;
    margin-top: 0.15rem;
    margin-bottom: 0.4rem;
}
.table-divider {
    border-bottom: 1px solid #3f4b5f;
    margin-top: 10px;
    margin-bottom: 10px;
    width: 100%;
}
.header-text {
    font-weight: 600;
    opacity: 0.9;
}
</style>
"""


def _render_page_header(host: str, token) -> None:
    """Render the admin page header and primary create-account action.

    Args:
        host (str): Backend base URL forwarded to the modal helper.
        token: Authenticated session token object used by account actions.

    Returns:
        None: Writes header content and may open the create-account modal.
    """
    left_col, _, _, right_col = st.columns([4, 2, 2, 4], gap="xxlarge", vertical_alignment="center")
    with left_col:
        st.markdown('<p class="tf-admin-title">Account Management</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="tf-admin-subtitle">Create, review, and maintain dashboard user accounts.</p>',
            unsafe_allow_html=True,
        )
    with right_col:
        if st.button("Create Account", type="primary", width="stretch"):
            add_account_modal(host, token)


def _render_table_header() -> None:
    """Render the static header row for the account management table.

    Returns:
        None: Writes Streamlit column labels and divider markup.
    """
    col_email, col_fullname, col_role, col_action = st.columns([4, 3, 3, 3], vertical_alignment="center")
    col_email.markdown('<div class="header-text">Email</div>', unsafe_allow_html=True)
    col_fullname.markdown('<div class="header-text">Fullname</div>', unsafe_allow_html=True)
    col_role.markdown('<div class="header-text">Role</div>', unsafe_allow_html=True)
    col_action.markdown('<div class="header-text" style="text-align:left;">Action</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)


def _render_stats(users) -> None:
    """Render summary metrics for the current account dataset.

    Args:
        users: Tabular user dataset returned by ``get_accounts``.

    Returns:
        None: Displays Streamlit metric cards for total users and role counts.
    """
    total_users = len(users)
    admin_users = len(users[users["role"] == "admin"])
    superadmin_users = len(users[users["role"] == "superadmin"])
    staff_users = len(users[users["role"].isin(["digital_marketing", "sales"])])

    col1, col2, col3, col4 = st.columns(4, gap="small")
    with col1:
        st.metric("Total Accounts", total_users)
    with col2:
        st.metric("Super Admin", superadmin_users)
    with col3:
        st.metric("Admin", admin_users)
    with col4:
        st.metric("Staff", staff_users)


def _filter_users(users):
    """Render account filters and return the filtered dataset.

    The function applies a free-text search across email and fullname plus an
    optional exact role filter, returning a new filtered view without mutating
    the original dataset.

    Args:
        users: Source user dataframe shown in the management table.

    Returns:
        DataFrame: Filtered user dataframe based on current UI controls.
    """
    search_col, role_col = st.columns([3, 2], vertical_alignment="center")
    with search_col:
        search_term = st.text_input("Search", placeholder="Search by email or fullname")
    with role_col:
        role_filter = st.selectbox(
            "Role Filter",
            options=["All", "superadmin", "admin", "digital_marketing", "sales"],
            index=0,
        )

    filtered = users.copy()
    if search_term:
        keyword = search_term.strip().lower()
        filtered = filtered[
            filtered["email"].str.lower().str.contains(keyword, na=False)
            | filtered["fullname"].str.lower().str.contains(keyword, na=False)
        ]

    if role_filter != "All":
        filtered = filtered[filtered["role"] == role_filter]

    return filtered


def _render_account_rows(host: str, token, users) -> None:
    """Render account rows with per-user management actions.

    Args:
        host (str): Backend base URL forwarded to the edit modal helper.
        token: Authenticated session token object used by account actions.
        users: Filtered user dataframe to display.

    Returns:
        None: Writes one row per account and may open the manage modal.
    """
    if users.empty:
        st.info("No account records found.")
        return

    for user in users.itertuples():
        col_email, col_fullname, col_role, col_action = st.columns([4, 3, 3, 3], vertical_alignment="center")
        col_email.markdown(f'<div class="header-text">{user.email}</div>', unsafe_allow_html=True)
        col_fullname.markdown(f'<div class="header-text">{user.fullname}</div>', unsafe_allow_html=True)
        col_role.markdown(f'<div class="header-text">{user.role}</div>', unsafe_allow_html=True)

        with col_action:
            if st.button("Manage", key=f"manage_{user.user_id}", type="primary"):
                edit_account_modal(host, user, token)

        st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)


async def create_account(host):
    """Render account management page for create/update/delete operations.

    Args:
        host (str): Base URL of backend API service.

    Returns:
        None: UI side effects only.
    """
    access_token = st.session_state.get("access_token")
    if not access_token:
        st.error("Unable to load session token. Please log in again.")
        return

    users = await get_accounts(host)
    if users.empty:
        st.info("No account records found yet, or the account list could not be loaded.")
    st.markdown(TABLE_STYLE, unsafe_allow_html=True)
    _render_page_header(host=host, token=access_token)

    _render_stats(users)
    filtered_users = _filter_users(users)
    _render_table_header()
    _render_account_rows(host=host, token=access_token, users=filtered_users)
