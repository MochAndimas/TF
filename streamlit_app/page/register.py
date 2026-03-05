import streamlit as st
from streamlit_app.functions.utils import (
    add_account_modal,
    edit_account_modal,
    get_accounts,
    get_user,
)


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
    """Render title and create-account action row."""
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
    """Render account table header."""
    col_email, col_fullname, col_role, col_action = st.columns([4, 3, 3, 3], vertical_alignment="center")
    col_email.markdown('<div class="header-text">Email</div>', unsafe_allow_html=True)
    col_fullname.markdown('<div class="header-text">Fullname</div>', unsafe_allow_html=True)
    col_role.markdown('<div class="header-text">Role</div>', unsafe_allow_html=True)
    col_action.markdown('<div class="header-text" style="text-align:left;">Action</div>', unsafe_allow_html=True)
    st.markdown('<div class="table-divider"></div>', unsafe_allow_html=True)


def _render_stats(users) -> None:
    """Render compact account statistics cards."""
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
    """Render table filters and return filtered DataFrame."""
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
    """Render account rows and action buttons."""
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
    token = get_user(st.session_state._user_id)
    if token is None:
        st.error("Unable to load session token. Please log in again.")
        return

    users = get_accounts()
    st.markdown(TABLE_STYLE, unsafe_allow_html=True)
    _render_page_header(host=host, token=token)

    _render_stats(users)
    filtered_users = _filter_users(users)
    _render_table_header()
    _render_account_rows(host=host, token=token, users=filtered_users)
