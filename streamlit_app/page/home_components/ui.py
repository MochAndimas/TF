"""UI helpers for the home page."""

from __future__ import annotations

import streamlit as st

PAGE_STYLE = """
<style>
.tf-home-shell {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}
.tf-home-hero {
    position: relative;
    overflow: hidden;
    border-radius: 24px;
    padding: 1.5rem 1.5rem 1.35rem 1.5rem;
    background:
        radial-gradient(circle at top right, rgba(255,255,255,0.18), transparent 30%),
        linear-gradient(135deg, #0f172a 0%, #1d4ed8 55%, #0891b2 100%);
    color: #f8fafc;
    border: 1px solid rgba(255,255,255,0.12);
}
.tf-home-eyebrow {
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: 0.78rem;
    opacity: 0.8;
    margin-bottom: 0.45rem;
}
.tf-home-title {
    font-size: 2.4rem;
    font-weight: 800;
    line-height: 1.05;
    margin: 0;
}
.tf-home-subtitle {
    margin-top: 0.6rem;
    font-size: 1rem;
    max-width: 760px;
    opacity: 0.88;
}
.tf-home-section-title {
    font-size: 0.9rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    opacity: 0.72;
    margin-top: 0.35rem;
    margin-bottom: 0.35rem;
}
.tf-home-shortcut {
    border: 1px solid rgba(148, 163, 184, 0.24);
    border-radius: 20px;
    padding: 1rem 1rem 0.85rem 1rem;
    min-height: 182px;
    background:
        linear-gradient(180deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);
}
.tf-home-shortcut-kicker {
    font-size: 0.75rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    opacity: 0.64;
}
.tf-home-shortcut-title {
    font-size: 1.2rem;
    font-weight: 700;
    margin-top: 0.35rem;
    margin-bottom: 0.45rem;
}
.tf-home-shortcut-copy {
    font-size: 0.94rem;
    line-height: 1.45;
    opacity: 0.82;
    min-height: 58px;
}
.tf-home-status-card {
    border-radius: 18px;
    border: 1px solid rgba(148, 163, 184, 0.22);
    padding: 1rem;
    background: rgba(15, 23, 42, 0.03);
    min-height: 136px;
}
.tf-home-status-label {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    opacity: 0.62;
}
.tf-home-status-value {
    font-size: 1.2rem;
    font-weight: 700;
    margin-top: 0.45rem;
    margin-bottom: 0.35rem;
}
.tf-home-status-copy {
    font-size: 0.92rem;
    opacity: 0.78;
    line-height: 1.45;
}
</style>
"""

SHORTCUT_CONTENT: dict[str, dict[str, str]] = {
    "overview": {"kicker": "Overall", "title": "Overview", "description": "High-level campaign performance across active users, ad cost, leads, and brand awareness."},
    "user_acquisition": {"kicker": "Campaign", "title": "User Acquisition", "description": "Review campaign breakdown, lead efficiency, source mix, and daily performance charts."},
    "brand_awareness": {"kicker": "Campaign", "title": "Brand Awareness", "description": "Track reach, impressions, CTR, CPM, CPC, and spend performance by source platform."},
    "deposit_report": {"kicker": "Revenue", "title": "First Deposit", "description": "View daily first-deposit reports, new vs existing user volume, and average order value."},
    "update_data": {"kicker": "Settings", "title": "Update Data", "description": "Trigger ETL synchronization for campaign, GA4, or first-deposit data from external sources."},
    "register": {"kicker": "Settings", "title": "Create Account", "description": "Manage new user onboarding and assign dashboard access roles."},
}


def go_to(page_key: str) -> None:
    """Navigate to another Streamlit page by updating shared session state."""
    st.session_state.page = page_key
    st.rerun()


def render_hero(fullname: str) -> None:
    """Render the home-page hero."""
    st.markdown(
        f"""
        <div class="tf-home-hero">
            <div class="tf-home-eyebrow">Internal Dashboard</div>
            <h1 class="tf-home-title">Welcome back, {fullname}</h1>
            <div class="tf-home-subtitle">
                This dashboard includes active users, ad cost, lead acquisition,
                brand awareness, and first deposit.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_quick_access() -> None:
    """Render quick access cards for allowed pages."""
    st.markdown('<div class="tf-home-section-title">Quick Access</div>', unsafe_allow_html=True)
    available_pages = [page_key for page_key in ("overview", "user_acquisition", "brand_awareness", "deposit_report", "update_data", "register") if page_key in st.session_state.get("allowed_pages", [])]
    if not available_pages:
        return
    shortcut_columns = st.columns(min(len(available_pages), 3), gap="small")
    for index, page_key in enumerate(available_pages):
        content = SHORTCUT_CONTENT[page_key]
        column = shortcut_columns[index % len(shortcut_columns)]
        with column:
            with st.container(border=False):
                st.markdown(
                    f"""
                    <div class="tf-home-shortcut">
                        <div class="tf-home-shortcut-kicker">{content["kicker"]}</div>
                        <div class="tf-home-shortcut-title">{content["title"]}</div>
                        <div class="tf-home-shortcut-copy">{content["description"]}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                if st.button(f"Open {content['title']}", key=f"home_nav_{page_key}", type="secondary", width="stretch"):
                    go_to(page_key)


def render_status_cards(*, account, latest_run, role_label: str) -> None:
    """Render session and workspace status cards."""
    st.markdown('<div class="tf-home-section-title">Workspace Status</div>', unsafe_allow_html=True)
    is_superadmin = st.session_state.get("role") == "superadmin"
    status_columns = st.columns(3 if is_superadmin else 2, gap="small")

    with status_columns[0]:
        session_status_copy = f"Signed in as {account.get('email', '-')}"
        st.markdown(
            f"""
            <div class="tf-home-status-card">
                <div class="tf-home-status-label">Session</div>
                <div class="tf-home-status-value">{role_label}</div>
                <div class="tf-home-status-copy">{session_status_copy}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    if is_superadmin:
        with status_columns[1]:
            latest_status = latest_run.get("status") if latest_run else None
            latest_source = latest_run.get("source") if latest_run else None
            latest_started = latest_run.get("formatted_started_at") if latest_run else None
            st.markdown(
                f"""
                <div class="tf-home-status-card">
                    <div class="tf-home-status-label">Last ETL Run</div>
                    <div class="tf-home-status-value">{latest_status}</div>
                    <div class="tf-home-status-copy">
                        Source: {latest_source}<br/>
                        Started at: {latest_started}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    data_window_column = status_columns[2] if is_superadmin else status_columns[1]
    with data_window_column:
        latest_window = "-"
        if latest_run and latest_run.get("window_start") and latest_run.get("window_end"):
            latest_window = f"{latest_run['window_start']} to {latest_run['window_end']}"
        st.markdown(
            f"""
            <div class="tf-home-status-card">
                <div class="tf-home-status-label">Data Window</div>
                <div class="tf-home-status-value">{latest_window}</div>
                <div class="tf-home-status-copy">
                    Pipeline: {latest_run.get("pipeline", "-") if latest_run else "-"}<br/>
                    Message: {(latest_run.get("message") if latest_run else None) or "No ETL activity recorded yet."}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
