"""UI helpers for the home page."""

from __future__ import annotations

from html import escape

import streamlit as st

PAGE_STYLE = """
<style>
.tf-home-shell {
    display: flex;
    flex-direction: column;
    gap: 1.35rem;
    max-width: 1320px;
    margin: 0 auto;
}
.tf-home-hero {
    position: relative;
    overflow: hidden;
    border-radius: 26px;
    padding: 2rem 2rem 1.8rem;
    background:
        radial-gradient(circle at 87% 15%, rgba(125, 211, 252, 0.34), transparent 22%),
        radial-gradient(circle at 72% 110%, rgba(59, 130, 246, 0.42), transparent 35%),
        linear-gradient(120deg, #0b1224 0%, #172554 58%, #0c4a6e 100%);
    color: #f8fafc;
    border: 1px solid rgba(255,255,255,0.12);
    box-shadow: 0 18px 42px rgba(2, 6, 23, 0.24);
}
.tf-home-hero::after {
    content: "";
    position: absolute;
    width: 250px;
    height: 250px;
    right: -82px;
    top: -118px;
    border: 1px solid rgba(255,255,255,0.18);
    border-radius: 50%;
    box-shadow: 0 0 0 26px rgba(255,255,255,0.04), 0 0 0 54px rgba(255,255,255,0.025);
}
.tf-home-hero-content {
    position: relative;
    z-index: 1;
    max-width: 760px;
}
.tf-home-eyebrow {
    display: inline-flex;
    align-items: center;
    gap: 0.42rem;
    padding: 0.32rem 0.65rem;
    border: 1px solid rgba(255,255,255,0.22);
    border-radius: 999px;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    font-size: 0.7rem;
    font-weight: 700;
    background: rgba(255,255,255,0.09);
    margin-bottom: 0.8rem;
}
.tf-home-title {
    font-size: clamp(2rem, 4vw, 3.25rem);
    font-weight: 800;
    letter-spacing: -0.045em;
    line-height: 1.02;
    margin: 0;
}
.tf-home-subtitle {
    margin-top: 0.85rem;
    font-size: 1.02rem;
    line-height: 1.55;
    max-width: 760px;
    opacity: 0.84;
}
.tf-home-hero-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 1.25rem;
}
.tf-home-hero-meta span {
    padding: 0.38rem 0.65rem;
    border-radius: 8px;
    background: rgba(15, 23, 42, 0.32);
    border: 1px solid rgba(255,255,255,0.12);
    color: rgba(255,255,255,0.88);
    font-size: 0.82rem;
}
.tf-home-section-title {
    font-size: 1.12rem;
    font-weight: 750;
    letter-spacing: -0.02em;
    opacity: 0.95;
    margin: 0.15rem 0 0.2rem;
}
.tf-home-section-note {
    color: rgba(148, 163, 184, 0.94);
    font-size: 0.88rem;
    margin: 0 0 0.55rem;
}
.tf-home-launcher {
    padding: 1.25rem 0 1.1rem;
    border-top: 1px solid rgba(96, 165, 250, 0.45);
    border-bottom: 1px solid rgba(148, 163, 184, 0.22);
}
.tf-home-launcher-title {
    font-size: 1.45rem;
    font-weight: 760;
    letter-spacing: -0.03em;
    margin-bottom: 0.25rem;
}
.tf-home-launcher-copy {
    color: rgba(203, 213, 225, 0.82);
    font-size: 0.94rem;
    margin-bottom: 0.85rem;
}
.tf-home-status-strip {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    border-top: 1px solid rgba(148, 163, 184, 0.22);
    border-bottom: 1px solid rgba(148, 163, 184, 0.22);
}
.tf-home-status-item {
    padding: 0.85rem 1rem;
}
.tf-home-status-item + .tf-home-status-item {
    border-left: 1px solid rgba(148, 163, 184, 0.22);
}
.tf-home-status-label {
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    opacity: 0.62;
}
.tf-home-status-value {
    font-size: 1rem;
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
    "overview": {"kicker": "Overall", "title": "Overview", "description": "High-level campaign performance across active users, ad cost, register, and brand awareness."},
    "user_acquisition": {"kicker": "Campaign", "title": "User Acquisition", "description": "Review campaign breakdown, register efficiency, source mix, and daily performance charts."},
    "brand_awareness": {"kicker": "Campaign", "title": "Brand Awareness", "description": "Track reach, impressions, CTR, CPM, CPC, and spend performance by source platform."},
    "remarketing": {"kicker": "Campaign", "title": "Remarketing", "description": "Review remarketing campaign performance using login volume from MS deposit activity data."},
    "instagram": {"kicker": "Socmed", "title": "Instagram", "description": "Monitor Instagram followers, engagement, and post or Reels performance from synced insights data."},
    "facebook": {"kicker": "Socmed", "title": "Facebook", "description": "Track Facebook Page followers, reactions, video views, and media performance trends."},
    "tiktok": {"kicker": "Socmed", "title": "TikTok", "description": "Review TikTok followers, video performance, engagement, and content trends from synced account insights."},
    "youtube": {"kicker": "Socmed", "title": "YouTube", "description": "Review YouTube views, watch hours, subscriber movement, and content performance by video type."},
    "internal_register": {"kicker": "Activity", "title": "Register", "description": "Analyze daily register trends, campaign contribution, pacing, and source mix from internal register data."},
    "login_activity": {"kicker": "Activity", "title": "Login", "description": "Track daily login users from MS deposit last activity with source and campaign breakdown."},
    "install": {"kicker": "Activity", "title": "Install", "description": "Inspect app install data, channel contribution, daily movement, and campaign-level acquisition quality."},
    "deposit_report": {"kicker": "Revenue", "title": "First Deposit", "description": "View daily first deposit reports, new vs existing user volume, and average order value."},
    "remarketing_deposit": {"kicker": "Revenue", "title": "Remarketing Deposit", "description": "View MS1 remarketing deposit reports by last activity and last deposit date range."},
    "update_data": {"kicker": "Settings", "title": "Update Data", "description": "Trigger ETL synchronization for campaign, GA4, or first deposit data from external sources."},
    "register": {"kicker": "Settings", "title": "Create Account", "description": "Manage new user onboarding and assign dashboard access roles."},
}

def go_to(page_key: str) -> None:
    """Navigate to another Streamlit page by updating shared session state."""
    st.session_state["page_override_once"] = page_key
    st.session_state.page = page_key
    st.rerun()


def render_hero(fullname: str) -> None:
    """Render the home-page hero."""
    st.markdown(
        f"""
        <div class="tf-home-hero">
            <div class="tf-home-hero-content">
                <div class="tf-home-eyebrow">● Internal Intelligence</div>
                <div class="tf-home-title">Good to see you, {escape(fullname)}</div>
                <div class="tf-home-subtitle">
                    One place to follow campaign efficiency, audience growth, and revenue signals across every active channel.
                </div>
                <div class="tf-home-hero-meta">
                    <span>Campaign performance</span>
                    <span>Audience insights</span>
                    <span>Revenue reporting</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_quick_access() -> None:
    """Render a single workspace launcher without duplicating sidebar navigation."""
    available_pages = [
        page_key
        for page_key in (
            "overview",
            "user_acquisition",
            "brand_awareness",
            "remarketing",
            "instagram",
            "facebook",
            "tiktok",
            "youtube",
            "internal_register",
            "login_activity",
            "install",
            "deposit_report",
            "remarketing_deposit",
            "update_data",
            "register",
        )
        if page_key in st.session_state.get("allowed_pages", [])
    ]
    if not available_pages:
        return
    st.markdown(
        '<div class="tf-home-launcher">'
        '<div class="tf-home-launcher-title">Where do you want to work?</div>'
        '<div class="tf-home-launcher-copy">Choose a workspace, then continue. Your full navigation stays in the sidebar.</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    default_page = "overview" if "overview" in available_pages else available_pages[0]
    selected_page = st.selectbox(
        "Workspace",
        options=available_pages,
        index=available_pages.index(default_page),
        format_func=lambda page_key: f"{SHORTCUT_CONTENT[page_key]['kicker']} · {SHORTCUT_CONTENT[page_key]['title']}",
        key="home_workspace_launcher",
    )
    selected_content = SHORTCUT_CONTENT[selected_page]
    action_column, description_column = st.columns([1, 2.3], gap="medium")
    with action_column:
        if st.button(f"Open {selected_content['title']} →", key="home_workspace_open", type="primary", width="stretch"):
            go_to(selected_page)
    with description_column:
        st.caption(selected_content["description"])


def render_status_cards(*, account, latest_run, role_label: str) -> None:
    """Render session and workspace status in a single compact strip."""
    st.markdown('<div class="tf-home-section-title">Workspace Status</div>', unsafe_allow_html=True)
    latest_window = "-"
    if latest_run and latest_run.get("window_start") and latest_run.get("window_end"):
        latest_window = f"{latest_run['window_start']} to {latest_run['window_end']}"
    status_items = [
        ("Session", role_label, f"Signed in as {account.get('email', '-')}"),
        (
            "Last ETL Run",
            latest_run.get("status") if latest_run else "No recent run",
            f"{latest_run.get('source', '-')} · {latest_run.get('formatted_started_at', '-')}" if latest_run else "No ETL activity recorded yet.",
        ),
        ("Data Window", latest_window, f"Pipeline: {latest_run.get('pipeline', '-') if latest_run else '-'}"),
    ]
    st.markdown(
        '<div class="tf-home-status-strip">'
        + "".join(
            f'''<div class="tf-home-status-item">
                <div class="tf-home-status-label">{escape(str(label))}</div>
                <div class="tf-home-status-value">{escape(str(value or '-'))}</div>
                <div class="tf-home-status-copy">{escape(str(copy))}</div>
            </div>'''
            for label, value, copy in status_items
        )
        + "</div>",
        unsafe_allow_html=True,
    )
