"""Public legal pages for external platform app reviews."""

from __future__ import annotations

import streamlit as st


_LEGAL_CSS = """
<style>
    [data-testid="stSidebar"],
    [data-testid="collapsedControl"] {
        display: none;
    }
    .block-container {
        max-width: 860px;
        padding-top: 3.5rem;
        padding-bottom: 4rem;
    }
    .tf-legal-card {
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 2rem;
        background: #ffffff;
        color: #1f2937;
    }
    .tf-legal-card h1 {
        margin-top: 0;
        color: #111827;
        font-size: 2rem;
        line-height: 1.2;
    }
    .tf-legal-card h2 {
        margin-top: 1.75rem;
        color: #111827;
        font-size: 1.25rem;
    }
    .tf-legal-card p {
        color: #374151;
    }
    .tf-legal-updated {
        color: #6b7280;
        margin-bottom: 1.5rem;
    }
</style>
"""

_TERMS_BODY = """
<p class="tf-legal-updated">Last updated: June 30, 2026</p>
<p>
    Traders Family Dashboard is an internal reporting and analytics tool for
    authorized staff. By using this application, you agree to access it only
    for legitimate business reporting and operational purposes.
</p>
<h2>Authorized Use</h2>
<p>
    Access is limited to approved users. You must keep your login credentials
    secure and may not share access with unauthorized parties.
</p>
<h2>Connected Accounts</h2>
<p>
    The application may connect to third-party platforms, including TikTok, only
    after an authorized user grants the required permissions. Data is used to
    display account profile information and performance metrics inside the
    internal dashboard.
</p>
<h2>Acceptable Use</h2>
<p>
    Users may not misuse platform data, attempt to bypass platform permissions,
    scrape unauthorized data, or use the dashboard in a way that violates
    applicable platform terms or laws.
</p>
<h2>Availability</h2>
<p>
    The service is provided for internal reporting needs. Features may change as
    platform APIs, permissions, or business requirements change.
</p>
<h2>Contact</h2>
<p>
    For questions about these terms, contact the Traders Family dashboard
    administrator.
</p>
"""

_PRIVACY_BODY = """
<p class="tf-legal-updated">Last updated: June 30, 2026</p>
<p>
    Traders Family Dashboard is an internal analytics application. This policy
    explains how the dashboard handles data connected by authorized users.
</p>
<h2>Data We Collect</h2>
<p>
    When an authorized user connects an account or data source, the dashboard may
    collect account profile information, campaign performance data, content
    performance metrics, audience statistics, transaction or conversion records,
    and other reporting fields allowed by the granted permissions or provided by
    the connected source.
</p>
<h2>How We Use Data</h2>
<p>
    Data is used to provide internal reporting, performance monitoring, and
    analytics for authorized staff. The dashboard does not sell connected
    account data.
</p>
<h2>Tokens And Access</h2>
<p>
    Access tokens or refresh tokens may be stored securely so the dashboard can
    refresh authorized data. Access is limited to approved internal users and
    backend services.
</p>
<h2>Data Sharing</h2>
<p>
    Data is not shared publicly. It may be processed by infrastructure providers
    used to host or operate the dashboard, subject to internal access controls.
</p>
<h2>Data Retention</h2>
<p>
    Analytics data and connection records are retained for internal reporting
    needs unless removal is requested or no longer required.
</p>
<h2>Contact</h2>
<p>
    For privacy questions or data removal requests, contact the Traders Family
    dashboard administrator.
</p>
"""


async def show_terms_page(host: str) -> None:  # noqa: ARG001
    """Render public terms of service."""
    _render_legal_page(title="Terms of Service", body=_TERMS_BODY)


async def show_privacy_page(host: str) -> None:  # noqa: ARG001
    """Render public privacy policy."""
    _render_legal_page(title="Privacy Policy", body=_PRIVACY_BODY)


def _render_legal_page(*, title: str, body: str) -> None:
    st.markdown(_LEGAL_CSS, unsafe_allow_html=True)
    st.markdown(
        f'<div class="tf-legal-card"><h1>{title}</h1>{body}</div>',
        unsafe_allow_html=True,
    )
