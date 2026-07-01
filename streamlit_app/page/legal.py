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
<p class="tf-legal-updated">Last updated: July 1, 2026</p>
<p>
    Traders Family Dashboard is built for one simple purpose: to help authorized
    Traders Family staff understand business performance from approved internal
    and third-party data sources. It is not a public consumer product, and it is
    not meant for personal use outside company work.
</p>
<p>
    By signing in or using the dashboard, you agree to use it responsibly, only
    for legitimate reporting, analysis, monitoring, and operational needs related
    to Traders Family.
</p>
<h2>Authorized Use</h2>
<p>
    Access is limited to users approved by Traders Family. If you are given
    access, you are responsible for keeping your login credentials secure and for
    making sure the dashboard is used only by you. Please do not share your
    account, access token, screenshots containing sensitive data, exported files,
    or dashboard links with anyone who has not been authorized.
</p>
<h2>Connected Accounts</h2>
<p>
    The dashboard may connect to third-party platforms and business tools, such
    as social media platforms, advertising platforms, analytics tools, or
    internal company systems. A connection is used only after an authorized user
    or administrator grants the required permissions.
</p>
<p>
    Connected data may include account details, content performance, campaign
    performance, audience or engagement metrics, conversion data, and other
    reporting fields made available by the connected source. We use this data to
    show reports inside the internal dashboard, not to publish content or take
    action on connected accounts unless that feature is clearly provided and
    authorized.
</p>
<h2>Data Accuracy</h2>
<p>
    The dashboard depends on data supplied by connected platforms, APIs, files,
    databases, and internal systems. We work to keep the reports useful and
    reliable, but numbers may change when a platform updates its metrics,
    permissions, attribution rules, API responses, or historical data. The
    dashboard should support business decisions, but users should review the
    context before treating any report as final.
</p>
<h2>Acceptable Use</h2>
<p>
    Please use the dashboard in a way that respects company policy, platform
    rules, and applicable law. You may not use the dashboard to access data you
    are not allowed to see, bypass permissions, scrape unauthorized data, reverse
    engineer the service, interfere with the system, or use connected platform
    data in a way that violates the terms of those platforms.
</p>
<h2>Exports And Internal Sharing</h2>
<p>
    Some reports may be copied, downloaded, or discussed internally. When you
    share dashboard data, make sure it stays within the right team or business
    context. If a report includes account data, campaign data, customer data, or
    other sensitive information, handle it carefully and avoid unnecessary public
    sharing.
</p>
<h2>Access Changes</h2>
<p>
    Traders Family may update, limit, suspend, or remove access at any time,
    especially if a user no longer needs access, leaves the organization,
    misuses the dashboard, or if a connected platform changes its requirements.
</p>
<h2>Availability</h2>
<p>
    We try to keep the dashboard available and useful, but it may occasionally
    be unavailable because of maintenance, platform API issues, expired tokens,
    permission changes, infrastructure issues, or business requirement changes.
    Features and metrics may also be added, changed, renamed, or removed over
    time.
</p>
<h2>Changes To These Terms</h2>
<p>
    These terms may be updated when the dashboard changes, when connected
    platforms change their rules, or when internal requirements change. The
    latest version will be available on this page.
</p>
<h2>Contact</h2>
<p>
    If you have questions about these terms, your access, or how dashboard data
    should be used, please contact the Traders Family dashboard administrator.
</p>
"""

_PRIVACY_BODY = """
<p class="tf-legal-updated">Last updated: July 1, 2026</p>
<p>
    Traders Family Dashboard is an internal reporting and analytics application
    used by authorized Traders Family staff. This Privacy Policy explains what
    data the dashboard may collect, why it is used, and how it is handled inside
    our internal reporting environment.
</p>
<p>
    The dashboard is designed for business reporting. It is not intended to sell,
    rent, or publicly publish data from connected accounts or internal systems.
</p>
<h2>Data We Collect</h2>
<p>
    When an authorized user connects an account, uploads a source, or enables an
    integration, the dashboard may collect data needed for reporting and
    analytics. This can include account profile information, campaign
    performance, content performance, engagement metrics, audience statistics,
    traffic data, transaction or conversion records, and other fields made
    available by the connected source or granted permissions.
</p>
<p>
    Connected sources may include social media platforms, advertising platforms,
    analytics tools, internal databases, spreadsheets, operational systems, and
    other business tools approved for internal reporting.
</p>
<h2>Connected Account Data</h2>
<p>
    For connected platform accounts, the dashboard only requests data that is
    needed to display reports, monitor performance, and support business
    analysis. The exact fields available may depend on the platform, the account
    type, the permissions granted, and the platform API version.
</p>
<h2>How We Use Data</h2>
<p>
    We use dashboard data to prepare internal reports, monitor marketing and
    content performance, compare results across channels, troubleshoot data
    pipelines, and help authorized teams make operational decisions. The
    dashboard does not sell connected account data.
</p>
<h2>Tokens And Access</h2>
<p>
    Some integrations require access tokens, refresh tokens, API keys, or other
    credentials so the dashboard can refresh authorized data. These credentials
    are used only for the connected integration and are limited to approved
    backend services and authorized administrators who need access for setup or
    maintenance.
</p>
<p>
    Users should not share tokens, passwords, or connected account credentials
    outside the approved setup process.
</p>
<h2>Data Sharing</h2>
<p>
    Dashboard data is used internally and is not shared publicly by the
    application. It may be processed by infrastructure, database, hosting, or
    monitoring providers that help operate the dashboard, subject to access
    controls and business requirements.
</p>
<p>
    Authorized staff may share reports internally when needed for work, but they
    should avoid sharing sensitive exports, screenshots, or account data outside
    the proper team or business context.
</p>
<h2>Data Quality And Platform Changes</h2>
<p>
    Reporting data may change when a connected platform updates its API,
    attribution logic, metric definitions, permissions, or historical reporting.
    We may store snapshots or transformed reporting tables so the dashboard can
    remain useful for trend analysis and operational review.
</p>
<h2>Data Retention</h2>
<p>
    Analytics data, connection records, and refresh history may be retained for
    internal reporting, audit, troubleshooting, and trend analysis. Data may be
    removed when it is no longer needed, when an integration is disconnected, or
    when removal is requested and approved according to internal requirements.
</p>
<h2>Security</h2>
<p>
    We use access controls and operational safeguards to limit dashboard access
    to authorized users. No system is perfect, so users should also protect their
    accounts, use the dashboard only on trusted devices, and report suspicious
    access or data issues to the dashboard administrator.
</p>
<h2>Your Choices</h2>
<p>
    If you are an authorized user and want an integration reviewed, disconnected,
    refreshed, or removed, contact the dashboard administrator. Some data may
    need to be kept for legitimate internal reporting, compliance, or audit
    reasons.
</p>
<h2>Changes To This Policy</h2>
<p>
    This policy may be updated when the dashboard changes, when integrations are
    added or removed, or when platform requirements change. The latest version
    will be available on this page.
</p>
<h2>Contact</h2>
<p>
    For privacy questions, access questions, or data removal requests, please
    contact the Traders Family dashboard administrator.
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
