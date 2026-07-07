"""Application bootstrap utilities for FastAPI service wiring.

This module encapsulates application startup concerns including:
    - FastAPI app construction and metadata,
    - middleware registration (CORS, session, security headers, CSRF),
    - versioned router registration,
    - runtime server boot configuration.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Awaitable, Callable

from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from sqlalchemy import text
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.sessions import SessionMiddleware
from uvicorn import run as uvicorn_run

from app.api.v1.endpoint.auth import router as auth_router
from app.api.v1.endpoint.campaign import router as campaign_router
from app.api.v1.endpoint.deposit import router as deposit_router
from app.api.v1.endpoint.facebook import router as facebook_router
from app.api.v1.endpoint.feature import router as feature_router
from app.api.v1.endpoint.google_ads_oauth import router as google_ads_oauth_router
from app.api.v1.endpoint.instagram_token import router as instagram_token_router
from app.api.v1.endpoint.meta_ads_token import router as meta_ads_token_router
from app.api.v1.endpoint.overview import router as overview_router
from app.api.v1.endpoint.tiktok import router as tiktok_router
from app.api.v1.endpoint.tiktok_oauth import router as tiktok_oauth_router
from app.api.v1.endpoint.youtube import router as youtube_router
from app.api.v1.endpoint.youtube_oauth import router as youtube_oauth_router
from app.core.config import settings
from app.db.bootstrap import initialize_database_schema, verify_database_ready
from app.db.session import sqlite_async_session, sqlite_engine
from app.utils.http_security import security_headers_middleware
from app.utils.request_logging import RequestLogService
from app.utils.superadmin_bootstrap import SuperadminBootstrapService

NextHandler = Callable[[Request], Awaitable[Response]]


_LEGAL_STYLE = """
body {
    margin: 0;
    background: #f6f7fb;
    color: #1f2937;
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
        "Segoe UI", sans-serif;
    line-height: 1.6;
}
main {
    max-width: 820px;
    margin: 0 auto;
    padding: 56px 24px 72px;
}
section {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 32px;
}
h1 {
    margin: 0 0 8px;
    color: #111827;
    font-size: 32px;
    line-height: 1.2;
}
h2 {
    margin: 28px 0 8px;
    color: #111827;
    font-size: 20px;
}
p, li {
    font-size: 16px;
}
.updated {
    margin: 0 0 24px;
    color: #6b7280;
}
"""

_TERMS_BODY = """
<p class="updated">Last updated: July 1, 2026</p>
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
<p class="updated">Last updated: July 1, 2026</p>
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


def _render_legal_page(*, title: str, body: str) -> str:
    """Render a small standalone legal document page."""
    return f"""<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{title} - Traders Family Dashboard</title>
    <style>{_LEGAL_STYLE}</style>
</head>
<body>
    <main>
        <section>
            <h1>{title}</h1>
            {body}
        </section>
    </main>
</body>
</html>"""


class FastApiApp:
    """Create and configure the FastAPI application instance.

    This wrapper centralizes FastAPI bootstrap concerns such as:
    - application metadata and lifespan wiring
    - middleware registration (CORS, session, security, CSRF, request logging)
    - API router registration
    - Uvicorn runtime startup configuration

    Attributes:
        CSRF_TOKEN_NAME (str): Session and cookie key used to store CSRF token value.
        app_version (str): API version exposed in OpenAPI metadata.
        logger (logging.Logger): Logger used for lifecycle and runtime diagnostics.
        app (FastAPI): Configured FastAPI application instance.
    """

    def __init__(self, version: str = "1.0.0") -> None:
        """Initialize app, middleware, routes, and runtime configuration.

        Args:
            version (str): API version string shown in OpenAPI metadata.

        Returns:
            None: This constructor initializes instance attributes in place.
        """
        self.app_version = version
        self.logger = logging.getLogger(self.__class__.__name__)
        self._request_logging = RequestLogService(self.logger)
        self._bootstrap_service = SuperadminBootstrapService(self.logger)
        self._configure_logging()
        settings.validate_runtime_constraints()

        self.app = FastAPI(
            title="Traders Family Campaign Data API",
            description="API for handling Traders Family campaign data.",
            version=self.app_version,
            docs_url=None if not settings.DEBUG else "/docs",
            redoc_url=None if not settings.DEBUG else "/redoc",
            lifespan=self._lifespan,
        )

        self._add_middlewares()
        self._add_builtin_routes()
        self._include_routers_v1()

    @staticmethod
    def _configure_logging() -> None:
        """Configure process-wide logging format once.

        Returns:
            None: Applies logging configuration to the root logger if needed.
        """
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):  # noqa: ARG002
        """Handle application startup and shutdown tasks.

        Args:
            app (FastAPI): Active FastAPI application instance passed by framework.

        Yields:
            None: Control is yielded back to FastAPI during normal runtime.

        Returns:
            None: Disposes database engine after shutdown sequence.
        """
        if settings.AUTO_INIT_DB_ON_STARTUP:
            self.logger.warning(
                "AUTO_INIT_DB_ON_STARTUP is enabled. Prefer `python init_db.py` "
                "or the dedicated db-init service for controlled schema bootstrap."
            )
            await initialize_database_schema()
        else:
            self.logger.info("Application startup: verifying database readiness")
            await verify_database_ready()
        await self._bootstrap_service.ensure_bootstrap_superadmin()
        self._request_logging.start_worker()
        yield
        await self._request_logging.stop_worker()
        self.logger.info("Application shutdown: disposing database engine")
        await sqlite_engine.dispose()

    def _add_builtin_routes(self) -> None:
        """Register lightweight operational routes that do not belong to API v1."""

        @self.app.get("/health")
        async def healthcheck() -> JSONResponse:
            async with sqlite_async_session() as session:
                await session.execute(text("SELECT 1"))

            return JSONResponse(
                content={
                    "status": "ok",
                    "database": "ready",
                    "db_backend": settings.db_backend_name,
                    "worker_mode": "single-worker" if settings.WORKERS == 1 else "multi-worker",
                }
            )

        @self.app.get("/terms", response_class=HTMLResponse, include_in_schema=False)
        async def terms_of_service() -> HTMLResponse:
            return HTMLResponse(
                _render_legal_page(title="Terms of Service", body=_TERMS_BODY)
            )

        @self.app.get("/privacy", response_class=HTMLResponse, include_in_schema=False)
        async def privacy_policy() -> HTMLResponse:
            return HTMLResponse(
                _render_legal_page(title="Privacy Policy", body=_PRIVACY_BODY)
            )

    def _add_middlewares(self) -> None:
        """Register HTTP middleware stack for the API service.

        Returns:
            None: Middleware handlers are attached to ``self.app``.
        """
        self._add_cors_middleware()
        self._add_trusted_host_middleware()

        @self.app.middleware("http")
        async def request_logger(request: Request, call_next: NextHandler) -> Response:
            return await self._request_logging.middleware(request, call_next)

        @self.app.middleware("http")
        async def security_headers(request: Request, call_next: NextHandler) -> Response:
            return await security_headers_middleware(request, call_next)

        # Add SessionMiddleware after decorator-based middleware registration
        # so it wraps outermost and initializes request.session first.
        self._add_session_middleware()

    def _add_cors_middleware(self) -> None:
        """Attach CORS middleware using environment-specific origin policy.

        Returns:
            None: CORS middleware is registered on ``self.app``.
        """
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-CSRF-Token"],
        )

    def _add_trusted_host_middleware(self) -> None:
        """Restrict accepted ``Host`` headers to configured safe origins.

        Returns:
            None: Registers Starlette's trusted-host middleware on ``self.app``
            so unexpected host headers are rejected early.
        """
        self.app.add_middleware(
            TrustedHostMiddleware,
            allowed_hosts=settings.trusted_hosts,
        )

    def _add_session_middleware(self) -> None:
        """Attach server-side session middleware.

        Returns:
            None: Session middleware is registered on ``self.app``.
        """
        self.app.add_middleware(
            SessionMiddleware,
            secret_key=settings.CSRF_SECRET,
            same_site=settings.cookie_samesite,
            https_only=settings.cookie_secure,
        )

    def _include_routers_v1(self) -> None:
        """Register API routers for versioned endpoints.

        Returns:
            None: Routers are attached to ``self.app``.
        """
        self.app.include_router(auth_router, tags=["Authentication"])
        self.app.include_router(google_ads_oauth_router, tags=["Google Ads OAuth"])
        self.app.include_router(youtube_oauth_router, tags=["YouTube OAuth"])
        self.app.include_router(tiktok_oauth_router, tags=["TikTok OAuth"])
        self.app.include_router(instagram_token_router, tags=["Instagram Token"])
        self.app.include_router(meta_ads_token_router, tags=["Meta Ads Token"])
        self.app.include_router(feature_router, tags=["Update Data"])
        self.app.include_router(overview_router, tags=["Overview Analytics"])
        self.app.include_router(campaign_router, tags=["Campaign Analytics"])
        self.app.include_router(deposit_router, tags=["Deposit Analytics"])
        self.app.include_router(facebook_router, tags=["Facebook Analytics"])
        self.app.include_router(tiktok_router, tags=["TikTok Analytics"])
        self.app.include_router(youtube_router, tags=["YouTube Analytics"])

    def run(self) -> None:
        """Start Uvicorn server with environment-aware runtime options.

        Returns:
            None: Blocks current process while ASGI server is running.
        """
        uvicorn_run(
            "main:app_instance.app",
            host=settings.HOST,
            port=settings.PORT,
            workers=settings.WORKERS,
            reload=settings.DEBUG,
            log_level="debug" if settings.DEBUG else "info",
            timeout_keep_alive=30,
        )
