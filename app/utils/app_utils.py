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
<p class="updated">Last updated: June 30, 2026</p>
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
<p class="updated">Last updated: June 30, 2026</p>
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
        self.app.include_router(instagram_token_router, tags=["Instagram Token"])
        self.app.include_router(meta_ads_token_router, tags=["Meta Ads Token"])
        self.app.include_router(feature_router, tags=["Update Data"])
        self.app.include_router(overview_router, tags=["Overview Analytics"])
        self.app.include_router(campaign_router, tags=["Campaign Analytics"])
        self.app.include_router(deposit_router, tags=["Deposit Analytics"])
        self.app.include_router(facebook_router, tags=["Facebook Analytics"])
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
