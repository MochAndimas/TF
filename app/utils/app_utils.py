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
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from sqlalchemy import text
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.sessions import SessionMiddleware
from uvicorn import run as uvicorn_run

from app.api.v1.endpoint.auth import router as auth_router
from app.api.v1.endpoint.campaign import router as campaign_router
from app.api.v1.endpoint.deposit import router as deposit_router
from app.api.v1.endpoint.feature import router as feature_router
from app.api.v1.endpoint.google_ads_oauth import router as google_ads_oauth_router
from app.api.v1.endpoint.instagram_token import router as instagram_token_router
from app.api.v1.endpoint.meta_ads_token import router as meta_ads_token_router
from app.api.v1.endpoint.overview import router as overview_router
from app.core.config import settings
from app.db.bootstrap import initialize_database_schema, verify_database_ready
from app.db.session import sqlite_async_session, sqlite_engine
from app.utils.http_security import security_headers_middleware
from app.utils.request_logging import RequestLogService
from app.utils.superadmin_bootstrap import SuperadminBootstrapService

NextHandler = Callable[[Request], Awaitable[Response]]


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
        self.app.include_router(instagram_token_router, tags=["Instagram Token"])
        self.app.include_router(meta_ads_token_router, tags=["Meta Ads Token"])
        self.app.include_router(feature_router, tags=["Update Data"])
        self.app.include_router(overview_router, tags=["Overview Analytics"])
        self.app.include_router(campaign_router, tags=["Campaign Analytics"])
        self.app.include_router(deposit_router, tags=["Deposit Analytics"])

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
