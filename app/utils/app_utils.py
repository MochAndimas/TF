"""Application bootstrap utilities for FastAPI service wiring.

This module encapsulates application startup concerns including:
    - FastAPI app construction and metadata,
    - middleware registration (CORS, session, security headers, CSRF),
    - versioned router registration,
    - runtime server boot configuration.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from random import random
from time import perf_counter
from typing import Awaitable, Callable

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from sqlalchemy import delete, select, text
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.sessions import SessionMiddleware
from uvicorn import run as uvicorn_run

from app.api.v1.endpoint.auth import router as auth_router
from app.api.v1.endpoint.campaign import router as campaign_router
from app.api.v1.endpoint.deposit import router as deposit_router
from app.api.v1.endpoint.feature import router as feature_router
from app.api.v1.endpoint.google_ads_oauth import router as google_ads_oauth_router
from app.api.v1.endpoint.meta_ads_token import router as meta_ads_token_router
from app.api.v1.endpoint.overview import router as overview_router
from app.core.config import settings
from app.core.security import pwd_context, validate_password_policy
from app.db.bootstrap import initialize_database_schema, verify_database_ready
from app.db.models.user import LogData, TfUser
from app.db.session import sqlite_async_session, sqlite_engine

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

    SKIPPED_LOG_PATHS = {
        "/health",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/api/login",
        "/api/register",
        "/api/token/refresh",
        "/api/google-ads/oauth/callback",
        "/api/google-ads/oauth/start",
        "/api/meta-ads/token/exchange",
    }

    def __init__(self, version: str = "1.0.0") -> None:
        """Initialize app, middleware, routes, and runtime configuration.

        Args:
            version (str): API version string shown in OpenAPI metadata.

        Returns:
            None: This constructor initializes instance attributes in place.
        """
        self.app_version = version
        self.logger = logging.getLogger(self.__class__.__name__)
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
        await self._bootstrap_superadmin_if_enabled()
        yield
        self.logger.info("Application shutdown: disposing database engine")
        await sqlite_engine.dispose()

    async def _bootstrap_superadmin_if_enabled(self) -> None:
        """Create initial superadmin account when bootstrap mode is active.

        This bootstrap routine is intentionally one-time and guarded by:
            - environment validation,
            - existing-account check for configured bootstrap email,
            - empty-active-user check to prevent accidental overwrite.

        Returns:
            None: Writes bootstrap account into database when conditions pass.
        """
        settings.validate_bootstrap_config()
        if not settings.BOOTSTRAP_SUPERADMIN:
            return

        bootstrap_email = str(settings.INITIAL_SUPERADMIN_EMAIL).lower().strip()
        validate_password_policy(str(settings.INITIAL_SUPERADMIN_PASSWORD))
        now = datetime.now()

        async with sqlite_async_session() as session:
            existing_user_result = await session.execute(
                select(TfUser).where(
                    TfUser.email == bootstrap_email,
                    TfUser.deleted_at == None,
                )
            )
            existing_user = existing_user_result.scalars().first()
            if existing_user is not None:
                self.logger.info(
                    "Bootstrap superadmin skipped: account already exists for %s",
                    bootstrap_email,
                )
                return

            active_users_result = await session.execute(
                select(TfUser.user_id).where(TfUser.deleted_at == None)
            )
            if active_users_result.first() is not None:
                self.logger.warning(
                    "Bootstrap superadmin skipped: active users already exist. "
                    "Disable BOOTSTRAP_SUPERADMIN."
                )
                return

            session.add(
                TfUser(
                    user_id=str(uuid.uuid4()),
                    fullname=str(settings.INITIAL_SUPERADMIN_NAME).strip(),
                    email=bootstrap_email,
                    role="superadmin",
                    password=pwd_context.hash(str(settings.INITIAL_SUPERADMIN_PASSWORD)),
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                )
            )
            await session.commit()

        self.logger.warning(
            "Bootstrap superadmin created for %s. Set BOOTSTRAP_SUPERADMIN=false after first deploy.",
            bootstrap_email,
        )

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
            return await self._request_logging_middleware(request, call_next)

        @self.app.middleware("http")
        async def security_headers(request: Request, call_next: NextHandler) -> Response:
            return await self._security_headers_middleware(request, call_next)

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

    async def _request_logging_middleware(self, request: Request, call_next: NextHandler) -> Response:
        """Log request/response summary into database in a fail-safe way.

        Args:
            request (Request): Incoming HTTP request.
            call_next (NextHandler): Next handler in middleware chain.

        Returns:
            Response: Response object returned by downstream handler.
        """
        started_at = perf_counter()
        response = await call_next(request)

        process_time = perf_counter() - started_at
        if self._should_persist_request_log(request):
            await self._persist_request_log(
                request=request,
                response=response,
                process_time=process_time,
            )
        return response

    async def _persist_request_log(
        self,
        request: Request,
        response: Response,
        process_time: float,
    ) -> None:
        """Persist request/response metadata to database.

        Args:
            request (Request): Incoming HTTP request metadata source.
            response (Response): Response metadata source.
            process_time (float): Request processing duration in seconds.

        Returns:
            None: Logging failures are swallowed and only written to application logger.
        """
        try:
            async with sqlite_async_session() as session:
                session.add(
                    LogData(
                        url=self._safe_log_url(request),
                        method=request.method,
                        time=process_time,
                        status=response.status_code,
                        response=self._request_log_metadata(request, response),
                        created_at=datetime.now(),
                    )
                )
                await session.commit()
                await self._cleanup_old_request_logs(session)
        except Exception:
            self.logger.exception("Failed to persist request log")

    @classmethod
    def _should_persist_request_log(cls, request: Request) -> bool:
        """Return whether this request should be sampled into DB logs."""
        if not settings.REQUEST_LOG_ENABLED:
            return False
        if request.method.upper() == "OPTIONS":
            return False
        if request.url.path in cls.SKIPPED_LOG_PATHS:
            return False
        return random() < cls._clamped_rate(settings.REQUEST_LOG_SAMPLE_RATE)

    @staticmethod
    def _clamped_rate(value: float) -> float:
        """Clamp a configured sampling rate into the inclusive 0..1 range."""
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _safe_log_url(request: Request) -> str:
        """Store path plus query key names, never raw query values."""
        query_keys = sorted(set(request.query_params.keys()))
        if not query_keys:
            return request.url.path
        return f"{request.url.path}?query_keys={','.join(query_keys)}"

    @staticmethod
    def _request_log_metadata(request: Request, response: Response) -> dict[str, object]:
        """Build a bounded metadata payload without request/response bodies."""
        user_agent = request.headers.get("user-agent")
        return {
            "log_type": "http_metadata_v1",
            "path": request.url.path,
            "query_keys": sorted(set(request.query_params.keys())),
            "client_host": request.client.host if request.client else None,
            "user_agent": user_agent[:200] if user_agent else None,
            "content_type": response.headers.get("content-type"),
            "content_length": response.headers.get("content-length"),
            "referer_present": "referer" in request.headers,
            "body_logged": False,
        }

    async def _cleanup_old_request_logs(self, session) -> None:
        """Probabilistically delete request logs older than configured retention."""
        retention_days = settings.REQUEST_LOG_RETENTION_DAYS
        if retention_days <= 0:
            return
        cleanup_rate = self._clamped_rate(settings.REQUEST_LOG_RETENTION_CLEANUP_SAMPLE_RATE)
        if random() >= cleanup_rate:
            return

        cutoff = datetime.now() - timedelta(days=retention_days)
        await session.execute(delete(LogData).where(LogData.created_at < cutoff))
        await session.commit()

    @staticmethod
    async def _security_headers_middleware(request: Request, call_next: NextHandler) -> Response:
        """Attach baseline security headers to every HTTP response.

        Args:
            request (Request): Incoming HTTP request.
            call_next (NextHandler): Next middleware/route callable in chain.

        Returns:
            Response: Response enriched with security-related HTTP headers.
        """
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        docs_paths = {"/docs", "/redoc", "/openapi.json"}
        if request.url.path in docs_paths:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self' https://cdn.jsdelivr.net; "
                "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
                "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com data:; "
                "img-src 'self' data: https:; "
                "connect-src 'self'; "
                "frame-ancestors 'none'; "
                "base-uri 'self'"
            )
        else:
            response.headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'; base-uri 'self'"
        return response

    def _include_routers_v1(self) -> None:
        """Register API routers for versioned endpoints.

        Returns:
            None: Routers are attached to ``self.app``.
        """
        self.app.include_router(auth_router, tags=["Authentication"])
        self.app.include_router(google_ads_oauth_router, tags=["Google Ads OAuth"])
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
