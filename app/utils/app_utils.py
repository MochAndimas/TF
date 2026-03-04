"""Application bootstrap utilities for FastAPI service wiring."""

from __future__ import annotations

import json
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from time import perf_counter
from typing import Any, Awaitable, Callable

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.requests import Request
from starlette.middleware.sessions import SessionMiddleware
from uvicorn import run as uvicorn_run

from app.api.v1.endpoint.auth import router as auth_router
from app.api.v1.endpoint.feature import router as feature_router
from app.core.config import settings
from app.db.base import SqliteBase
from app.db.models.user import LogData
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

    CSRF_TOKEN_NAME = "csrf_token"

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

        self.app = FastAPI(
            title="Traders Family Campaign Data API",
            description="API for handling Traders Family campaign data.",
            version=self.app_version,
            docs_url=None if not settings.DEBUG else "/docs",
            redoc_url="/redoc",
            lifespan=self._lifespan,
        )

        self._add_middlewares()
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
        self.logger.info("Application startup: preparing database schema")
        async with sqlite_engine.begin() as connection:
            await connection.run_sync(SqliteBase.metadata.create_all)
        yield
        self.logger.info("Application shutdown: disposing database engine")
        await sqlite_engine.dispose()

    def _add_middlewares(self) -> None:
        """Register HTTP middleware stack for the API service.

        Returns:
            None: Middleware handlers are attached to ``self.app``.
        """
        self._add_cors_middleware()

        @self.app.middleware("http")
        async def request_logger(request: Request, call_next: NextHandler) -> Response:
            return await self._request_logging_middleware(request, call_next)

        @self.app.middleware("http")
        async def security_headers(request: Request, call_next: NextHandler) -> Response:
            return await self._security_headers_middleware(request, call_next)

        @self.app.middleware("http")
        async def csrf_cookie(request: Request, call_next: NextHandler) -> Response:
            return await self._csrf_cookie_middleware(request, call_next)

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
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def _add_session_middleware(self) -> None:
        """Attach server-side session middleware.

        Returns:
            None: Session middleware is registered on ``self.app``.
        """
        self.app.add_middleware(
            SessionMiddleware,
            secret_key=settings.CSRF_SECRET,
            same_site="lax",
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
        original_response = await call_next(request)
        replay_response, content = await self._clone_response(original_response)

        process_time = perf_counter() - started_at
        response_body = self._parse_response_body(content)
        await self._persist_request_log(
            request=request,
            status_code=replay_response.status_code,
            response_body=response_body,
            process_time=process_time,
        )
        return replay_response

    async def _clone_response(self, response: Response) -> tuple[Response, bytes]:
        """Clone streamed response body for safe logging and replay.

        Args:
            response (Response): Original downstream response object.

        Returns:
            tuple[Response, bytes]: Reconstructed response and raw buffered body bytes.
        """
        content = b""
        async for chunk in response.body_iterator:
            content += chunk

        replay_response = Response(
            content=content,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
            background=response.background,
        )
        return replay_response, content

    @staticmethod
    def _parse_response_body(content: bytes) -> Any:
        """Parse response body bytes into JSON-like structure or text.

        Args:
            content (bytes): Raw response body bytes.

        Returns:
            Any: Decoded JSON object/list/dict, empty dict for blank body, or UTF-8 text.
        """
        if not content:
            return {}
        try:
            return json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return content.decode("utf-8", errors="replace")

    async def _persist_request_log(
        self,
        request: Request,
        status_code: int,
        response_body: Any,
        process_time: float,
    ) -> None:
        """Persist request/response audit record to database.

        Args:
            request (Request): Incoming HTTP request metadata source.
            status_code (int): HTTP status code returned to client.
            response_body (Any): Parsed response payload snapshot for auditing.
            process_time (float): Request processing duration in seconds.

        Returns:
            None: Logging failures are swallowed and only written to application logger.
        """
        try:
            async with sqlite_async_session() as session:
                session.add(
                    LogData(
                        url=str(request.url),
                        method=request.method,
                        time=process_time,
                        status=status_code,
                        response=response_body,
                        created_at=datetime.now(),
                    )
                )
                await session.commit()
        except Exception:
            self.logger.exception("Failed to persist request log")

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
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        return response

    async def _csrf_cookie_middleware(self, request: Request, call_next: NextHandler) -> Response:
        """Ensure CSRF token exists in session and mirrored cookie.

        Args:
            request (Request): Incoming HTTP request that carries session context.
            call_next (NextHandler): Next middleware/route callable in chain.

        Returns:
            Response: Downstream response augmented with CSRF cookie.
        """
        if "session" not in request.scope:
            self.logger.error("Session scope missing in request; CSRF cookie middleware skipped.")
            return await call_next(request)

        if self.CSRF_TOKEN_NAME not in request.session:
            request.session[self.CSRF_TOKEN_NAME] = secrets.token_hex(16)

        response = await call_next(request)
        response.set_cookie(
            key=self.CSRF_TOKEN_NAME,
            value=request.session[self.CSRF_TOKEN_NAME],
            httponly=True,
            secure=settings.cookie_secure,
            samesite="lax",
        )
        return response

    def _include_routers_v1(self) -> None:
        """Register API routers for versioned endpoints.

        Returns:
            None: Routers are attached to ``self.app``.
        """
        self.app.include_router(auth_router, tags=["Authentication"])
        self.app.include_router(feature_router, tags=["Update Data"])

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
