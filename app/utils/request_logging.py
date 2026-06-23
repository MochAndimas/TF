"""Request logging middleware and background worker service."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import random
from time import perf_counter
from typing import Any

from fastapi.requests import Request
from fastapi.responses import Response
from sqlalchemy import delete

from app.core.config import settings
from app.db.models.user import LogData
from app.db.session import sqlite_async_session


@dataclass(slots=True)
class RequestLogEvent:
    """In-memory request log payload queued by middleware and flushed by worker."""

    url: str
    method: str
    process_time: float
    status: int
    metadata: dict[str, object]
    created_at: datetime


class RequestLogService:
    """Own request-log sampling, queueing, worker lifecycle, and DB persistence."""

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
        "/api/youtube/oauth/callback",
        "/api/youtube/oauth/start",
        "/api/instagram/token/exchange",
        "/api/instagram/token/refresh",
        "/api/instagram/token/save",
        "/api/meta-ads/token/exchange",
    }

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._request_log_queue: asyncio.Queue[RequestLogEvent] | None = None
        self._request_log_worker_task: asyncio.Task[Any] | None = None
        self._request_log_worker_stop = asyncio.Event()
        self._request_log_drop_count = 0

    async def middleware(self, request: Request, call_next) -> Response:
        """Log request/response summary into database in a fail-safe way."""
        started_at = perf_counter()
        response = await call_next(request)
        process_time = perf_counter() - started_at

        if self._should_persist_request_log(request):
            self._enqueue_request_log(
                request=request,
                response=response,
                process_time=process_time,
            )
        return response

    def start_worker(self) -> None:
        """Start background worker that flushes queued request logs to DB."""
        if not settings.REQUEST_LOG_ENABLED:
            self._logger.info("Request log worker disabled by configuration.")
            return
        if self._request_log_worker_task is not None:
            return

        queue_size = max(1, int(settings.REQUEST_LOG_QUEUE_MAX_SIZE))
        self._request_log_queue = asyncio.Queue(maxsize=queue_size)
        self._request_log_worker_stop.clear()
        self._request_log_worker_task = asyncio.create_task(self._request_log_worker())
        self._logger.info("Request log worker started with queue size=%s", queue_size)

    async def stop_worker(self) -> None:
        """Stop request log worker and flush pending logs before shutdown."""
        if self._request_log_worker_task is None:
            return

        self._request_log_worker_stop.set()
        try:
            await self._request_log_worker_task
        except Exception:
            self._logger.exception("Request log worker crashed during shutdown")
        finally:
            self._request_log_worker_task = None
            self._request_log_queue = None

    def _enqueue_request_log(
        self,
        *,
        request: Request,
        response: Response,
        process_time: float,
    ) -> None:
        """Queue request/response metadata for async DB persistence."""
        queue = self._request_log_queue
        if queue is None:
            return

        event = RequestLogEvent(
            url=self._safe_log_url(request),
            method=request.method,
            process_time=process_time,
            status=response.status_code,
            metadata=self._request_log_metadata(request, response),
            created_at=datetime.now(),
        )
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            self._request_log_drop_count += 1
            if self._request_log_drop_count % 100 == 1:
                self._logger.warning(
                    "Request log queue full; dropped %s log event(s) so far.",
                    self._request_log_drop_count,
                )

    @classmethod
    def _should_persist_request_log(cls, request: Request) -> bool:
        if not settings.REQUEST_LOG_ENABLED:
            return False
        if request.method.upper() == "OPTIONS":
            return False
        if request.url.path in cls.SKIPPED_LOG_PATHS:
            return False
        return random() < cls._clamped_rate(settings.REQUEST_LOG_SAMPLE_RATE)

    @staticmethod
    def _clamped_rate(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _safe_log_url(request: Request) -> str:
        query_keys = sorted(set(request.query_params.keys()))
        if not query_keys:
            return request.url.path
        return f"{request.url.path}?query_keys={','.join(query_keys)}"

    @staticmethod
    def _request_log_metadata(request: Request, response: Response) -> dict[str, object]:
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
        retention_days = settings.REQUEST_LOG_RETENTION_DAYS
        if retention_days <= 0:
            return

        cleanup_rate = self._clamped_rate(settings.REQUEST_LOG_RETENTION_CLEANUP_SAMPLE_RATE)
        if random() >= cleanup_rate:
            return

        cutoff = datetime.now() - timedelta(days=retention_days)
        await session.execute(delete(LogData).where(LogData.created_at < cutoff))

    async def _request_log_worker(self) -> None:
        flush_interval = max(0.1, float(settings.REQUEST_LOG_FLUSH_INTERVAL_SECONDS))
        batch_size = max(1, int(settings.REQUEST_LOG_FLUSH_BATCH_SIZE))
        queue = self._request_log_queue
        if queue is None:
            return

        while not self._request_log_worker_stop.is_set() or not queue.empty():
            batch = await self._dequeue_request_log_batch(
                queue=queue,
                batch_size=batch_size,
                timeout_seconds=flush_interval,
            )
            if not batch:
                continue
            await self._persist_request_log_batch(batch=batch)

    async def _dequeue_request_log_batch(
        self,
        *,
        queue: asyncio.Queue[RequestLogEvent],
        batch_size: int,
        timeout_seconds: float,
    ) -> list[RequestLogEvent]:
        batch: list[RequestLogEvent] = []
        try:
            first_item = await asyncio.wait_for(queue.get(), timeout=timeout_seconds)
            batch.append(first_item)
        except asyncio.TimeoutError:
            return batch

        while len(batch) < batch_size:
            try:
                batch.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    async def _persist_request_log_batch(self, *, batch: list[RequestLogEvent]) -> None:
        try:
            async with sqlite_async_session() as session:
                await self._cleanup_old_request_logs(session)
                session.add_all(
                    [
                        LogData(
                            url=item.url,
                            method=item.method,
                            time=item.process_time,
                            status=item.status,
                            response=item.metadata,
                            created_at=item.created_at,
                        )
                        for item in batch
                    ]
                )
                await session.commit()
        except Exception:
            self._logger.exception("Failed to persist request log batch")
