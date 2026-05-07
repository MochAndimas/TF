"""Small HTTP helpers for authentication endpoints."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
from threading import Lock

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.db.models.etl_run import EtlRun
from app.db.models.user import TfUser
from app.schemas.user import AccountSummary, LatestEtlRunSummary

_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = defaultdict(deque)
_RATE_LIMIT_LOCK = Lock()


def serialize_account(user: TfUser) -> AccountSummary:
    """Convert one SQLAlchemy user model into a response-safe account payload."""
    return AccountSummary(
        user_id=user.user_id,
        fullname=user.fullname,
        email=user.email,
        role=user.role,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


def serialize_latest_run(run: EtlRun | None) -> LatestEtlRunSummary | None:
    """Convert the latest ETL run model into a compact API payload."""
    if run is None:
        return None
    return LatestEtlRunSummary(
        run_id=run.run_id,
        pipeline=run.pipeline,
        source=run.source,
        mode=run.mode,
        status=run.status,
        message=run.message,
        error_detail=run.error_detail,
        window_start=run.window_start,
        window_end=run.window_end,
        started_at=run.started_at,
        ended_at=run.ended_at,
        triggered_by=run.triggered_by,
    )


def set_auth_session_cookie(response: JSONResponse, session_id: str) -> None:
    """Persist the opaque session handle as an HttpOnly cookie."""
    response.set_cookie(
        key=settings.auth_cookie_name,
        value=session_id,
        max_age=settings.auth_cookie_max_age,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        path="/",
    )


def clear_auth_session_cookie(response: JSONResponse) -> None:
    """Expire the persistent auth cookie in the browser."""
    response.delete_cookie(
        key=settings.auth_cookie_name,
        httponly=True,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        path="/",
    )


def _client_identifier(request: Request, scope: str, extra: str | None = None) -> str:
    """Build a stable in-memory rate-limit key for one client/scope pair."""
    client_host = request.client.host if request.client else "unknown"
    suffix = f":{extra.strip().lower()}" if extra else ""
    return f"{scope}:{client_host}{suffix}"


def enforce_rate_limit(
    request: Request,
    scope: str,
    max_requests: int,
    window_seconds: int,
    extra: str | None = None,
) -> None:
    """Enforce a simple sliding-window rate limit for sensitive auth actions."""
    now = datetime.now().timestamp()
    bucket_key = _client_identifier(request, scope, extra)
    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS[bucket_key]
        while bucket and now - bucket[0] > window_seconds:
            bucket.popleft()
        if len(bucket) >= max_requests:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many requests. Please try again later.",
            )
        bucket.append(now)
