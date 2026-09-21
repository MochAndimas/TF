"""Shared comparison policy, with request-local context for analytics services."""
from contextvars import ContextVar
from datetime import date, timedelta
from typing import Literal

ComparisonMode = Literal["previous_period", "month_to_date", "previous_month"]
comparison_mode: ContextVar[str] = ContextVar("analytics_comparison", default="previous_period")


def previous_period_range(start: date, end: date, mode: str | None = None) -> tuple[date, date]:
    mode = mode or comparison_mode.get()
    if start > end:
        raise ValueError("Start date cannot be after end date.")
    if mode in {"month_to_date", "previous_month"}:
        previous_end = start.replace(day=1) - timedelta(days=1)
        previous_start = previous_end.replace(day=1)
        if mode == "month_to_date":
            previous_end = previous_end.replace(day=min(end.day, previous_end.day))
        return previous_start, previous_end
    if mode != "previous_period":
        raise ValueError("Unsupported comparison mode.")
    return start - timedelta(days=(end - start).days + 1), start - timedelta(days=1)


def growth_percentage(current: float, previous: float) -> float:
    # Preserve the dashboard convention for a newly nonzero metric.
    if previous == 0:
        return 100.0 if current > 0 else -100.0 if current < 0 else 0.0
    return round((current - previous) / abs(previous) * 100, 2)


class AnalyticsComparisonMiddleware:
    """Scope comparison selection to a single request, including concurrent loaders."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        from starlette.requests import Request
        from starlette.responses import JSONResponse
        mode = Request(scope).query_params.get("comparison", "previous_period")
        if mode not in {"previous_period", "month_to_date", "previous_month"}:
            return await JSONResponse({"detail": "Invalid comparison mode."}, status_code=422)(scope, receive, send)
        token = comparison_mode.set(mode)
        try:
            await self.app(scope, receive, send)
        finally:
            comparison_mode.reset(token)
