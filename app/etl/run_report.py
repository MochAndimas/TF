"""ETL run report helpers used by job execution and status endpoints."""

from __future__ import annotations

from typing import Any


def build_quality_report(
    *,
    source: str,
    status: str,
    message: str | None = None,
    error_detail: str | None = None,
    rows_extracted: int | None = None,
    rows_loaded: int | None = None,
    duration_ms: int | None = None,
) -> dict[str, Any]:
    """Build a stable quality-report payload for one ETL run."""
    checks: list[dict[str, Any]] = []
    if error_detail and "DQ failed:" in error_detail:
        checks.append(
            {
                "name": "data_quality",
                "status": "failed",
                "detail": error_detail[:500],
            }
        )
    elif status == "success":
        checks.append(
            {
                "name": "data_quality",
                "status": "passed",
                "detail": "Pipeline completed without data-quality exceptions.",
            }
        )

    return {
        "version": "etl_quality_report_v1",
        "source": source,
        "status": status,
        "message": message,
        "error_detail": error_detail[:1000] if error_detail else None,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "duration_ms": duration_ms,
        "checks": checks,
    }
