"""ETL run lifecycle helpers."""

from __future__ import annotations

import uuid
from datetime import date, datetime

from fastapi import HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.etl_run import EtlRun

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


async def _ensure_not_running_conflict(
    session: AsyncSession,
    *,
    source: str,
    mode: str,
    window_start: date | None,
    window_end: date | None,
) -> None:
    """Raise 409 when an equivalent ETL run is already running."""
    existing = await session.execute(
        select(EtlRun.run_id)
        .where(
            EtlRun.status == STATUS_RUNNING,
            EtlRun.source == source,
            EtlRun.mode == mode,
            EtlRun.window_start == window_start,
            EtlRun.window_end == window_end,
        )
        .limit(1)
    )
    active_run = existing.scalar_one_or_none()
    if active_run:
        window_label = f"{window_start} to {window_end}" if window_start and window_end else "unspecified window"
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"ETL run is already in progress for source '{source}' "
                f"({window_label}). Active run_id: {active_run}"
            ),
        )


async def start_run(
    session: AsyncSession,
    *,
    source: str,
    mode: str,
    window_start: date | None,
    window_end: date | None,
    triggered_by: str | None,
) -> str:
    """Create and persist a new ETL run in running state."""
    await _ensure_not_running_conflict(
        session=session,
        source=source,
        mode=mode,
        window_start=window_start,
        window_end=window_end,
    )
    run_id = str(uuid.uuid4())
    session.add(
        EtlRun(
            run_id=run_id,
            pipeline="external_api_sync",
            source=source,
            mode=mode,
            window_start=window_start,
            window_end=window_end,
            status=STATUS_RUNNING,
            started_at=datetime.now(),
            triggered_by=triggered_by,
        )
    )
    await session.commit()
    return run_id


async def finish_run(session: AsyncSession, run_id: str, message: str | None = None) -> None:
    """Mark an ETL run as success."""
    await session.execute(
        update(EtlRun)
        .where(EtlRun.run_id == run_id)
        .values(
            status=STATUS_SUCCESS,
            message=message,
            ended_at=datetime.now(),
            error_detail=None,
        )
    )
    await session.commit()


async def fail_run(session: AsyncSession, run_id: str, error_detail: str) -> None:
    """Mark an ETL run as failed."""
    await session.rollback()
    await session.execute(
        update(EtlRun)
        .where(EtlRun.run_id == run_id)
        .values(
            status=STATUS_FAILED,
            error_detail=error_detail[:1000],
            ended_at=datetime.now(),
        )
    )
    await session.commit()
