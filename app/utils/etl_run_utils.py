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
    """Reject duplicate active ETL runs for the same source/window.

    Args:
        session (AsyncSession): Active database session.
        source (str): ETL source identifier (for example ``google_ads``).
        mode (str): ETL mode (`auto` or `manual`).
        window_start (date | None): Inclusive run window start.
        window_end (date | None): Inclusive run window end.

    Returns:
        None: Validation-only helper.

    Raises:
        HTTPException: ``409`` when a matching running ETL run already exists.
    """
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
    """Create and persist a new ETL run in ``running`` status.

    Args:
        session (AsyncSession): Active database session.
        source (str): ETL source identifier.
        mode (str): ETL mode (`auto` or `manual`).
        window_start (date | None): Inclusive run window start.
        window_end (date | None): Inclusive run window end.
        triggered_by (str | None): User identifier that triggered the job.

    Returns:
        str: Generated ``run_id`` used for status polling.

    Raises:
        HTTPException: Propagated conflict error from duplicate active runs.
    """
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
    """Mark an ETL run as ``success`` and finalize metadata.

    Args:
        session (AsyncSession): Active database session.
        run_id (str): ETL run identifier.
        message (str | None): Optional completion message from pipeline.

    Returns:
        None: Persists status change as side effect.
    """
    result = await session.execute(
        update(EtlRun)
        .where(EtlRun.run_id == run_id)
        .values(
            status=STATUS_SUCCESS,
            message=message,
            ended_at=datetime.now(),
            error_detail=None,
        )
    )
    if result.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ETL run not found while finishing run_id: {run_id}",
        )
    await session.commit()


async def fail_run(session: AsyncSession, run_id: str, error_detail: str) -> None:
    """Mark an ETL run as ``failed`` and store truncated error detail.

    Args:
        session (AsyncSession): Active database session.
        run_id (str): ETL run identifier.
        error_detail (str): Error text persisted into run metadata.

    Returns:
        None: Persists status change as side effect.
    """
    await session.rollback()
    result = await session.execute(
        update(EtlRun)
        .where(EtlRun.run_id == run_id)
        .values(
            status=STATUS_FAILED,
            error_detail=error_detail[:1000],
            ended_at=datetime.now(),
        )
    )
    if result.rowcount == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ETL run not found while failing run_id: {run_id}",
        )
    await session.commit()
