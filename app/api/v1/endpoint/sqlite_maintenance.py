"""Admin endpoints for SQLite maintenance."""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.v1.endpoint.common import require_roles_dep
from app.db.models.user import TfUser
from app.schemas.sqlite_maintenance import (
    SqliteMaintenanceStats,
    SqliteVacuumResponse,
)
from app.utils.sqlite_maintenance import (
    SqliteMaintenanceStatus,
    get_sqlite_maintenance_status,
    vacuum_sqlite_database,
)

router = APIRouter()

SuperadminUser = Annotated[TfUser, Depends(require_roles_dep("superadmin"))]


def _serialize_status(
    status_payload: SqliteMaintenanceStatus,
    *,
    message: str,
) -> SqliteMaintenanceStats:
    return SqliteMaintenanceStats(
        success=True,
        message=message,
        database_size_bytes=status_payload.database_size_bytes,
        page_size_bytes=status_payload.page_size_bytes,
        page_count=status_payload.page_count,
        freelist_count=status_payload.freelist_count,
        reclaimable_bytes=status_payload.reclaimable_bytes,
        active_etl_runs=status_payload.active_etl_runs,
        stg_ads_raw_rows=status_payload.stg_ads_raw_rows,
    )


@router.get("/api/sqlite-maintenance/status", response_model=SqliteMaintenanceStats)
async def sqlite_maintenance_status(
    current_user: SuperadminUser,  # noqa: ARG001
) -> SqliteMaintenanceStats:
    """Return current SQLite maintenance stats."""
    try:
        status_payload = await asyncio.to_thread(get_sqlite_maintenance_status)
    except (FileNotFoundError, ValueError, sqlite3.Error) as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error
    return _serialize_status(status_payload, message="SQLite status loaded.")


@router.post("/api/sqlite-maintenance/vacuum", response_model=SqliteVacuumResponse)
async def sqlite_maintenance_vacuum(
    current_user: SuperadminUser,  # noqa: ARG001
) -> SqliteVacuumResponse:
    """Run VACUUM after checking active ETL jobs."""
    try:
        result = await asyncio.to_thread(vacuum_sqlite_database)
    except RuntimeError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error
    except (FileNotFoundError, ValueError, sqlite3.Error) as error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(error),
        ) from error

    return SqliteVacuumResponse(
        success=True,
        message="VACUUM completed successfully.",
        before=_serialize_status(result.before, message="Before VACUUM."),
        after=_serialize_status(result.after, message="After VACUUM."),
    )
