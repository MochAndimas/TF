"""Schemas for SQLite maintenance endpoints."""

from __future__ import annotations

from app.schemas.responses import ApiResponseV1


class SqliteMaintenanceStats(ApiResponseV1):
    """SQLite file and page statistics for admin maintenance."""

    database_size_bytes: int
    page_size_bytes: int
    page_count: int
    freelist_count: int
    reclaimable_bytes: int
    active_etl_runs: int
    stg_ads_raw_rows: int


class SqliteVacuumResponse(ApiResponseV1):
    """Before/after payload returned after VACUUM."""

    before: SqliteMaintenanceStats
    after: SqliteMaintenanceStats
