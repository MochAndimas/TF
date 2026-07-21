"""SQLite maintenance helpers used by admin endpoints."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

from sqlalchemy.engine import make_url

from app.core.config import settings


ACTIVE_ETL_STATUSES = ("queued", "running")


@dataclass(frozen=True)
class SqliteMaintenanceStatus:
    """Compact SQLite status payload for the admin maintenance UI."""

    database_size_bytes: int
    page_size_bytes: int
    page_count: int
    freelist_count: int
    reclaimable_bytes: int
    active_etl_runs: int
    stg_ads_raw_rows: int


@dataclass(frozen=True)
class SqliteVacuumResult:
    """Result payload returned after VACUUM completes."""

    before: SqliteMaintenanceStatus
    after: SqliteMaintenanceStatus


def resolve_sqlite_path(db_url: str | None = None) -> Path:
    """Resolve the configured file-backed SQLite path."""
    url = make_url(db_url or settings.DB_URL)
    if url.get_backend_name() != "sqlite":
        raise ValueError(f"Unsupported database backend: {url.get_backend_name()}")

    database = (url.database or "").strip()
    if database in {"", ":memory:"} or "mode=memory" in database:
        raise ValueError("Maintenance requires a file-backed SQLite database.")
    return Path(unquote(database)).expanduser().resolve()


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _count_active_etl_runs(connection: sqlite3.Connection) -> int:
    if not _table_exists(connection, "etl_run"):
        return 0
    placeholders = ",".join("?" for _ in ACTIVE_ETL_STATUSES)
    row = connection.execute(
        f"SELECT COUNT(*) FROM etl_run WHERE status IN ({placeholders})",
        ACTIVE_ETL_STATUSES,
    ).fetchone()
    return int(row[0]) if row else 0


def _count_stg_ads_raw_rows(connection: sqlite3.Connection) -> int:
    if not _table_exists(connection, "stg_ads_raw"):
        return 0
    row = connection.execute("SELECT COUNT(*) FROM stg_ads_raw").fetchone()
    return int(row[0]) if row else 0


def _read_status(connection: sqlite3.Connection, db_path: Path) -> SqliteMaintenanceStatus:
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
    return SqliteMaintenanceStatus(
        database_size_bytes=db_path.stat().st_size,
        page_size_bytes=page_size,
        page_count=page_count,
        freelist_count=freelist_count,
        reclaimable_bytes=page_size * freelist_count,
        active_etl_runs=_count_active_etl_runs(connection),
        stg_ads_raw_rows=_count_stg_ads_raw_rows(connection),
    )


def get_sqlite_maintenance_status() -> SqliteMaintenanceStatus:
    """Return current file size and SQLite page stats."""
    db_path = resolve_sqlite_path()
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database does not exist: {db_path}")

    with sqlite3.connect(db_path) as connection:
        return _read_status(connection, db_path)


def vacuum_sqlite_database() -> SqliteVacuumResult:
    """Run VACUUM when no ETL job is queued or running."""
    db_path = resolve_sqlite_path()
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database does not exist: {db_path}")

    with sqlite3.connect(db_path, isolation_level=None) as connection:
        connection.execute(f"PRAGMA busy_timeout = {settings.SQLITE_BUSY_TIMEOUT_MS}")
        before = _read_status(connection, db_path)
        if before.active_etl_runs:
            raise RuntimeError("VACUUM is blocked while ETL jobs are queued or running.")

        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("VACUUM")
        after = _read_status(connection, db_path)
    return SqliteVacuumResult(before=before, after=after)
