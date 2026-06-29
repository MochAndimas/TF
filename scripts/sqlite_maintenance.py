"""SQLite maintenance helper for integrity checks, stats, backups, and vacuum."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

from sqlalchemy.engine import make_url

from app.core.config import settings


def resolve_sqlite_path(db_url: str) -> Path:
    """Resolve a file-backed SQLite path from a SQLAlchemy database URL."""
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        raise ValueError(f"Unsupported database backend: {url.get_backend_name()}")

    database = (url.database or "").strip()
    if database in {"", ":memory:"} or "mode=memory" in database:
        raise ValueError("Maintenance requires a file-backed SQLite database.")
    return Path(unquote(database)).expanduser().resolve()


def run_integrity_check(connection: sqlite3.Connection) -> str:
    """Run SQLite integrity_check and return the result string."""
    row = connection.execute("PRAGMA integrity_check").fetchone()
    return str(row[0]) if row else "unknown"


def table_row_counts(connection: sqlite3.Connection) -> list[tuple[str, int]]:
    """Return row counts for user tables in the SQLite database."""
    rows = connection.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    counts: list[tuple[str, int]] = []
    for (table_name,) in rows:
        quoted_table = table_name.replace('"', '""')
        count = connection.execute(f'SELECT COUNT(*) FROM "{quoted_table}"').fetchone()[0]
        counts.append((table_name, int(count)))
    return counts


def backup_database(source_path: Path, backup_dir: Path) -> Path:
    """Create a consistent SQLite backup and verify the copied database."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{source_path.stem}_{timestamp}.sqlite3"

    with sqlite3.connect(source_path) as source, sqlite3.connect(backup_path) as backup:
        source.backup(backup)

    with sqlite3.connect(backup_path) as backup:
        integrity = run_integrity_check(backup)
    if integrity.lower() != "ok":
        backup_path.unlink(missing_ok=True)
        raise RuntimeError(f"Backup verification failed: {integrity}")
    return backup_path


def vacuum_database(connection: sqlite3.Connection) -> None:
    """Run VACUUM on the active SQLite database connection."""
    connection.execute("VACUUM")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SQLite maintenance tasks.")
    parser.add_argument("--db-url", default=settings.DB_URL, help="SQLAlchemy SQLite URL.")
    parser.add_argument("--backup", action="store_true", help="Create and verify a backup.")
    parser.add_argument(
        "--backup-dir",
        default="backups/sqlite",
        help="Directory for verified SQLite backup files.",
    )
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after checks.")
    args = parser.parse_args()

    db_path = resolve_sqlite_path(args.db_url)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database does not exist: {db_path}")

    print(f"database={db_path}")
    print(f"size_bytes={db_path.stat().st_size}")
    with sqlite3.connect(db_path) as connection:
        integrity = run_integrity_check(connection)
        print(f"integrity_check={integrity}")
        if integrity.lower() != "ok":
            raise RuntimeError(f"SQLite integrity check failed: {integrity}")

        print("table_row_counts:")
        for table_name, count in table_row_counts(connection):
            print(f"  {table_name}={count}")

        if args.vacuum:
            vacuum_database(connection)
            print("vacuum=completed")

    if args.backup:
        backup_path = backup_database(db_path, Path(args.backup_dir))
        print(f"backup={backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
