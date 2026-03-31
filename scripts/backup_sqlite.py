"""Create consistent SQLite backups with retention management."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

from sqlalchemy.engine import make_url

from app.core.config import settings


def resolve_sqlite_db_path(db_url: str) -> Path:
    """Resolve the filesystem path for a file-backed SQLite database URL."""
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        raise ValueError("SQLite backup script only supports SQLite database URLs.")

    database = (url.database or "").strip()
    if database in {"", ":memory:"} or "mode=memory" in database:
        raise ValueError("In-memory SQLite databases cannot be backed up to disk.")

    return Path(database).expanduser()


def backup_sqlite_database(db_path: Path, output_dir: Path, keep: int) -> Path:
    """Create a timestamped SQLite backup and prune old snapshots."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = output_dir / f"{db_path.stem}-{timestamp}{db_path.suffix}"

    source = sqlite3.connect(db_path)
    destination = sqlite3.connect(backup_path)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()

    backups = sorted(output_dir.glob(f"{db_path.stem}-*{db_path.suffix}"))
    for old_backup in backups[:-keep]:
        old_backup.unlink(missing_ok=True)

    return backup_path


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for the SQLite backup helper."""
    parser = argparse.ArgumentParser(
        description="Create a safe SQLite backup snapshot with retention.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Override SQLite database path. Defaults to DB_URL from app settings.",
    )
    parser.add_argument(
        "--output-dir",
        default="backups/sqlite",
        help="Directory where timestamped backups will be stored.",
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=14,
        help="How many backup snapshots to retain.",
    )
    return parser


def main() -> int:
    """Run the backup workflow and print the generated file path."""
    args = build_parser().parse_args()
    db_path = Path(args.db_path).expanduser() if args.db_path else resolve_sqlite_db_path(settings.DB_URL)
    backup_path = backup_sqlite_database(
        db_path=db_path,
        output_dir=Path(args.output_dir).expanduser(),
        keep=max(args.keep, 1),
    )
    print(f"SQLite backup created: {backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
