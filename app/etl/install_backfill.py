"""One-time deployment bootstrap for bundled Apple and Play install CSVs.

This module is intentionally called by ``init_db.py`` rather than the daily
scheduler. The upserts are idempotent, so rebuilding/restarting ``db-init`` is
safe and never requires a separate backfill flag in ``.env``.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from decouple import config

from app.db.session import sqlite_async_session
from app.etl.load import (
    build_apple_install_rows,
    build_play_console_install_rows,
    upsert_apple_install_rows,
    upsert_play_console_install_rows,
)
from app.etl.quality import (
    validate_apple_install_dataframe,
    validate_play_console_install_dataframe,
)
from app.etl.transform import (
    parse_apple_install_dataframe,
    parse_play_console_install_dataframe,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APPLE_BACKFILL_CSV = PROJECT_ROOT / "apple_backfill.csv"
PLAY_CONSOLE_BACKFILL_CSV = PROJECT_ROOT / "backfill_install.csv"


def _parse_apple_date(raw_value: str) -> date | None:
    normalized = str(raw_value).strip()
    for date_format in ("%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    return None


def _parse_play_date(raw_value: str) -> date | None:
    normalized = str(raw_value).strip().strip('"')
    for date_format in ("%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, date_format).date()
        except ValueError:
            continue
    return None


def _parse_count(raw_value) -> int:
    normalized = str(raw_value or "0").replace(",", "").strip()
    return int(float(normalized or "0"))


def load_apple_backfill_rows(path: Path = APPLE_BACKFILL_CSV) -> list[dict]:
    """Read every historical Apple download row bundled with the image."""
    if not path.is_file():
        raise FileNotFoundError(f"Apple backfill CSV is missing: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        csv_rows = list(csv.reader(handle))
    header_index = next(
        (index for index, row in enumerate(csv_rows) if row and row[0].strip().lower() == "date"),
        None,
    )
    if header_index is None:
        raise ValueError(f"Apple backfill CSV has no Date header: {path}")

    processing_date = datetime.now(timezone.utc).date().isoformat()
    normalized_csv = "\n".join(",".join(row) for row in csv_rows[header_index:])
    reader = csv.DictReader(io.StringIO(normalized_csv))
    rows: list[dict] = []
    for row in reader:
        row_date = _parse_apple_date(row.get("Date", ""))
        if row_date is None:
            continue
        for download_type, raw_count in (
            ("First-time download", row.get("First-Time Downloads", 0)),
            ("Redownload", row.get("Redownloads", 0)),
        ):
            rows.append(
                {
                    "Date": row_date.isoformat(),
                    "Download Type": download_type,
                    "Counts": _parse_count(raw_count),
                    "_apple_report": "downloads",
                    "_apple_report_name": "Apple Downloads Backfill",
                    "_apple_processing_date": processing_date,
                }
            )
    return rows


def load_play_console_backfill_rows(
    package_name: str,
    path: Path = PLAY_CONSOLE_BACKFILL_CSV,
) -> list[dict]:
    """Read the complete bundled Play Console CSV without a hard-coded cutoff."""
    if not package_name.strip():
        raise ValueError("PLAY_CONSOLE_NAME_APP is required for Play Console backfill rows.")
    if not path.is_file():
        raise FileNotFoundError(f"Play Console backfill CSV is missing: {path}")

    rows: list[dict] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or len(reader.fieldnames) < 2:
            raise ValueError(f"Play Console backfill CSV has invalid headers: {path}")
        date_column, installers_column = reader.fieldnames[:2]
        for row in reader:
            row_date = _parse_play_date(row.get(date_column, ""))
            if row_date is None:
                continue
            rows.append(
                {
                    "date": row_date.isoformat(),
                    "package_name": package_name.strip(),
                    "country": "all",
                    "installers": _parse_count(row.get(installers_column, 0)),
                    "uninstallers": 0,
                    "active_devices": 0,
                    "source_object": "stats/installs/backfill_install_overview.csv",
                }
            )
    return rows


async def run_install_backfills() -> tuple[int, int]:
    """Upsert both bundled histories during database initialization."""
    package_name = config("PLAY_CONSOLE_NAME_APP", default="", cast=str).strip()
    apple_df = parse_apple_install_dataframe(load_apple_backfill_rows())
    play_df = parse_play_console_install_dataframe(
        load_play_console_backfill_rows(package_name)
    )
    validate_apple_install_dataframe(apple_df)
    validate_play_console_install_dataframe(play_df)

    pull_date = datetime.now(timezone.utc).date()
    apple_rows = build_apple_install_rows(apple_df, pull_date=pull_date)
    play_rows = build_play_console_install_rows(play_df, pull_date=pull_date)
    async with sqlite_async_session() as session:
        await upsert_apple_install_rows(session, apple_rows)
        await upsert_play_console_install_rows(session, play_rows)
        await session.commit()

    logger.info(
        "Install CSV bootstrap completed: apple_rows=%s play_rows=%s",
        len(apple_rows),
        len(play_rows),
    )
    return len(apple_rows), len(play_rows)
