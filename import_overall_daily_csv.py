"""Backfill Overall Daily Data from CSV exports. Pass --apply to write."""

import argparse
import asyncio
import csv
import re
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.exc import SQLAlchemyError

from app.db.models.external_api import AllDepo, AllSubscription, FirstDepo, FirstSubs
from app.db.session import sqlite_engine
from app.etl import load, quality, transform


DATASETS = {
    "ALL DEPO": (AllDepo, transform.parse_all_depo_dataframe, quality.validate_all_depo_dataframe, load.build_all_depo_rows),
    "FIRST DEPO": (FirstDepo, transform.parse_first_depo_dataframe, quality.validate_first_depo_dataframe, load.build_first_depo_rows),
    "ALL SUBS": (AllSubscription, transform.parse_all_subscription_dataframe, quality.validate_all_subscription_dataframe, load.build_all_subscription_rows),
    "FIRST SUBS": (FirstSubs, transform.parse_first_subs_dataframe, quality.validate_first_subs_dataframe, load.build_first_subs_rows),
}
EXPORT_NAME = re.compile(r"(\d{4}) - Overall Daily Data\s*- \1 (ALL DEPO|FIRST DEPO|ALL SUBS|FIRST SUBS)\.csv")
CSV_DIRECTORY = Path(__file__).resolve().parent / "csv"


def normalize_export(raw_rows: list[list[str]], tab: str) -> None:
    for row in raw_rows[1:]:
        if row and row[0].strip().isdigit():
            row[0] = (date(1899, 12, 30) + timedelta(days=int(row[0]))).isoformat()

    if tab == "ALL SUBS" and raw_rows:
        headers = {" ".join(value.lower().split()): index for index, value in enumerate(raw_rows[0])}
        for metric in ("total subscription", "new subscription"):
            qty = headers.get(f"{metric} (qty)")
            amount = headers.get(f"{metric} (amount)")
            if qty is None or amount is None:
                continue
            for row in raw_rows[1:]:
                if len(row) > max(qty, amount) and row[qty].strip() == "0" and not row[amount].strip():
                    row[amount] = "0"


def read_exports(directory: Path) -> dict:
    datasets = {}
    seen_dates = {}
    pull_date = date.today()
    for path in sorted(directory.glob("*.csv")):
        match = EXPORT_NAME.fullmatch(path.name)
        if match is None:
            continue
        year, tab = match.groups()
        model, parse, validate, build = DATASETS[tab]
        try:
            with path.open(encoding="utf-8-sig", newline="") as source:
                raw_rows = list(csv.reader(source, strict=True))
            normalize_export(raw_rows, tab)
            frame = parse(raw_rows)
            if frame.empty or any(day.year != int(year) for day in frame["date"]):
                raise ValueError(f"Expected non-empty data for {year}.")
            validate(frame)
            dates = set(frame["date"])
            known = seen_dates.setdefault(model, set())
            if dates & known:
                raise ValueError("Duplicate dates across CSV exports.")
            known.update(dates)
            datasets.setdefault(model, []).extend(build(frame, pull_date))
        except (ValueError, OverflowError, csv.Error) as exc:
            raise ValueError(f"{path.name}: {exc}") from exc
    if not datasets:
        raise ValueError(f"No matching Overall Daily Data CSV exports in {directory}.")
    return datasets


async def backfill(datasets: dict, apply: bool, *, report: bool = False) -> int:
    missing_count = 0
    async with sqlite_engine.begin() as connection:
        if apply:
            await connection.exec_driver_sql("BEGIN IMMEDIATE")
        for model, rows in datasets.items():
            table = model.__table__
            first, last = min(row["date"] for row in rows), max(row["date"] for row in rows)
            existing = set((await connection.scalars(
                select(table.c.date).where(table.c.date.between(first, last))
            )).all())
            pending = [row for row in rows if row["date"] not in existing]
            missing_count += len(pending)
            if apply and pending:
                statement = insert(table).on_conflict_do_nothing(index_elements=[table.c.date])
                await connection.execute(statement, pending)
            if report:
                action = "inserted" if apply else "to insert"
                print(f"{table.name}: {first} to {last}, {len(pending):,} {action}, "
                      f"{len(rows) - len(pending):,} existing dates kept")
    return missing_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path, default=CSV_DIRECTORY)
    parser.add_argument("--apply", action="store_true", help="Write missing dates to the configured database.")
    args = parser.parse_args()
    try:
        datasets = read_exports(args.directory)
        print(f"Database: {sqlite_engine.url.render_as_string(hide_password=True)}")
        asyncio.run(backfill(datasets, args.apply, report=True))
    except (OSError, ValueError, SQLAlchemyError) as exc:
        parser.exit(1, f"Import failed: {exc}\n")
    print("Import committed." if args.apply else "Dry run complete. Pass --apply to insert the missing dates.")


if __name__ == "__main__":
    main()
