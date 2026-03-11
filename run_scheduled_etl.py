"""Run ETL jobs sequentially so the script can be scheduled by cron or Task Scheduler."""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Sequence

from fastapi import HTTPException

from app.etl.job_runner import DEFAULT_SCHEDULED_SOURCES, trigger_and_wait_update_job


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the daily ETL sequence for Traders Family data sources.",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=list(DEFAULT_SCHEDULED_SOURCES),
        help="Ordered source list to execute. Default runs the standard daily ETL sequence.",
    )
    parser.add_argument(
        "--triggered-by",
        default="scheduler",
        help="Actor label persisted into etl_run.triggered_by.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop execution immediately after the first failed source.",
    )
    return parser


async def run_sources(
    *,
    sources: Sequence[str],
    triggered_by: str,
    fail_fast: bool,
) -> int:
    failures = 0
    logging.info("Starting scheduled ETL for sources: %s", ", ".join(sources))

    for source in sources:
        try:
            result = await trigger_and_wait_update_job(
                data=source,
                types="auto",
                triggered_by=triggered_by,
            )
            if result.get("success"):
                logging.info(
                    "[SUCCESS] source=%s run_id=%s message=%s",
                    source,
                    result.get("run_id"),
                    result.get("message"),
                )
            else:
                failures += 1
                logging.error(
                    "[FAILED] source=%s run_id=%s error=%s",
                    source,
                    result.get("run_id"),
                    result.get("error"),
                )
                if fail_fast:
                    break
        except HTTPException as error:
            failures += 1
            logging.error("[FAILED] source=%s error=%s", source, error.detail)
            if fail_fast:
                break
        except Exception as error:
            failures += 1
            logging.exception("[FAILED] source=%s error=%s", source, error)
            if fail_fast:
                break

    if failures:
        logging.error("Scheduled ETL finished with %s failure(s).", failures)
        return 1

    logging.info("Scheduled ETL finished successfully.")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return asyncio.run(
        run_sources(
            sources=args.sources,
            triggered_by=args.triggered_by,
            fail_fast=args.fail_fast,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
