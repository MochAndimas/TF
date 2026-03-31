"""Explicit database initialization entrypoint.

Run this once before starting the API when AUTO_INIT_DB_ON_STARTUP is disabled.
"""

from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.db.bootstrap import initialize_database_schema, verify_database_ready


async def _run() -> None:
    settings.validate_runtime_constraints()
    await initialize_database_schema()
    await verify_database_ready()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(_run())
    logging.info("Database initialization completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
