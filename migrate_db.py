"""Apply lightweight SQLite schema migrations to an existing database."""

from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.db.bootstrap import apply_schema_migrations, verify_database_ready
from app.db.session import sqlite_engine


async def _run() -> list[str]:
    settings.validate_runtime_constraints()
    async with sqlite_engine.begin() as connection:
        applied = await apply_schema_migrations(connection)
    await verify_database_ready()
    return applied


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    applied = asyncio.run(_run())
    if applied:
        logging.info("Applied schema migrations: %s", ", ".join(applied))
    else:
        logging.info("No pending schema migrations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
