"""Database bootstrap and readiness helpers for SQLite-backed deployments."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.db.base import SqliteBase
import app.db.models  # noqa: F401
from app.db.session import sqlite_engine

logger = logging.getLogger(__name__)

REQUIRED_TABLES: tuple[str, ...] = (
    "tf_user",
    "user_token",
    "etl_run",
)


async def initialize_database_schema() -> None:
    """Create core tables and apply lightweight SQLite schema maintenance."""
    logger.info("Initializing database schema")
    async with sqlite_engine.begin() as connection:
        await connection.run_sync(SqliteBase.metadata.create_all)
        await apply_schema_maintenance(connection)


async def apply_schema_maintenance(connection) -> None:
    """Apply small additive schema changes for legacy SQLite databases."""
    user_token_columns = {
        row[1]
        for row in (await connection.execute(text("PRAGMA table_info('user_token')"))).fetchall()
    }
    desired_columns = {
        "created_ip": "TEXT",
        "last_seen_ip": "TEXT",
        "last_seen_user_agent": "TEXT",
        "last_rotated_at": "DATETIME",
    }
    for column_name, column_type in desired_columns.items():
        if column_name not in user_token_columns:
            await connection.execute(
                text(f"ALTER TABLE user_token ADD COLUMN {column_name} {column_type}")
            )

    auth_indexes = (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_tf_user_email ON tf_user(email)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_token_session_id ON user_token(session_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_token_access_token ON user_token(access_token)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_token_refresh_token ON user_token(refresh_token)",
        "CREATE INDEX IF NOT EXISTS ix_user_token_user_id ON user_token(user_id)",
        "CREATE INDEX IF NOT EXISTS ix_log_data_created_at ON log_data(created_at)",
        "CREATE INDEX IF NOT EXISTS ix_auth_audit_event_created_at ON auth_audit_event(created_at)",
        "CREATE INDEX IF NOT EXISTS ix_auth_audit_event_event_type ON auth_audit_event(event_type)",
        "CREATE INDEX IF NOT EXISTS ix_auth_rate_limit_event_bucket_key ON auth_rate_limit_event(bucket_key)",
        "CREATE INDEX IF NOT EXISTS ix_auth_rate_limit_event_request_at ON auth_rate_limit_event(request_at)",
    )
    for ddl in auth_indexes:
        await connection.execute(text(ddl))

    instagram_insights_columns = {
        row[1]
        for row in (await connection.execute(text("PRAGMA table_info('instagram_insights')"))).fetchall()
    }
    if instagram_insights_columns and "unfollowers" not in instagram_insights_columns:
        await connection.execute(
            text("ALTER TABLE instagram_insights ADD COLUMN unfollowers INTEGER NOT NULL DEFAULT 0")
        )

    facebook_media_columns = {
        row[1]
        for row in (
            await connection.execute(text("PRAGMA table_info('facebook_page_media_insights')"))
        ).fetchall()
    }
    if facebook_media_columns and "post_media_view" not in facebook_media_columns:
        await connection.execute(
            text(
                "ALTER TABLE facebook_page_media_insights "
                "ADD COLUMN post_media_view INTEGER NOT NULL DEFAULT 0"
            )
        )
        facebook_media_columns.add("post_media_view")

    deprecated_facebook_media_columns = (
        "post_impressions",
        "post_impressions_unique",
        "post_impressions_paid",
        "post_impressions_organic",
        "post_engaged_users",
    )
    for column_name in deprecated_facebook_media_columns:
        if column_name in facebook_media_columns:
            await connection.execute(
                text(f"ALTER TABLE facebook_page_media_insights DROP COLUMN {column_name}")
            )

    youtube_media_columns = {
        row[1]
        for row in (
            await connection.execute(text("PRAGMA table_info('youtube_media_insight')"))
        ).fetchall()
    }
    for column_name in ("thumbnail_impressions", "thumbnail_ctr"):
        if column_name in youtube_media_columns:
            await connection.execute(
                text(f"ALTER TABLE youtube_media_insight DROP COLUMN {column_name}")
            )


async def verify_database_ready(
    *,
    required_tables: Sequence[str] = REQUIRED_TABLES,
) -> None:
    """Verify that the DB is reachable and required schema already exists."""
    try:
        async with sqlite_engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
            existing_tables = {
                row[0]
                for row in (
                    await connection.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    )
                ).fetchall()
            }
    except OperationalError as error:
        raise RuntimeError(f"Database is not reachable: {error}") from error

    missing_tables = [table for table in required_tables if table not in existing_tables]
    if missing_tables:
        raise RuntimeError(
            "Database schema is not initialized. Missing tables: "
            + ", ".join(missing_tables)
            + ". Run `python init_db.py` before starting the app."
        )
