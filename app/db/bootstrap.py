"""Database bootstrap and readiness helpers for SQLite-backed deployments."""

from __future__ import annotations

import logging
from collections.abc import Callable
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.core.clock import now
from app.db.base import SqliteBase
import app.db.models  # noqa: F401
from app.db.session import sqlite_engine

logger = logging.getLogger(__name__)

REQUIRED_TABLES: tuple[str, ...] = (
    "tf_user",
    "user_token",
    "etl_run",
    "schema_migration",
)

MigrationHandler = Callable[[object], object]


async def _migration_20260624_001_auth_indexes(connection) -> None:
    """Create auth/request-log indexes required by current runtime flows."""
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


async def _migration_20260624_002_user_token_metadata(connection) -> None:
    """Add persisted session metadata columns for legacy SQLite databases."""
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


async def _migration_20260624_003_external_metric_columns(connection) -> None:
    """Apply additive external-metric schema changes for legacy databases."""
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


async def _migration_20260624_004_drop_deprecated_metric_columns(connection) -> None:
    """Drop deprecated metric columns that current transforms no longer write."""
    facebook_media_columns = {
        row[1]
        for row in (
            await connection.execute(text("PRAGMA table_info('facebook_page_media_insights')"))
        ).fetchall()
    }
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


async def _migration_20260624_005_etl_run_observability(connection) -> None:
    """Add ETL run observability metadata columns."""
    etl_run_columns = {
        row[1]
        for row in (await connection.execute(text("PRAGMA table_info('etl_run')"))).fetchall()
    }
    desired_columns = {
        "rows_extracted": "INTEGER",
        "rows_loaded": "INTEGER",
        "duration_ms": "INTEGER",
        "quality_report": "JSON",
    }
    for column_name, column_type in desired_columns.items():
        if column_name not in etl_run_columns:
            await connection.execute(
                text(f"ALTER TABLE etl_run ADD COLUMN {column_name} {column_type}")
            )


async def _migration_20260701_001_instagram_media_views(connection) -> None:
    """Rename Instagram media impressions to views and drop deprecated plays."""
    columns = {
        row[1]
        for row in (
            await connection.execute(text("PRAGMA table_info('instagram_media_insights')"))
        ).fetchall()
    }
    if not columns or ("impressions" not in columns and "plays" not in columns):
        return

    await connection.execute(text("DROP TABLE IF EXISTS instagram_media_insights_new"))
    await connection.execute(
        text(
            """
            CREATE TABLE instagram_media_insights_new (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                media_id VARCHAR NOT NULL,
                media_type VARCHAR NOT NULL,
                media_product_type VARCHAR NOT NULL,
                timestamp DATETIME,
                caption VARCHAR,
                permalink VARCHAR,
                media_url VARCHAR,
                thumbnail_url VARCHAR,
                likes INTEGER NOT NULL DEFAULT 0,
                comments INTEGER NOT NULL DEFAULT 0,
                shares INTEGER NOT NULL DEFAULT 0,
                saves INTEGER NOT NULL DEFAULT 0,
                reach INTEGER NOT NULL DEFAULT 0,
                views INTEGER NOT NULL DEFAULT 0,
                profile_visits INTEGER NOT NULL DEFAULT 0,
                follows INTEGER NOT NULL DEFAULT 0,
                total_engagement INTEGER NOT NULL DEFAULT 0,
                pull_date DATE NOT NULL,
                CONSTRAINT uq_instagram_media_insights_media_id UNIQUE (media_id)
            )
            """
        )
    )

    views_expression = "views" if "views" in columns else "impressions"
    await connection.execute(
        text(
            f"""
            INSERT INTO instagram_media_insights_new (
                id, date, media_id, media_type, media_product_type, timestamp, caption,
                permalink, media_url, thumbnail_url, likes, comments, shares, saves,
                reach, views, profile_visits, follows, total_engagement, pull_date
            )
            SELECT
                id, date, media_id, media_type, media_product_type, timestamp, caption,
                permalink, media_url, thumbnail_url, likes, comments, shares, saves,
                reach, COALESCE({views_expression}, 0), 0, 0, total_engagement, pull_date
            FROM instagram_media_insights
            """
        )
    )
    await connection.execute(text("DROP TABLE instagram_media_insights"))
    await connection.execute(text("ALTER TABLE instagram_media_insights_new RENAME TO instagram_media_insights"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_instagram_media_insights_date ON instagram_media_insights(date)"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_instagram_media_insights_media_type ON instagram_media_insights(media_type)"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_instagram_media_insights_media_product_type ON instagram_media_insights(media_product_type)"))


async def _migration_20260701_002_instagram_media_profile_actions(connection) -> None:
    """Add Feed-level profile action metrics to Instagram media insights."""
    columns = {
        row[1]
        for row in (
            await connection.execute(text("PRAGMA table_info('instagram_media_insights')"))
        ).fetchall()
    }
    if not columns:
        return
    for column_name in ("profile_visits", "follows"):
        if column_name not in columns:
            await connection.execute(
                text(
                    f"ALTER TABLE instagram_media_insights "
                    f"ADD COLUMN {column_name} INTEGER NOT NULL DEFAULT 0"
                )
            )


async def _migration_20260715_001_daily_register_tag_name(connection) -> None:
    """Store daily register totals at date + campaign + tag grain."""
    columns = {
        row[1]
        for row in (await connection.execute(text("PRAGMA table_info('daily_register')"))).fetchall()
    }
    if not columns or "tag_name" in columns:
        return

    await connection.execute(text("DROP TABLE IF EXISTS daily_register_new"))
    await connection.execute(
        text(
            """
            CREATE TABLE daily_register_new (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                campaign_id VARCHAR NOT NULL,
                tag_name VARCHAR NOT NULL DEFAULT 'CP1',
                total_regis INTEGER NOT NULL DEFAULT 0,
                pull_date DATE NOT NULL,
                CONSTRAINT uq_daily_register_date_campaign_tag UNIQUE (date, campaign_id, tag_name),
                FOREIGN KEY(campaign_id) REFERENCES campaign (campaign_id)
            )
            """
        )
    )
    await connection.execute(
        text(
            """
            INSERT INTO daily_register_new (date, campaign_id, tag_name, total_regis, pull_date)
            SELECT date, campaign_id, 'CP1', SUM(total_regis), MAX(pull_date)
            FROM daily_register
            GROUP BY date, campaign_id
            """
        )
    )
    await connection.execute(text("DROP TABLE daily_register"))
    await connection.execute(text("ALTER TABLE daily_register_new RENAME TO daily_register"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_daily_register_date ON daily_register(date)"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_daily_register_campaign_id ON daily_register(campaign_id)"))
    await connection.execute(text("CREATE INDEX IF NOT EXISTS ix_daily_register_tag_name ON daily_register(tag_name)"))


SCHEMA_MIGRATIONS: tuple[tuple[str, str, MigrationHandler], ...] = (
    (
        "20260624_001_auth_indexes",
        "Create auth, token, audit, rate-limit, and request-log indexes.",
        _migration_20260624_001_auth_indexes,
    ),
    (
        "20260624_002_user_token_metadata",
        "Add persisted session metadata columns to user_token.",
        _migration_20260624_002_user_token_metadata,
    ),
    (
        "20260624_003_external_metric_columns",
        "Add current external metric columns for Instagram and Facebook insights.",
        _migration_20260624_003_external_metric_columns,
    ),
    (
        "20260624_004_drop_deprecated_metric_columns",
        "Drop deprecated Facebook and YouTube metric columns when present.",
        _migration_20260624_004_drop_deprecated_metric_columns,
    ),
    (
        "20260624_005_etl_run_observability",
        "Add ETL run row-count, duration, and quality-report metadata.",
        _migration_20260624_005_etl_run_observability,
    ),
    (
        "20260701_001_instagram_media_views",
        "Rename Instagram media impressions to views and drop plays.",
        _migration_20260701_001_instagram_media_views,
    ),
    (
        "20260701_002_instagram_media_profile_actions",
        "Add Instagram media profile visits and follows metrics.",
        _migration_20260701_002_instagram_media_profile_actions,
    ),
    (
        "20260715_001_daily_register_tag_name",
        "Store daily register totals by tag name.",
        _migration_20260715_001_daily_register_tag_name,
    ),
)


async def initialize_database_schema() -> None:
    """Create core tables and apply lightweight SQLite schema maintenance."""
    logger.info("Initializing database schema")
    async with sqlite_engine.begin() as connection:
        await connection.run_sync(SqliteBase.metadata.create_all)
        await apply_schema_migrations(connection)


async def apply_schema_migrations(connection) -> list[str]:
    """Apply unapplied lightweight SQLite schema migrations in order."""
    await connection.run_sync(SqliteBase.metadata.create_all)
    applied_rows = (
        await connection.execute(text("SELECT migration_id FROM schema_migration"))
    ).fetchall()
    applied_ids = {row[0] for row in applied_rows}
    newly_applied: list[str] = []

    for migration_id, description, handler in SCHEMA_MIGRATIONS:
        if migration_id in applied_ids:
            continue
        logger.info("Applying schema migration %s", migration_id)
        await handler(connection)
        await connection.execute(
            text(
                "INSERT INTO schema_migration "
                "(migration_id, description, applied_at) "
                "VALUES (:migration_id, :description, :applied_at)"
            ),
            {
                "migration_id": migration_id,
                "description": description,
                "applied_at": now(),
            },
        )
        newly_applied.append(migration_id)
    return newly_applied


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
