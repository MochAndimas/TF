"""SQLite schema migration tracking model."""

from sqlalchemy import Column, DateTime, String

from app.db.base import SqliteBase


class SchemaMigration(SqliteBase):
    """Record applied lightweight SQLite schema migrations."""

    __tablename__ = "schema_migration"

    migration_id = Column("migration_id", String, primary_key=True)
    description = Column("description", String, nullable=False)
    applied_at = Column("applied_at", DateTime, nullable=False)
