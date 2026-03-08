"""ETL run tracking models.

This module stores execution metadata for ETL trigger requests.
"""

from sqlalchemy import Column, Date, DateTime, Index, Integer, String

from app.db.base import SqliteBase


class EtlRun(SqliteBase):
    """Track ETL pipeline execution lifecycle."""

    __tablename__ = "etl_run"
    __table_args__ = (
        Index("ix_etl_runs_status", "status"),
        Index("ix_etl_runs_source_window", "source", "window_start", "window_end"),
        Index("ix_etl_runs_started_at", "started_at"),
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    run_id = Column("run_id", String, nullable=False, unique=True)
    pipeline = Column("pipeline", String, nullable=False)
    source = Column("source", String, nullable=False)
    mode = Column("mode", String, nullable=False)
    window_start = Column("window_start", Date, nullable=True)
    window_end = Column("window_end", Date, nullable=True)
    status = Column("status", String, nullable=False)
    message = Column("message", String, nullable=True)
    error_detail = Column("error_detail", String, nullable=True)
    started_at = Column("started_at", DateTime, nullable=False)
    ended_at = Column("ended_at", DateTime, nullable=True)
    triggered_by = Column("triggered_by", String, nullable=True)
