"""Durable Adjust inbox and normalized install facts, independent of store reports."""
from sqlalchemy import Column, Date, DateTime, Index, Integer, JSON, String
from app.db.base import SqliteBase


class AdjustCallbackRaw(SqliteBase):
    __tablename__ = "adjust_callback_raw"
    __table_args__ = (Index("ix_adjust_raw_status_id", "status", "id"),)
    id = Column(Integer, primary_key=True, autoincrement=True)
    payload_hash = Column(String(64), nullable=False, unique=True)
    payload = Column(JSON, nullable=False)
    received_at = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=False, default="pending")
    attempts = Column(Integer, nullable=False, default=0)
    error_detail = Column(String, nullable=True)
    processed_at = Column(DateTime, nullable=True)
    run_id = Column(String, nullable=True)


class AdjustInstall(SqliteBase):
    __tablename__ = "adjust_install"
    install_key = Column(String(64), primary_key=True)
    app_token = Column(String, nullable=False)
    environment = Column(String, nullable=False, index=True)
    adid = Column(String, nullable=False)
    installed_at = Column(DateTime, nullable=False)
    date = Column(Date, nullable=False, index=True)
    os_name = Column(String, nullable=True)
    source_name = Column(String, nullable=True)
    campaign_id = Column(String, nullable=True, index=True)
    campaign_name = Column(String, nullable=True)
    adgroup_name = Column(String, nullable=True)
    creative_name = Column(String, nullable=True)
    tracker_token = Column(String, nullable=True)
    tracker_name = Column(String, nullable=True)
    raw_id = Column(Integer, nullable=False)
