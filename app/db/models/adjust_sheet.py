"""Delivery receipts for append-only Adjust register exports."""
from sqlalchemy import Column, DateTime, String
from app.db.base import SqliteBase


class AdjustRegisterSheetReceipt(SqliteBase):
    __tablename__ = "adjust_register_sheet_receipt"
    destination = Column(String(64), primary_key=True)
    event_key = Column(String(64), primary_key=True)
    delivered_at = Column(DateTime, nullable=False)
