from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float, Date
from app.db.base import SqliteBase


class GsheetApi(SqliteBase):
    """
    """
    __tablename__ = "google_sheet"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    campaign_id = Column("campaign_id", String, nullable=False)
    campaign_name = Column("campaign_name", String, nullable=False)
    status = Column("status", String, nullable=False)
    email = Column("email", String, nullable=False)
    first_depo = Column("first_depo", String, nullable=False)
    bulan = Column("bulan", String, nullable=False)
    pull_date = Column("pull_date", Date, nullable=False)
