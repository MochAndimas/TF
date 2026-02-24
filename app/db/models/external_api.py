from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float, Date
from app.db.base import SqliteBase


class DataDepo(SqliteBase):
    """
    """
    __tablename__ = "data_depo"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    campaign_id = Column("campaign_id", String, nullable=False)
    campaign_name = Column("campaign_name", String, nullable=False)
    status = Column("status", String, nullable=False)
    email = Column("email", String, nullable=False)
    first_depo = Column("first_depo", String, nullable=False)
    bulan = Column("bulan", String, nullable=False)
    pull_date = Column("pull_date", Date, nullable=False)


class GoogleAds(SqliteBase):
    """
    """
    __tablename__ = "google_ads"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", DateTime, nullable=False)
    campaign_name = Column("campaign_name", String, nullable=False)
    ad_group = Column("ad_group", String, nullable=False)
    ad_name = Column("ad_name", String, nullable=False)
    cost = Column("cost", Integer, nullable=True)
    impressions = Column("impressions", Integer, nullable=True)
    clicks = Column("clicks", Integer, nullable=True)
    leads = Column("leads", Integer, nullable=True)
