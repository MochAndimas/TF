"""Daily ChatGPT Ads facts; aggregation level is part of the primary key."""
from sqlalchemy import Column, Date, DateTime, Integer, Numeric, String
from app.db.base import SqliteBase


class GptAdsDaily(SqliteBase):
    __tablename__ = "gpt_ads_daily"
    report_level = Column(String, primary_key=True)
    account_id = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    entity_id = Column(String, primary_key=True)
    campaign_id = Column(String, nullable=False, index=True)
    campaign_name = Column(String, nullable=True)
    ad_group_id = Column(String, nullable=True, index=True)
    ad_group_name = Column(String, nullable=True)
    ad_id = Column(String, nullable=True, index=True)
    ad_name = Column(String, nullable=True)
    currency = Column(String(3), nullable=False)
    timezone = Column(String, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    spend = Column(Numeric(28, 10), nullable=True)
    ctr = Column(Numeric(24, 12), nullable=True)
    cpc = Column(Numeric(28, 10), nullable=True)
    cpm = Column(Numeric(28, 10), nullable=True)
    conversions = Column(Numeric(24, 6), nullable=True)
    attribution_window_days = Column(Integer, nullable=False)
    view_through_attribution_window_days = Column(Integer, nullable=False)
    attribution_time_basis = Column(String, nullable=False)
    run_id = Column(String, nullable=True)
    updated_at = Column(DateTime, nullable=False)

