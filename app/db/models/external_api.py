"""External Api module.

This module is part of `app.db.models` and contains runtime logic used by the
Traders Family application.
"""

from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Index, Integer, UniqueConstraint
from sqlalchemy import String, DateTime, JSON, Boolean, Float, Date
from sqlalchemy.orm import relationship
from app.db.base import SqliteBase


class Campaign(SqliteBase):
    """Master campaign mapping model used across ad sources.

    Attributes:
        campaign_id (str): Primary key campaign identifier.
        campaign_name (str): Human-readable campaign name.
        ad_source (str): Advertising platform source label.
        ad_type (str): Campaign objective type.
        created_at (DateTime): Creation timestamp.
        deleted_at (DateTime | None): Soft-delete timestamp.
    """
    __tablename__ = "campaign"
    
    campaign_id = Column("campaign_id", String, primary_key=True)
    campaign_name = Column("campaign_name", String, nullable=False)
    ad_source = Column("ad_source", String, nullable=False)
    ad_type = Column("ad_type", String, nullable=False)
    created_at = Column("created_at", DateTime, nullable=False)
    deleted_at = Column("deleted_at", DateTime, nullable=True)

    data_depo = relationship(
        'DataDepo', 
        lazy=True, 
        back_populates='campaign', 
        viewonly=True
    )

    google_ads = relationship(
        'GoogleAds', 
        lazy=True, 
        back_populates='campaign', 
        viewonly=True
    )

    facebook_ads = relationship(
        'FacebookAds', 
        lazy=True, 
        back_populates='campaign', 
        viewonly=True
    )

    tiktok_ads = relationship(
        'TikTokAds', 
        lazy=True, 
        back_populates='campaign', 
        viewonly=True
    )

class DataDepo(SqliteBase):
    """Model for imported deposit/user records tied to campaigns.

    Attributes:
        id (int): Primary key.
        user_id (int): External user identifier.
        tanggal_regis (Date): Registration date.
        campaign_id (str): Foreign key to campaign table.
        first_depo (float | None): First deposit amount.
        pull_date (Date): Data import date.
    """
    __tablename__ = "data_depo"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "user_id",
            "tanggal_regis",
            "campaign_id",
            name="uq_data_depo_user_regis_campaign",
        ),
        Index("ix_data_depo_tanggal_regis", "tanggal_regis"),
        Index("ix_data_depo_campaign_id", "campaign_id"),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "data_depo"
    }

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    user_id = Column("user_id", Integer, nullable=False)
    tanggal_regis = Column("tanggal_regis", Date, nullable=False)
    fullname = Column("fullname", String, nullable=True)
    email = Column("email", String, nullable=True)
    phone = Column("phone", String, nullable=True)
    user_status = Column("user_status", String, nullable=True)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    tag = Column("tag", String, nullable=True)
    protection = Column("protection", Integer, nullable=True)
    assign_date = Column("assign_date", Date, nullable=True)
    analyst = Column("analyst", Integer, nullable=True)
    first_depo_date = Column("first_depo_date", Date, nullable=True)
    first_depo = Column("first_depo", Float, nullable=True)
    time_to_closing = Column("time_to_closing", String, nullable=True)
    nmi = Column("nmi", Float, nullable=True)
    lot = Column("lot", Float, nullable=True)
    cabang = Column("cabang", String, nullable=True)
    pool = Column("pool", Boolean, nullable=True)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign', 
        lazy=True, 
        back_populates='data_depo', 
        viewonly=True
    )


class GoogleAds(SqliteBase):
    """Model for daily Google Ads campaign metrics.

    Attributes:
        id (int): Primary key.
        date (Date): Metric date.
        campaign_id (str): Foreign key to campaign table.
        cost (float | None): Advertising cost.
        impressions (int | None): Impression count.
        clicks (int | None): Click count.
        leads (int | None): Lead count.
        pull_date (Date): Data import date.
    """
    __tablename__ = "google_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "date",
            "campaign_id",
            "ad_group",
            "ad_name",
            name="uq_google_ads_date_campaign_adgroup_adname",
        ),
        Index("ix_google_ads_date", "date"),
        Index("ix_google_ads_campaign_id", "campaign_id"),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "google_ads"
    }

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    campaign_name = Column("campaign_name", String, nullable=False)
    ad_group = Column("ad_group", String, nullable=False)
    ad_name = Column("ad_name", String, nullable=False)
    cost = Column("cost", Float, nullable=True)
    impressions = Column("impressions", Integer, nullable=True)
    clicks = Column("clicks", Integer, nullable=True)
    leads = Column("leads", Integer, nullable=True)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign', 
        lazy=True, 
        back_populates='google_ads', 
        viewonly=True
    )


class FacebookAds(SqliteBase):
    """Model for daily Meta/Facebook Ads campaign metrics.

    Attributes:
        id (int): Primary key.
        date (Date): Metric date.
        campaign_id (str): Foreign key to campaign table.
        cost (float | None): Advertising cost.
        impressions (int | None): Impression count.
        clicks (int | None): Click count.
        leads (int | None): Lead count.
        pull_date (Date): Data import date.
    """
    __tablename__ = "facebook_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "date",
            "campaign_id",
            "ad_group",
            "ad_name",
            name="uq_facebook_ads_date_campaign_adgroup_adname",
        ),
        Index("ix_facebook_ads_date", "date"),
        Index("ix_facebook_ads_campaign_id", "campaign_id"),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "facebook_ads"
    }

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    campaign_name = Column("campaign_name", String, nullable=False)
    ad_group = Column("ad_group", String, nullable=False)
    ad_name = Column("ad_name", String, nullable=False)
    cost = Column("cost", Float, nullable=True)
    impressions = Column("impressions", Integer, nullable=True)
    clicks = Column("clicks", Integer, nullable=True)
    leads = Column("leads", Integer, nullable=True)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign', 
        lazy=True, 
        back_populates='facebook_ads', 
        viewonly=True
    )


class TikTokAds(SqliteBase):
    """Model for daily TikTok Ads campaign metrics.

    Attributes:
        id (int): Primary key.
        date (Date): Metric date.
        campaign_id (str): Foreign key to campaign table.
        cost (float | None): Advertising cost.
        impressions (int | None): Impression count.
        clicks (int | None): Click count.
        leads (int | None): Lead count.
        pull_date (Date): Data import date.
    """
    __tablename__ = "tiktok_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "date",
            "campaign_id",
            "ad_group",
            "ad_name",
            name="uq_tiktok_ads_date_campaign_adgroup_adname",
        ),
        Index("ix_tiktok_ads_date", "date"),
        Index("ix_tiktok_ads_campaign_id", "campaign_id"),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "tiktok_ads"
    }

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    campaign_name = Column("campaign_name", String, nullable=False)
    ad_group = Column("ad_group", String, nullable=False)
    ad_name = Column("ad_name", String, nullable=False)
    cost = Column("cost", Float, nullable=True)
    impressions = Column("impressions", Integer, nullable=True)
    clicks = Column("clicks", Integer, nullable=True)
    leads = Column("leads", Integer, nullable=True)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign', 
        lazy=True, 
        back_populates='tiktok_ads', 
        viewonly=True
    )


class Ga4DailyMetrics(SqliteBase):
    """Store GA4 daily user metrics split by logical source (`app`/`web`).

    Business key is ``date + source`` so ETL loads are idempotent and can be
    safely re-run for the same window.
    """

    __tablename__ = "ga4_daily_metrics"
    __table_args__ = (
        UniqueConstraint(
            "date",
            "source",
            name="uq_ga4_daily_metrics_date_source",
        ),
        Index("ix_ga4_daily_metrics_date", "date"),
        Index("ix_ga4_daily_metrics_source", "source"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    source = Column("source", String, nullable=False)
    daily_active_users = Column("daily_active_users", Integer, nullable=False, default=0)
    monthly_active_users = Column("monthly_active_users", Integer, nullable=False, default=0)
    active_users = Column("active_users", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)


class StgAdsRaw(SqliteBase):
    """Raw staging table for ads and GA4 source payload.

    Stores immutable extracted records for audit/replay and ETL traceability.
    """

    __tablename__ = "stg_ads_raw"
    __table_args__ = (
        Index("ix_stg_ads_raw_run_id", "run_id"),
        Index("ix_stg_ads_raw_ingested_at", "ingested_at"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    run_id = Column("run_id", String, nullable=True)
    source = Column("source", String, nullable=False)
    range_name = Column("range_name", String, nullable=True)
    payload = Column("payload", JSON, nullable=False)
    payload_hash = Column("payload_hash", String, nullable=False)
    ingested_at = Column("ingested_at", DateTime, nullable=False)
