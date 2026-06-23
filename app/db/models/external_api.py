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

    data_ms_deposit = relationship(
        'DataMsDeposit',
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

    daily_register = relationship(
        'DailyRegister',
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


class DataMsDeposit(SqliteBase):
    """Model for MS1 deposit/activity records tied to campaigns."""

    __tablename__ = "data_ms_deposit"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "email",
            "last_activity",
            "campaign_id",
            "tag",
            name="uq_data_ms_deposit_email_activity_campaign_tag",
        ),
        Index("ix_data_ms_deposit_last_activity", "last_activity"),
        Index("ix_data_ms_deposit_campaign_id", "campaign_id"),
        {"schema": None}
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    email = Column("email", String, nullable=False)
    tag = Column("tag", String, nullable=True)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    user_status = Column("user_status", String, nullable=True)
    first_depo = Column("first_depo", Float, nullable=True)
    time_to_closing = Column("time_to_closing", String, nullable=True)
    last_depo = Column("last_depo", Date, nullable=True)
    last_depo_amount = Column("last_depo_amount", Float, nullable=True)
    last_activity = Column("last_activity", Date, nullable=False)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign',
        lazy=True,
        back_populates='data_ms_deposit',
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


class DailyRegister(SqliteBase):
    """Store daily registration totals by campaign from Google Sheets."""

    __tablename__ = "daily_register"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        UniqueConstraint(
            "date",
            "campaign_id",
            name="uq_daily_register_date_campaign",
        ),
        Index("ix_daily_register_date", "date"),
        Index("ix_daily_register_campaign_id", "campaign_id"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    total_regis = Column("total_regis", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign',
        lazy=True,
        back_populates='daily_register',
        viewonly=True
    )


class InstagramInsights(SqliteBase):
    """Store daily Instagram account/content insight metrics."""

    __tablename__ = "instagram_insights"
    __table_args__ = (
        UniqueConstraint(
            "date",
            name="uq_instagram_insights_date",
        ),
        Index("ix_instagram_insights_date", "date"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    total_followers = Column("total_followers", Integer, nullable=False, default=0)
    new_followers = Column("new_followers", Integer, nullable=False, default=0)
    unfollowers = Column("unfollowers", Integer, nullable=False, default=0)
    total_engagement = Column("total_engagement", Integer, nullable=False, default=0)
    likes = Column("likes", Integer, nullable=False, default=0)
    comments = Column("comments", Integer, nullable=False, default=0)
    shares = Column("shares", Integer, nullable=False, default=0)
    saves = Column("saves", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)


class YouTubeDailyInsight(SqliteBase):
    """Store daily YouTube channel analytics metrics."""

    __tablename__ = "youtube_daily_insight"
    __table_args__ = (
        UniqueConstraint(
            "date",
            name="uq_youtube_daily_insight_date",
        ),
        Index("ix_youtube_daily_insight_date", "date"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    views = Column("views", Integer, nullable=False, default=0)
    watch_hours = Column("watch_hours", Float, nullable=False, default=0.0)
    subscribers_gained = Column("subscribers_gained", Integer, nullable=False, default=0)
    subscribers_lost = Column("subscribers_lost", Integer, nullable=False, default=0)
    net_subscribers = Column("net_subscribers", Integer, nullable=False, default=0)
    likes = Column("likes", Integer, nullable=False, default=0)
    comments = Column("comments", Integer, nullable=False, default=0)
    shares = Column("shares", Integer, nullable=False, default=0)
    average_view_duration = Column(
        "average_view_duration",
        Float,
        nullable=False,
        default=0.0,
    )
    pull_date = Column("pull_date", Date, nullable=False)


class YouTubeMediaInsight(SqliteBase):
    """Store YouTube video, Shorts, and live performance snapshots."""

    __tablename__ = "youtube_media_insight"
    __table_args__ = (
        UniqueConstraint(
            "video_id",
            name="uq_youtube_media_insight_video_id",
        ),
        Index("ix_youtube_media_insight_date", "date"),
        Index("ix_youtube_media_insight_content_type", "content_type"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    video_id = Column("video_id", String, nullable=False)
    title = Column("title", String, nullable=False)
    published_at = Column("published_at", DateTime, nullable=False)
    content_type = Column("content_type", String, nullable=False, default="UNKNOWN")
    thumbnail_url = Column("thumbnail_url", String, nullable=True)
    permalink = Column("permalink", String, nullable=False)
    views = Column("views", Integer, nullable=False, default=0)
    watch_hours = Column("watch_hours", Float, nullable=False, default=0.0)
    average_view_percentage = Column(
        "average_view_percentage",
        Float,
        nullable=False,
        default=0.0,
    )
    likes = Column("likes", Integer, nullable=False, default=0)
    comments = Column("comments", Integer, nullable=False, default=0)
    shares = Column("shares", Integer, nullable=False, default=0)
    subscribers_gained = Column("subscribers_gained", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)


class InstagramMediaInsights(SqliteBase):
    """Store Instagram post and reels lifetime media insight snapshots."""

    __tablename__ = "instagram_media_insights"
    __table_args__ = (
        UniqueConstraint(
            "media_id",
            name="uq_instagram_media_insights_media_id",
        ),
        Index("ix_instagram_media_insights_date", "date"),
        Index("ix_instagram_media_insights_media_type", "media_type"),
        Index("ix_instagram_media_insights_media_product_type", "media_product_type"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column("date", Date, nullable=False)
    media_id = Column("media_id", String, nullable=False)
    media_type = Column("media_type", String, nullable=False)
    media_product_type = Column("media_product_type", String, nullable=False)
    timestamp = Column("timestamp", DateTime, nullable=True)
    caption = Column("caption", String, nullable=True)
    permalink = Column("permalink", String, nullable=True)
    media_url = Column("media_url", String, nullable=True)
    thumbnail_url = Column("thumbnail_url", String, nullable=True)
    likes = Column("likes", Integer, nullable=False, default=0)
    comments = Column("comments", Integer, nullable=False, default=0)
    shares = Column("shares", Integer, nullable=False, default=0)
    saves = Column("saves", Integer, nullable=False, default=0)
    reach = Column("reach", Integer, nullable=False, default=0)
    impressions = Column("impressions", Integer, nullable=False, default=0)
    plays = Column("plays", Integer, nullable=False, default=0)
    total_engagement = Column("total_engagement", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)


class FacebookPageInsights(SqliteBase):
    """Store daily Facebook Page insight metrics."""

    __tablename__ = "facebook_page_insights"
    __table_args__ = (
        UniqueConstraint(
            "page_id",
            "date",
            name="uq_facebook_page_insights_page_date",
        ),
        Index("ix_facebook_page_insights_date", "date"),
        Index("ix_facebook_page_insights_page_id", "page_id"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    page_id = Column("page_id", String, nullable=False)
    date = Column("date", Date, nullable=False)
    page_fans = Column("page_fans", Integer, nullable=False, default=0)
    page_fan_adds = Column("page_fan_adds", Integer, nullable=False, default=0)
    page_fan_removes = Column("page_fan_removes", Integer, nullable=False, default=0)
    page_impressions = Column("page_impressions", Integer, nullable=False, default=0)
    page_impressions_unique = Column("page_impressions_unique", Integer, nullable=False, default=0)
    page_impressions_paid = Column("page_impressions_paid", Integer, nullable=False, default=0)
    page_impressions_organic_v2 = Column("page_impressions_organic_v2", Integer, nullable=False, default=0)
    page_post_engagements = Column("page_post_engagements", Integer, nullable=False, default=0)
    reaction_like = Column("reaction_like", Integer, nullable=False, default=0)
    reaction_love = Column("reaction_love", Integer, nullable=False, default=0)
    reaction_wow = Column("reaction_wow", Integer, nullable=False, default=0)
    reaction_haha = Column("reaction_haha", Integer, nullable=False, default=0)
    reaction_sorry = Column("reaction_sorry", Integer, nullable=False, default=0)
    reaction_anger = Column("reaction_anger", Integer, nullable=False, default=0)
    page_video_views = Column("page_video_views", Integer, nullable=False, default=0)
    page_views_total = Column("page_views_total", Integer, nullable=False, default=0)
    pull_date = Column("pull_date", Date, nullable=False)


class FacebookPageMediaInsights(SqliteBase):
    """Store Facebook Page post/media lifetime insight snapshots."""

    __tablename__ = "facebook_page_media_insights"
    __table_args__ = (
        UniqueConstraint(
            "post_id",
            name="uq_facebook_page_media_insights_post_id",
        ),
        Index("ix_facebook_page_media_insights_date", "date"),
        Index("ix_facebook_page_media_insights_page_id", "page_id"),
        Index("ix_facebook_page_media_insights_post_type", "post_type"),
        {"schema": None},
    )

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    page_id = Column("page_id", String, nullable=False)
    date = Column("date", Date, nullable=False)
    post_id = Column("post_id", String, nullable=False)
    post_type = Column("post_type", String, nullable=False)
    status_type = Column("status_type", String, nullable=True)
    created_time = Column("created_time", DateTime, nullable=True)
    message = Column("message", String, nullable=True)
    permalink_url = Column("permalink_url", String, nullable=True)
    full_picture = Column("full_picture", String, nullable=True)
    likes = Column("likes", Integer, nullable=False, default=0)
    comments = Column("comments", Integer, nullable=False, default=0)
    shares = Column("shares", Integer, nullable=False, default=0)
    reaction_like = Column("reaction_like", Integer, nullable=False, default=0)
    reaction_love = Column("reaction_love", Integer, nullable=False, default=0)
    reaction_wow = Column("reaction_wow", Integer, nullable=False, default=0)
    reaction_haha = Column("reaction_haha", Integer, nullable=False, default=0)
    reaction_sorry = Column("reaction_sorry", Integer, nullable=False, default=0)
    reaction_anger = Column("reaction_anger", Integer, nullable=False, default=0)
    post_media_view = Column("post_media_view", Integer, nullable=False, default=0)
    post_clicks = Column("post_clicks", Integer, nullable=False, default=0)
    post_video_views = Column("post_video_views", Integer, nullable=False, default=0)
    total_engagement = Column("total_engagement", Integer, nullable=False, default=0)
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


class ManagedSecret(SqliteBase):
    """Encrypted application secret persisted in backend storage."""

    __tablename__ = "managed_secret"

    secret_key = Column("secret_key", String, primary_key=True)
    secret_value = Column("secret_value", String, nullable=False)
    description = Column("description", String, nullable=True)
    created_at = Column("created_at", DateTime, nullable=False)
    updated_at = Column("updated_at", DateTime, nullable=False)
