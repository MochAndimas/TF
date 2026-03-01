from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, Integer
from sqlalchemy import String, DateTime, JSON, Boolean, Float, Date
from sqlalchemy.orm import relationship
from app.db.base import SqliteBase


class Campaign(SqliteBase):
    """
    Docstring for caampaign
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
    """
    """
    __tablename__ = "data_depo"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
        {"schema": None}
    )
    __mapper_args__ = {
        "polymorphic_identity": "data_depo"
    }

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    campaign_id = Column("campaign_id", String, nullable=False, default=ForeignKey("campaign.campaign_id"))
    campaign_name = Column("campaign_name", String, nullable=False)
    status = Column("status", String, nullable=False)
    email = Column("email", String, nullable=False)
    first_depo = Column("first_depo", Float, nullable=False)
    bulan = Column("bulan", Date, nullable=False)
    pull_date = Column("pull_date", Date, nullable=False)

    campaign = relationship(
        'Campaign', 
        lazy=True, 
        back_populates='data_depo', 
        viewonly=True
    )


class GoogleAds(SqliteBase):
    """
    """
    __tablename__ = "google_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
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
    cost = Column("cost", Integer, nullable=True)
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
    """
    """
    __tablename__ = "facebook_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
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
    cost = Column("cost", Integer, nullable=True)
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
    """
    """
    __tablename__ = "tiktok_ads"
    __table_args__ = (
        ForeignKeyConstraint(["campaign_id"], ["campaign.campaign_id"]),
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
    cost = Column("cost", Integer, nullable=True)
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
