"""Shared typing aliases for campaign analytics modules."""

from app.db.models.external_api import FacebookAds, GoogleAds, TikTokAds

AdsModel = GoogleAds | FacebookAds | TikTokAds
