"""Composable campaign analytics service."""

from app.utils.campaign.base import CampaignDataBase
from app.utils.campaign.brand_awareness import BrandAwarenessCampaignMixin
from app.utils.campaign.user_acquisition import UserAcquisitionCampaignMixin


class CampaignData(UserAcquisitionCampaignMixin, BrandAwarenessCampaignMixin, CampaignDataBase):
    """Concrete campaign analytics service composed from focused concerns."""
