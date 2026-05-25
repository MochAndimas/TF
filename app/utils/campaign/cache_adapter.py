"""Cache adapter for campaign dataframe caching concerns."""

from __future__ import annotations

from datetime import date

import pandas as pd

from app.utils.analytics_cache import (
    CAMPAIGN_CACHE_PREFIX,
    analytics_dataframe_cache_enabled,
    campaign_dataframe_cache,
)
from app.utils.dataframe_cache import CacheKey


class CampaignCacheAdapter:
    """Thin cache abstraction so service code does not depend on cache backend directly."""

    @staticmethod
    def make_key(*parts: object) -> CacheKey:
        normalized = tuple(part.isoformat() if isinstance(part, date) else part for part in parts)
        return CAMPAIGN_CACHE_PREFIX + normalized

    @staticmethod
    def get(cache_key: CacheKey) -> pd.DataFrame | None:
        if not analytics_dataframe_cache_enabled():
            return None
        return campaign_dataframe_cache.get(cache_key)

    @staticmethod
    def set(cache_key: CacheKey, df: pd.DataFrame) -> pd.DataFrame:
        if not analytics_dataframe_cache_enabled():
            return df
        return campaign_dataframe_cache.set(cache_key, df)
