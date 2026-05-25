"""Shared analytics cache instances and invalidation helpers."""

from __future__ import annotations

import logging

from app.core.config import settings
from app.utils.dataframe_cache import CacheKey, DataFrameCache

logger = logging.getLogger(__name__)

CAMPAIGN_CACHE_PREFIX: CacheKey = ("campaign",)

campaign_dataframe_cache = DataFrameCache(
    max_entries=settings.ANALYTICS_DATAFRAME_CACHE_MAX_ENTRIES,
    ttl_seconds=settings.ANALYTICS_DATAFRAME_CACHE_TTL_SECONDS,
)


def analytics_dataframe_cache_enabled() -> bool:
    """Return whether analytics DataFrame caching is enabled at runtime."""
    return settings.ANALYTICS_DATAFRAME_CACHE_ENABLED


def clear_campaign_analytics_cache() -> int:
    """Clear cached campaign analytics DataFrames for the current process."""
    removed = campaign_dataframe_cache.clear(prefix=CAMPAIGN_CACHE_PREFIX)
    logger.info("Cleared %s campaign analytics DataFrame cache entries", removed)
    return removed
