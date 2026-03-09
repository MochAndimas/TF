"""Package initializer for `app.db.models` namespace."""

# Ensure all SQLAlchemy models are imported and registered in metadata
# before schema bootstrap runs.
from app.db.models.etl_run import EtlRun  # noqa: F401
from app.db.models.external_api import (  # noqa: F401
    Campaign,
    DataDepo,
    FacebookAds,
    Ga4DailyMetrics,
    GoogleAds,
    StgAdsRaw,
    StgDepoRaw,
    TikTokAds,
)
from app.db.models.user import LogData, TfUser, UserToken  # noqa: F401

