"""Package initializer for `app.db.models` namespace."""

# Ensure all SQLAlchemy models are imported and registered in metadata
# before schema bootstrap runs.
from app.db.models.etl_run import EtlRun  # noqa: F401
from app.db.models.external_api import (  # noqa: F401
    Campaign,
    DataDepo,
    DataMsDeposit,
    DailyRegister,
    FacebookAds,
    Ga4DailyMetrics,
    GoogleAds,
    InstagramInsights,
    StgAdsRaw,
    TikTokAds,
)
from app.db.models.user import (  # noqa: F401
    AuthAuditEvent,
    AuthRateLimitEvent,
    LogData,
    LoginThrottle,
    TfUser,
    UserToken,
)
