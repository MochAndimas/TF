"""Package initializer for `app.db.models` namespace."""

# Ensure all SQLAlchemy models are imported and registered in metadata
# before schema bootstrap runs.
from app.db.models.etl_run import EtlRun  # noqa: F401
from app.db.models.schema_migration import SchemaMigration  # noqa: F401
from app.db.models.external_api import (  # noqa: F401
    Campaign,
    DataDepo,
    DataDepoBa,
    DataMsDeposit,
    DailyRegister,
    FacebookAds,
    FacebookPageInsights,
    FacebookPageMediaInsights,
    Ga4DailyMetrics,
    GoogleAds,
    InstagramInsights,
    InstagramMediaInsights,
    PlayConsoleInstallMetrics,
    StgAdsRaw,
    TikTokAds,
    TikTokInsights,
    TikTokMediaInsights,
    YouTubeDailyInsight,
    YouTubeMediaInsight,
)
from app.db.models.user import (  # noqa: F401
    AuthAuditEvent,
    AuthRateLimitEvent,
    LogData,
    LoginThrottle,
    TfUser,
    UserToken,
)
