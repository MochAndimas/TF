"""Stable GA4 acquisition report contract (API name -> storage name)."""
SOURCE = "ga4_user_acquisition"
ALLOWED_SOURCES = ("ois_id", "ois.id")
DIMENSIONS = {
    "date": "date",
    "platform": "platform",
    "firstUserSource": "first_user_source",
    "firstUserMedium": "first_user_medium",
    "firstUserCampaignName": "first_user_campaign_name",
}
METRICS = {
    "newUsers": "new_users",
    "activeUsers": "active_users",
    "totalUsers": "total_users",
    "engagedSessions": "engaged_sessions",
    "userEngagementDuration": "user_engagement_duration",
    "eventCount": "event_count",
    "keyEvents": "key_events",
    "userKeyEventRate": "user_key_event_rate",
}
KEY_COLUMNS = tuple(DIMENSIONS.values())
INTEGER_METRICS = ("new_users", "active_users", "total_users", "engaged_sessions", "event_count")
