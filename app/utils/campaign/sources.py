"""Source labels shared by the UA payload builder and its page selector."""
USER_ACQUISITION_SOURCE_OPTIONS = {
    "Google Ads": "google",
    "Facebook Ads": "facebook",
    "TikTok Ads": "tiktok",
    "GPT Ads": "gpt_ads",
}
USER_ACQUISITION_SOURCES = tuple(USER_ACQUISITION_SOURCE_OPTIONS.values())
GPT_REPORT_LEVEL_BY_DIMENSION = {"campaign_id": "campaign", "ad_group": "ad_group", "ad_name": "ad"}
