"""Shared app-shell configuration for Streamlit navigation."""

from __future__ import annotations

from typing import Awaitable, Callable

from streamlit_app.page import (
    brand_awareness,
    deposit,
    facebook,
    google_ads_token,
    home,
    internal_register,
    instagram,
    instagram_token,
    install,
    legal,
    login_activity,
    login,
    meta_ads_token,
    overview,
    register,
    remarketing,
    remarketing_deposit,
    database_maintenance,
    tiktok,
    tiktok_token,
    update_data,
    user_acquisition,
    youtube,
    youtube_token,
)

PageHandler = Callable[[str], Awaitable[None]]

PAGE_LABELS: dict[str, str] = {
    "home": "Home",
    "overview": "Overview",
    "install": "Install",
    "user_acquisition": "User Acquisition",
    "brand_awareness": "Brand Awareness",
    "remarketing": "Remarketing",
    "facebook": "Facebook",
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "youtube": "YouTube",
    "internal_register": "Register",
    "login_activity": "Login",
    "deposit_report": "First Deposit",
    "remarketing_deposit": "Remarketing Deposit",
    "register": "Create Account",
    "update_data": "Update Data",
    "database_maintenance": "Database Maintenance",
    "google_ads_token": "Google Ads Token",
    "instagram_token": "Instagram Token",
    "meta_ads_token": "Meta Ads Token",
    "tiktok_token": "TikTok Token",
    "youtube_token": "YouTube Token",
}

PAGE_SLUGS: dict[str, str] = {
    "home": "home",
    "overview": "overview",
    "install": "install",
    "user_acquisition": "user-acquisition",
    "brand_awareness": "brand-awareness",
    "remarketing": "remarketing",
    "facebook": "facebook",
    "instagram": "instagram",
    "tiktok": "tiktok",
    "youtube": "youtube",
    "internal_register": "internal-register",
    "login_activity": "login-activity",
    "deposit_report": "first-deposit",
    "remarketing_deposit": "remarketing-deposit",
    "register": "create-account",
    "update_data": "update-data",
    "database_maintenance": "database-maintenance",
    "google_ads_token": "google-ads-token",
    "instagram_token": "instagram-token",
    "meta_ads_token": "meta-ads-token",
    "tiktok_token": "tiktok-token",
    "youtube_token": "youtube-token",
}

PAGE_KEYS_BY_SLUG: dict[str, str] = {slug: page_key for page_key, slug in PAGE_SLUGS.items()}

PAGE_BUTTON_TYPES: dict[str, str] = {
    "home": "secondary",
    "overview": "tertiary",
    "install": "tertiary",
    "user_acquisition": "tertiary",
    "brand_awareness": "tertiary",
    "remarketing": "tertiary",
    "facebook": "tertiary",
    "instagram": "tertiary",
    "tiktok": "tertiary",
    "youtube": "tertiary",
    "internal_register": "tertiary",
    "login_activity": "tertiary",
    "deposit_report": "tertiary",
    "remarketing_deposit": "tertiary",
    "register": "tertiary",
    "update_data": "tertiary",
    "database_maintenance": "tertiary",
    "google_ads_token": "tertiary",
    "instagram_token": "tertiary",
    "meta_ads_token": "tertiary",
    "tiktok_token": "tertiary",
    "youtube_token": "tertiary",
}

NAV_GROUPS: dict[str, list[str]] = {
    "Portal": ["home"],
    "Overall": ["overview"],
    "Revenue": ["deposit_report", "remarketing_deposit"],
    "Campaign": ["user_acquisition", "brand_awareness", "remarketing"],
    "Socmed": ["instagram", "facebook", "tiktok", "youtube"],
    "Activity": ["internal_register", "login_activity", "install"],
    "Settings": ["register", "update_data", "database_maintenance", "google_ads_token", "meta_ads_token", "instagram_token", "youtube_token", "tiktok_token"],
}

ROLE_PAGE_ACCESS: dict[str, list[str]] = {
    "superadmin": ["home", "overview", "install", "user_acquisition", "brand_awareness", "remarketing", "instagram", "facebook", "tiktok", "youtube", "internal_register", "login_activity", "deposit_report", "remarketing_deposit", "register", "update_data", "database_maintenance", "google_ads_token", "meta_ads_token", "instagram_token", "youtube_token", "tiktok_token", "terms", "privacy"],
    "analyst": ["home", "overview", "install", "user_acquisition", "brand_awareness", "remarketing", "instagram", "facebook", "tiktok", "youtube", "internal_register", "login_activity", "deposit_report", "remarketing_deposit"],
    "admin": ["home", "overview", "user_acquisition", "brand_awareness", "remarketing", "instagram", "facebook", "tiktok", "youtube", "internal_register", "login_activity", "deposit_report", "remarketing_deposit"],
    "digital_marketing": ["home", "overview", "install", "user_acquisition", "brand_awareness", "remarketing", "instagram", "facebook", "tiktok", "youtube", "internal_register", "login_activity"],
    "finance": ["home", "overview", "user_acquisition", "brand_awareness", "remarketing", "deposit_report", "remarketing_deposit"],
    "social_media": ["home", "instagram", "facebook", "tiktok", "youtube"],
    "tech_it": ["home", "overview", "install", "user_acquisition", "brand_awareness", "remarketing", "instagram", "facebook", "tiktok", "youtube", "internal_register", "login_activity", "deposit_report", "remarketing_deposit"],
    "sales": [],
}

PAGE_HANDLERS: dict[str, PageHandler] = {
    "home": home.show_home_page,
    "overview": overview.show_overview_page,
    "install": install.show_install_page,
    "user_acquisition": user_acquisition.show_user_acquisition_page,
    "brand_awareness": brand_awareness.show_brand_awareness_page,
    "remarketing": remarketing.show_remarketing_page,
    "facebook": facebook.show_facebook_page,
    "instagram": instagram.show_instagram_page,
    "tiktok": tiktok.show_tiktok_page,
    "youtube": youtube.show_youtube_page,
    "internal_register": internal_register.show_internal_register_page,
    "login_activity": login_activity.show_login_activity_page,
    "deposit_report": deposit.show_deposit_page,
    "remarketing_deposit": remarketing_deposit.show_remarketing_deposit_page,
    "register": register.create_account,
    "update_data": update_data.show_update_page,
    "database_maintenance": database_maintenance.show_database_maintenance_page,
    "google_ads_token": google_ads_token.show_google_ads_token_page,
    "instagram_token": instagram_token.show_instagram_token_page,
    "meta_ads_token": meta_ads_token.show_meta_ads_token_page,
    "tiktok_token": tiktok_token.show_tiktok_token_page,
    "youtube_token": youtube_token.show_youtube_token_page,
    "terms": legal.show_terms_page,
    "privacy": legal.show_privacy_page,
}

PUBLIC_PAGE_KEYS = {
    "google_ads_token",
    "meta_ads_token",
    "instagram_token",
    "tiktok_token",
    "youtube_token",
    "terms",
    "privacy",
}
