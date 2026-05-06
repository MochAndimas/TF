"""Shared app-shell configuration for Streamlit navigation."""

from __future__ import annotations

from typing import Awaitable, Callable

from streamlit_app.page import (
    brand_awareness,
    deposit,
    google_ads_token,
    home,
    internal_register,
    login,
    meta_ads_token,
    overview,
    register,
    update_data,
    user_acquisition,
)

PageHandler = Callable[[str], Awaitable[None]]

PAGE_LABELS: dict[str, str] = {
    "home": "Home",
    "overview": "Overview",
    "user_acquisition": "User Acquisition",
    "brand_awareness": "Brand Awareness",
    "internal_register": "Internal Register",
    "deposit_report": "Deposit",
    "register": "Create Account",
    "update_data": "Update Data",
    "google_ads_token": "Google Ads Token",
    "meta_ads_token": "Meta Ads Token",
}

PAGE_BUTTON_TYPES: dict[str, str] = {
    "home": "secondary",
    "overview": "tertiary",
    "user_acquisition": "tertiary",
    "brand_awareness": "tertiary",
    "internal_register": "tertiary",
    "deposit_report": "tertiary",
    "register": "tertiary",
    "update_data": "tertiary",
    "google_ads_token": "tertiary",
    "meta_ads_token": "tertiary",
}

NAV_GROUPS: dict[str, list[str]] = {
    "Portal": ["home"],
    "Overall": ["overview"],
    "Revenue": ["deposit_report"],
    "Campaign": ["user_acquisition", "brand_awareness", "internal_register"],
    "Settings": ["register", "update_data", "google_ads_token", "meta_ads_token"],
}

ROLE_PAGE_ACCESS: dict[str, list[str]] = {
    "superadmin": ["home", "overview", "user_acquisition", "brand_awareness", "internal_register", "deposit_report", "register", "update_data", "google_ads_token", "meta_ads_token"],
    "admin": ["home", "overview", "user_acquisition", "brand_awareness", "internal_register", "deposit_report"],
    "digital_marketing": ["home", "overview", "user_acquisition", "brand_awareness", "internal_register"],
}

PAGE_HANDLERS: dict[str, PageHandler] = {
    "home": home.show_home_page,
    "overview": overview.show_overview_page,
    "user_acquisition": user_acquisition.show_user_acquisition_page,
    "brand_awareness": brand_awareness.show_brand_awareness_page,
    "internal_register": internal_register.show_internal_register_page,
    "deposit_report": deposit.show_deposit_page,
    "register": register.create_account,
    "update_data": update_data.show_update_page,
    "google_ads_token": google_ads_token.show_google_ads_token_page,
    "meta_ads_token": meta_ads_token.show_meta_ads_token_page,
}

PUBLIC_PAGE_KEYS = {"google_ads_token", "meta_ads_token"}
