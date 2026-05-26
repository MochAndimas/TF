"""Tests for Tech/IT role access mapping and RBAC normalization."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.utils.rbac import ANALYTICS_ROLES, FINANCE_ANALYTICS_ROLES, require_roles, validate_role_assignment
from streamlit_app.app_shell.config import NAV_GROUPS, ROLE_PAGE_ACCESS
from streamlit_app.app_shell.navigation import allowed_pages_for_role


def test_tech_it_has_access_to_all_non_settings_pages() -> None:
    tech_pages = set(ROLE_PAGE_ACCESS["tech_it"])
    settings_pages = set(NAV_GROUPS["Settings"])
    assert tech_pages
    assert tech_pages.isdisjoint(settings_pages)

    expected_pages = {
        "home",
        "overview",
        "user_acquisition",
        "brand_awareness",
        "remarketing",
        "internal_register",
        "login_activity",
        "deposit_report",
        "remarketing_deposit",
    }
    assert tech_pages == expected_pages
    assert set(allowed_pages_for_role("tech_it")) == expected_pages


def test_tech_it_is_allowed_in_analytics_and_finance_analytics_routes() -> None:
    assert "tech_it" in ANALYTICS_ROLES
    assert "tech_it" in FINANCE_ANALYTICS_ROLES
    require_roles(SimpleNamespace(role="tech_it"), *ANALYTICS_ROLES)
    require_roles(SimpleNamespace(role="tech_it"), *FINANCE_ANALYTICS_ROLES)


def test_superadmin_can_assign_tech_it_role() -> None:
    validate_role_assignment("superadmin", "tech_it")

    with pytest.raises(PermissionError):
        validate_role_assignment("analyst", "tech_it")
