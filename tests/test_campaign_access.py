"""Exercise Campaign HTTP authorization without accessing application data."""

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.v1.endpoint import campaign, deposit
from app.db.models.user import TfUser
from app.db.session import get_db
from app.utils.auth_dependencies import get_current_user


CAMPAIGN_PATHS = (
    "/api/campaign/user-acquisition",
    "/api/campaign/brand-awareness",
    "/api/campaign/remarketing",
)
PARAMS = {"start_date": "2026-09-01", "end_date": "2026-09-07"}


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(campaign.router)
    app.include_router(deposit.router)

    async def no_database():
        yield None

    app.dependency_overrides[get_db] = no_database
    for name in (
        "_load_user_acquisition_payload",
        "_load_brand_awareness_payload",
        "_load_remarketing_payload",
    ):
        monkeypatch.setattr(campaign, name, AsyncMock(return_value={"report": "ok"}))
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize("path", CAMPAIGN_PATHS)
@pytest.mark.parametrize(
    ("role", "expected_status"),
    (
        ("digital_marketing", 200),
        ("finance", 200),
        ("superadmin", 200),
        ("analyst", 200),
        ("admin", 200),
        ("tech_it", 200),
        ("social_media", 403),
        ("sales", 403),
    ),
)
def test_campaign_role_access(client, path, role, expected_status):
    client.app.dependency_overrides[get_current_user] = lambda: TfUser(role=role)
    response = client.get(path, params=PARAMS)
    assert response.status_code == expected_status
    if expected_status == 200:
        assert response.json()["data"] == {"report": "ok"}


@pytest.mark.parametrize("path", CAMPAIGN_PATHS)
def test_campaign_still_requires_authentication(client, path):
    assert client.get(path, params=PARAMS).status_code == 401


@pytest.mark.parametrize(
    "path", ("/api/deposit/daily-report", "/api/deposit/remarketing-report")
)
def test_digital_marketing_cannot_access_revenue(client, path):
    client.app.dependency_overrides[get_current_user] = lambda: TfUser(role="digital_marketing")
    assert client.get(path, params=PARAMS).status_code == 403
