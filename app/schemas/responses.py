"""Reusable versioned API response wrappers."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

API_RESPONSE_VERSION = "v1"


class ApiResponseV1(BaseModel):
    """Common response metadata returned by versioned API handlers."""

    api_version: Literal["v1"] = API_RESPONSE_VERSION
    success: bool
    message: str


class AnalyticsResponse(ApiResponseV1):
    """Generic wrapper for dashboard analytics payloads.

    Analytics payloads intentionally remain dictionary-shaped because the
    frontend consumes nested chart/table contracts from Plotly and Pandas.
    The wrapper keeps the transport contract stable and versioned.
    """

    data: dict[str, Any] = Field(default_factory=dict)
