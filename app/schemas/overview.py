"""Pydantic schema contracts for overview analytics APIs.

This file currently defines request-level contracts used by the active-user
overview endpoint and shared source typing for overview modules.
"""

from datetime import date
from typing import Literal

from pydantic import BaseModel


OverviewSource = Literal["app", "web"]
"""Supported GA4 source values for active-user overview analytics."""


class OverviewActiveUsersRequest(BaseModel):
    """Request schema for overview active-user endpoint.

    Attributes:
        start_date (date): Inclusive start date of reporting window.
        end_date (date): Inclusive end date of reporting window.
        source (OverviewSource): GA4 logical source selector (`app`/`web`).
    """

    start_date: date
    end_date: date
    source: OverviewSource = "app"
