"""Feature module.

This module is part of `app.schemas` and contains runtime logic used by the
Traders Family application.
"""

from pydantic import BaseModel
from datetime import date, datetime


class UpdateData(BaseModel):
    """Schema for requesting external data synchronization.

    Attributes:
        start_date (datetime): Inclusive start date for manual sync mode.
        end_date (datetime): Inclusive end date for manual sync mode.
        data (str): Source identifier to update (e.g., `google_ads`, `data_depo`).
        types (str): Update mode, usually `auto` or `manual`.
    """
    start_date: datetime
    end_date: datetime
    data: str
    types: str = "auto"


class UpdateDataResponse(BaseModel):
    """Schema for update endpoint response payload."""

    message: str
    run_id: str
    status: str


class UpdateDataStatusResponse(BaseModel):
    """Schema for checking asynchronous ETL run status."""

    run_id: str
    pipeline: str
    source: str
    mode: str
    status: str
    message: str | None
    error_detail: str | None
    window_start: date | None
    window_end: date | None
    started_at: datetime
    ended_at: datetime | None
