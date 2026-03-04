from pydantic import BaseModel
from typing import Dict
from datetime import datetime


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
