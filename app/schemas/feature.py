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
        data (str): Source identifier to update (e.g., `google_ads`, `ga4_daily_metrics`).
        types (str): Update mode, usually `auto` or `manual`.
    """
    start_date: datetime
    end_date: datetime
    data: str
    types: str = "auto"


class UpdateDataResponse(BaseModel):
    """Response schema returned when an ETL update request is accepted.

    Attributes:
        message (str): Human-readable acceptance or status message.
        run_id (str): Unique ETL run identifier used for polling progress.
        status (str): Initial run status returned to the frontend.
        recovered_stale_runs (int): Number of stale runs auto-failed before the
            new job was accepted.
    """

    message: str
    run_id: str
    status: str
    recovered_stale_runs: int = 0


class UpdateDataStatusResponse(BaseModel):
    """Response schema for asynchronous ETL run status polling.

    Attributes:
        run_id (str): Unique ETL run identifier.
        pipeline (str): Logical pipeline family, for example external API sync.
        source (str): Source selected by the user for this run.
        mode (str): Trigger mode such as ``auto`` or ``manual``.
        status (str): Current lifecycle state (`queued`, `running`, `success`, `failed`).
        message (str | None): Success/status message persisted by the ETL job.
        error_detail (str | None): Failure detail stored when the job fails.
        window_start (date | None): Inclusive ETL window start.
        window_end (date | None): Inclusive ETL window end.
        started_at (datetime): Timestamp when the ETL run started.
        ended_at (datetime | None): Timestamp when the run finished, if any.
    """

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
