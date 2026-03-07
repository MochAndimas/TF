"""Deposit module.

This module is part of `app.schemas` and contains runtime logic used by the
Traders Family application.
"""

from datetime import date
from typing import Literal

from pydantic import BaseModel

CampaignType = Literal["all", "user_acquisition", "brand_awareness"]
"""Supported campaign type filter for deposit report endpoint.

Values:
    - ``all``: Include all campaign types.
    - ``user_acquisition``: Include only user acquisition campaigns.
    - ``brand_awareness``: Include only brand awareness campaigns.
"""


class DepositReportRequest(BaseModel):
    """Request schema for deposit daily report API.

    Attributes:
        start_date (date): Inclusive start date of report window.
        end_date (date): Inclusive end date of report window.
        campaign_type (CampaignType): Campaign type selector used to filter
            rows before aggregation.
    """

    start_date: date
    end_date: date
    campaign_type: CampaignType = "all"
