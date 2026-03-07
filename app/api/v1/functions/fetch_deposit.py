"""Fetch Deposit module.

This module is part of `app.api.v1.functions` and contains runtime logic used by the
Traders Family application.
"""

from datetime import date

from app.utils.deposit_utils import DepositData


async def fetch_deposit_daily_overview_payload(
    deposit_data: DepositData,
    start_date: date,
    end_date: date,
    campaign_type: str | None = None,
) -> dict[str, object]:
    """Build normalized payload for Deposit Daily Report API response.

    Args:
        deposit_data (DepositData): Preloaded deposit aggregation service.
        start_date (date): Inclusive report start date requested by client.
        end_date (date): Inclusive report end date requested by client.
        campaign_type (str | None): Optional campaign type filter. ``None`` means
            all campaign types are included.

    Returns:
        dict[str, object]: Response-ready payload containing normalized date
        boundaries and fully aggregated report structure.
    """
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "report": await deposit_data.build_daily_report_payload(campaign_type=campaign_type),
    }
