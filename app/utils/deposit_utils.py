"""Deposit analytics helpers for the dashboard reporting layer.

This module contains the service object that reads deposit rows, normalizes
status buckets, compares current and previous periods, and produces the
frontend-friendly payloads used by deposit report screens.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataDepo


class DepositData:
    """Service object for loading and aggregating deposit report data.

    This class centralizes:
        - reading raw deposit rows from database,
        - status normalization (new/existing),
        - daily cross-tab aggregation by registration date,
        - summary totals and growth-vs-previous-period computation.

    Attributes:
        session (AsyncSession): Active async DB session.
        from_date (date): Inclusive start date for current report window.
        to_date (date): Inclusive end date for current report window.
        df_depo (pd.DataFrame): Cached deposit rows in current window.
    """

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        """Initialize deposit data container.

        Args:
            session (AsyncSession): Database session used for all queries.
            from_date (date): Inclusive start date for default load window.
            to_date (date): Inclusive end date for default load window.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_depo = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date) -> "DepositData":
        """Create service instance and preload source rows.

        Args:
            session (AsyncSession): Database session used for query execution.
            from_date (date): Inclusive start date of preload window.
            to_date (date): Inclusive end date of preload window.

        Returns:
            DepositData: Ready-to-use service with cached deposit dataframe.
        """
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Populate in-memory dataframe for the active report window.

        Returns:
            None: Populates ``self.df_depo`` as side effect.
        """
        self.df_depo = await self._read_depo_db_with_range(self.from_date, self.to_date)

    async def _read_depo_db_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Read deposit rows in arbitrary date range from database.

        Args:
            from_date (date): Inclusive range start.
            to_date (date): Inclusive range end.

        Returns:
            pd.DataFrame: Normalized dataframe with campaign metadata columns.
            Returns empty dataframe with expected columns when no rows exist.
        """
        query = (
            select(
                DataDepo.tanggal_regis.label("tanggal_regis"),
                DataDepo.campaign_id.label("campaign_id"),
                Campaign.campaign_name.label("campaign_name"),
                Campaign.ad_type.label("campaign_type"),
                DataDepo.user_status.label("user_status"),
                DataDepo.email.label("email"),
                DataDepo.first_depo.label("first_depo"),
            )
            .join(DataDepo.campaign)
            .filter(DataDepo.tanggal_regis.between(from_date, to_date))
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "tanggal_regis",
                    "campaign_id",
                    "campaign_name",
                    "campaign_type",
                    "user_status",
                    "email",
                    "first_depo",
                ]
            )

        df = pd.DataFrame(rows)
        df["tanggal_regis"] = pd.to_datetime(df["tanggal_regis"]).dt.date
        return df

    @staticmethod
    def _normalize_status(value: object) -> str | None:
        """Normalize raw user status values into supported bucket labels.

        Args:
            value (object): Raw status value from source rows.

        Returns:
            str | None: ``"new"`` / ``"existing"`` when recognized, otherwise
            ``None`` so caller can safely exclude unsupported values.
        """
        status = str(value or "").strip().lower()
        if status == "new":
            return "new"
        if status == "existing":
            return "existing"
        return None

    @staticmethod
    def _empty_metric_map(dates: list[date]) -> dict[str, dict[str, float]]:
        """Build zero-initialized nested metric map for all report dates.

        Args:
            dates (list[date]): Ordered date list used as cross-tab timeline.

        Returns:
            dict[str, dict[str, float]]: Metric map keyed by metric -> date ->
            status bucket (`new`/`existing`) with default zero values.
        """
        return {
            metric: {day.isoformat(): {"new": 0.0, "existing": 0.0} for day in dates}
            for metric in ("depo_amount", "qty", "aov")
        }

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Compute previous period with the same duration.

        Args:
            from_date (date): Current period inclusive start date.
            to_date (date): Current period inclusive end date.

        Returns:
            tuple[date, date]: ``(previous_from, previous_to)`` with identical
            number of days to current window.
        """
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        """Calculate growth percentage with zero-denominator safeguard.

        Args:
            current_value (float): Current-period metric value.
            previous_value (float): Previous-period metric value.

        Returns:
            float: Growth in percent, rounded to 2 decimals.
        """
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    def _status_totals(self, dataframe: pd.DataFrame) -> dict[str, dict[str, float]]:
        """Aggregate totals by user status for summary cards.

        Args:
            dataframe (pd.DataFrame): Raw/filtered deposit dataframe.

        Returns:
            dict[str, dict[str, float]]: Status keyed totals containing
            ``depo_amount``, ``qty``, and derived ``aov``.
        """
        base = {"new": {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}, "existing": {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0}}
        if dataframe.empty:
            return base

        filtered = dataframe.copy()
        filtered["user_status"] = filtered["user_status"].apply(self._normalize_status)
        filtered = filtered.loc[filtered["user_status"].isin(["new", "existing"])].copy()
        filtered["first_depo"] = pd.to_numeric(filtered["first_depo"], errors="coerce").fillna(0.0)
        filtered = filtered.loc[filtered["first_depo"] > 0].copy()
        if filtered.empty:
            return base

        amount = (
            filtered.groupby("user_status", as_index=False)["first_depo"]
            .sum()
            .rename(columns={"first_depo": "depo_amount"})
        )
        qty = (
            filtered.groupby("user_status", as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "qty"})
        )
        merged = amount.merge(qty, on="user_status", how="outer").fillna(0)
        for _, row in merged.iterrows():
            status_key = str(row["user_status"]).strip().lower()
            if status_key not in {"new", "existing"}:
                continue
            depo_amount = float(row.get("depo_amount", 0) or 0)
            qty_value = float(row.get("qty", 0) or 0)
            base[status_key]["depo_amount"] = round(depo_amount, 2)
            base[status_key]["qty"] = int(qty_value)
            base[status_key]["aov"] = round(depo_amount / qty_value, 2) if qty_value else 0.0
        return base

    async def _summary_with_growth(self, campaign_type: str | None = None) -> dict[str, object]:
        """Build summary totals and growth-versus-previous-period payload.

        Args:
            campaign_type (str | None): Optional campaign type filter.
                ``None`` means all campaign types.

        Returns:
            dict[str, object]: Summary object with current/previous period
            totals and growth percentages per status + metric.
        """
        current_df = self.df_depo.copy()
        if campaign_type:
            current_df = current_df.loc[current_df["campaign_type"] == campaign_type].copy()

        previous_from, previous_to = self._previous_period_range(self.from_date, self.to_date)
        previous_df = await self._read_depo_db_with_range(previous_from, previous_to)
        if campaign_type and not previous_df.empty:
            previous_df = previous_df.loc[previous_df["campaign_type"] == campaign_type].copy()

        current_totals = self._status_totals(current_df)
        previous_totals = self._status_totals(previous_df)
        growth: dict[str, dict[str, float]] = {"new": {}, "existing": {}}
        for status_key in ("new", "existing"):
            for metric_key in ("depo_amount", "qty", "aov"):
                growth[status_key][metric_key] = self._growth_percentage(
                    float(current_totals[status_key][metric_key]),
                    float(previous_totals[status_key][metric_key]),
                )

        return {
            "current_period": {
                "from_date": self.from_date.isoformat(),
                "to_date": self.to_date.isoformat(),
                "totals": current_totals,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "totals": previous_totals,
            },
            "growth_percentage": growth,
        }

    async def build_daily_report_payload(self, campaign_type: str | None = None) -> dict[str, object]:
        """Build full daily cross-tab report payload for frontend rendering.

        Args:
            campaign_type (str | None): Optional campaign type filter.
                ``None`` includes all rows.

        Returns:
            dict[str, object]: Report payload containing:
                - ``timeline`` ordered daily ISO dates.
                - ``sections`` table blocks (`TOTAL` + each campaign).
                - ``campaign_type`` active filter marker.
                - ``summary`` totals + growth for metric cards.

        Raises:
            ValueError: Raised when ``from_date`` is after ``to_date``.
        """
        if self.from_date > self.to_date:
            raise ValueError("from_date cannot be after to_date.")

        dates = pd.date_range(start=self.from_date, end=self.to_date, freq="D").date.tolist()
        timeline = [day.isoformat() for day in dates]
        base_df = self.df_depo.copy()
        if base_df.empty:
            return {
                "timeline": timeline,
                "sections": [],
                "campaign_type": campaign_type or "all",
                "summary": await self._summary_with_growth(campaign_type=campaign_type),
            }

        base_df["user_status"] = base_df["user_status"].apply(self._normalize_status)
        base_df = base_df.loc[base_df["user_status"].isin(["new", "existing"])].copy()
        base_df["first_depo"] = pd.to_numeric(base_df["first_depo"], errors="coerce").fillna(0.0)
        base_df = base_df.loc[base_df["tanggal_regis"].between(self.from_date, self.to_date)]
        if campaign_type:
            base_df = base_df.loc[base_df["campaign_type"] == campaign_type]

        if base_df.empty:
            return {
                "timeline": timeline,
                "sections": [],
                "campaign_type": campaign_type or "all",
                "summary": await self._summary_with_growth(campaign_type=campaign_type),
            }

        positive_df = base_df.loc[base_df["first_depo"] > 0].copy()
        amount_agg = (
            positive_df.groupby(["tanggal_regis", "user_status"], as_index=False)["first_depo"]
            .sum()
            .rename(columns={"first_depo": "depo_amount"})
        )
        qty_agg = (
            positive_df.groupby(["tanggal_regis", "user_status"], as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "qty"})
        )
        merged = amount_agg.merge(
            qty_agg,
            on=["tanggal_regis", "user_status"],
            how="outer",
        ).fillna(0)

        # Total section first
        sections: list[dict[str, object]] = [
            self._build_section_payload(
                title="TOTAL",
                campaign_id="TOTAL",
                campaign_name="TOTAL",
                dates=dates,
                dataframe=merged,
            )
        ]

        campaign_meta = (
            base_df[["campaign_id", "campaign_name"]]
            .drop_duplicates()
            .sort_values(["campaign_name", "campaign_id"])
        )

        for _, meta_row in campaign_meta.iterrows():
            campaign_id = str(meta_row["campaign_id"])
            campaign_name = str(meta_row["campaign_name"])
            campaign_positive = positive_df.loc[positive_df["campaign_id"] == campaign_id].copy()
            if campaign_positive.empty:
                campaign_merged = pd.DataFrame(columns=["tanggal_regis", "user_status", "depo_amount", "qty"])
            else:
                campaign_amount = (
                    campaign_positive.groupby(["tanggal_regis", "user_status"], as_index=False)["first_depo"]
                    .sum()
                    .rename(columns={"first_depo": "depo_amount"})
                )
                campaign_qty = (
                    campaign_positive.groupby(["tanggal_regis", "user_status"], as_index=False)["email"]
                    .nunique()
                    .rename(columns={"email": "qty"})
                )
                campaign_merged = campaign_amount.merge(
                    campaign_qty,
                    on=["tanggal_regis", "user_status"],
                    how="outer",
                ).fillna(0)

            sections.append(
                self._build_section_payload(
                    title=campaign_name,
                    campaign_id=campaign_id,
                    campaign_name=campaign_name,
                    dates=dates,
                    dataframe=campaign_merged,
                )
            )

        return {
            "timeline": timeline,
            "sections": sections,
            "campaign_type": campaign_type or "all",
            "summary": await self._summary_with_growth(campaign_type=campaign_type),
        }

    def _build_section_payload(
        self,
        title: str,
        campaign_id: str,
        campaign_name: str,
        dates: list[date],
        dataframe: pd.DataFrame,
    ) -> dict[str, object]:
        """Build one table section payload for a single campaign block.

        Args:
            title (str): Display title shown in section header row.
            campaign_id (str): Campaign identifier for this section.
            campaign_name (str): Campaign name used for metadata.
            dates (list[date]): Ordered timeline used as table columns.
            dataframe (pd.DataFrame): Pre-aggregated per-day/per-status data.

        Returns:
            dict[str, object]: Section payload with metric rows
            (`Depo Amount`, `Qty`, `AOV`) and date/status cell values.
        """
        metric_map = self._empty_metric_map(dates)
        if not dataframe.empty:
            for _, row in dataframe.iterrows():
                day_key = pd.to_datetime(row["tanggal_regis"]).date().isoformat()
                status_key = str(row["user_status"]).strip().lower()
                if status_key not in {"new", "existing"}:
                    continue
                amount = float(row.get("depo_amount", 0) or 0)
                qty = float(row.get("qty", 0) or 0)
                metric_map["depo_amount"][day_key][status_key] = round(amount, 2)
                metric_map["qty"][day_key][status_key] = int(qty)
                metric_map["aov"][day_key][status_key] = round(amount / qty, 2) if qty else 0.0

        return {
            "title": title,
            "campaign_id": campaign_id,
            "campaign_name": campaign_name,
            "rows": [
                {"metric": "Depo Amount ($)", "key": "depo_amount", "values": metric_map["depo_amount"]},
                {"metric": "Jumlah Depo (Qty)", "key": "qty", "values": metric_map["qty"]},
                {"metric": "AOV ($)", "key": "aov", "values": metric_map["aov"]},
            ],
        }
