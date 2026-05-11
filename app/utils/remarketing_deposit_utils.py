"""Remarketing deposit analytics helpers."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataMsDeposit


class RemarketingDepositData:
    """Load and aggregate MS1 remarketing deposit rows."""

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_depo = pd.DataFrame()

    @classmethod
    async def load_data(
        cls,
        session: AsyncSession,
        from_date: date,
        to_date: date,
    ) -> "RemarketingDepositData":
        instance = cls(session=session, from_date=from_date, to_date=to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        self.df_depo = await self._read_depo_db_with_range(self.from_date, self.to_date)

    async def _read_depo_db_with_range(self, from_date: date, to_date: date) -> pd.DataFrame:
        """Read rows where both last_activity and last_depo are inside the range."""
        query = (
            select(
                DataMsDeposit.last_activity.label("report_date"),
                DataMsDeposit.last_depo.label("last_depo"),
                DataMsDeposit.campaign_id.label("campaign_id"),
                Campaign.campaign_name.label("campaign_name"),
                Campaign.ad_type.label("campaign_type"),
                DataMsDeposit.user_status.label("user_status"),
                DataMsDeposit.email.label("email"),
                DataMsDeposit.last_depo_amount.label("deposit_amount"),
                DataMsDeposit.time_to_closing.label("time_to_closing"),
            )
            .join(DataMsDeposit.campaign)
            .filter(
                DataMsDeposit.last_activity.between(from_date, to_date),
                DataMsDeposit.last_depo.between(from_date, to_date),
            )
        )
        result = await self.session.execute(query)
        rows = result.fetchall()
        columns = [
            "report_date",
            "last_depo",
            "campaign_id",
            "campaign_name",
            "campaign_type",
            "user_status",
            "email",
            "deposit_amount",
            "time_to_closing",
        ]
        if not rows:
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(rows)
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.date
        df["last_depo"] = pd.to_datetime(df["last_depo"]).dt.date
        return df

    @staticmethod
    def _normalize_status(value: object) -> str | None:
        status = str(value or "").strip().lower()
        if status == "new":
            return "new"
        if status == "existing":
            return "existing"
        return None

    @staticmethod
    def _empty_metric_map(dates: list[date]) -> dict[str, dict[str, float]]:
        return {
            metric: {day.isoformat(): {"new": 0.0, "existing": 0.0} for day in dates}
            for metric in ("depo_amount", "qty", "aov")
        }

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float:
        if previous_value == 0:
            return 100.0 if current_value else 0.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    def _status_totals(self, dataframe: pd.DataFrame) -> dict[str, dict[str, float]]:
        base = {
            "new": {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0},
            "existing": {"depo_amount": 0.0, "qty": 0.0, "aov": 0.0},
        }
        if dataframe.empty:
            return base

        filtered = dataframe.copy()
        filtered["user_status"] = filtered["user_status"].apply(self._normalize_status)
        filtered = filtered.loc[filtered["user_status"].isin(["new", "existing"])].copy()
        filtered["deposit_amount"] = pd.to_numeric(filtered["deposit_amount"], errors="coerce").fillna(0.0)
        filtered = filtered.loc[filtered["deposit_amount"] > 0].copy()
        if filtered.empty:
            return base

        amount = (
            filtered.groupby("user_status", as_index=False)["deposit_amount"]
            .sum()
            .rename(columns={"deposit_amount": "depo_amount"})
        )
        qty = (
            filtered.groupby("user_status", as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "qty"})
        )
        merged = amount.merge(qty, on="user_status", how="outer").fillna(0)
        for _, row in merged.iterrows():
            status_key = str(row["user_status"]).strip().lower()
            depo_amount = float(row.get("depo_amount", 0) or 0)
            qty_value = float(row.get("qty", 0) or 0)
            base[status_key]["depo_amount"] = round(depo_amount, 2)
            base[status_key]["qty"] = int(qty_value)
            base[status_key]["aov"] = round(depo_amount / qty_value, 2) if qty_value else 0.0
        return base

    async def _summary_with_growth(self, campaign_type: str | None = None) -> dict[str, object]:
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
        if self.from_date > self.to_date:
            raise ValueError("from_date cannot be after to_date.")

        dates = pd.date_range(start=self.from_date, end=self.to_date, freq="D").date.tolist()
        timeline = [day.isoformat() for day in dates]
        base_df = self.df_depo.copy()
        if base_df.empty:
            return {
                "timeline": timeline,
                "daily_metrics": [],
                "campaign_totals": [],
                "deposit_method_summary": self._build_deposit_method_summary(dataframe=base_df),
                "campaign_type": campaign_type or "all",
                "summary": await self._summary_with_growth(campaign_type=campaign_type),
            }

        base_df["user_status"] = base_df["user_status"].apply(self._normalize_status)
        base_df = base_df.loc[base_df["user_status"].isin(["new", "existing"])].copy()
        base_df["deposit_amount"] = pd.to_numeric(base_df["deposit_amount"], errors="coerce").fillna(0.0)
        base_df = base_df.loc[base_df["report_date"].between(self.from_date, self.to_date)]
        if campaign_type:
            base_df = base_df.loc[base_df["campaign_type"] == campaign_type]

        if base_df.empty:
            return {
                "timeline": timeline,
                "daily_metrics": [],
                "campaign_totals": [],
                "deposit_method_summary": self._build_deposit_method_summary(dataframe=base_df),
                "campaign_type": campaign_type or "all",
                "summary": await self._summary_with_growth(campaign_type=campaign_type),
            }

        positive_df = base_df.loc[base_df["deposit_amount"] > 0].copy()
        amount_agg = (
            positive_df.groupby(["report_date", "user_status"], as_index=False)["deposit_amount"]
            .sum()
            .rename(columns={"report_date": "tanggal_regis", "deposit_amount": "depo_amount"})
        )
        qty_agg = (
            positive_df.groupby(["report_date", "user_status"], as_index=False)["email"]
            .nunique()
            .rename(columns={"report_date": "tanggal_regis", "email": "qty"})
        )
        merged = amount_agg.merge(qty_agg, on=["tanggal_regis", "user_status"], how="outer").fillna(0)

        return {
            "timeline": timeline,
            "daily_metrics": self._build_daily_metrics_payload(dates=dates, dataframe=merged),
            "campaign_totals": self._build_campaign_totals_payload(dataframe=positive_df),
            "deposit_method_summary": self._build_deposit_method_summary(dataframe=positive_df),
            "campaign_type": campaign_type or "all",
            "summary": await self._summary_with_growth(campaign_type=campaign_type),
        }

    def _build_daily_metrics_payload(
        self,
        dates: list[date],
        dataframe: pd.DataFrame,
    ) -> list[dict[str, object]]:
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

        return [
            {
                "date": day.isoformat(),
                "depo_amount": metric_map["depo_amount"][day.isoformat()],
                "qty": metric_map["qty"][day.isoformat()],
                "aov": metric_map["aov"][day.isoformat()],
            }
            for day in dates
        ]

    def _build_campaign_totals_payload(self, dataframe: pd.DataFrame) -> list[dict[str, object]]:
        if dataframe.empty:
            return []

        amount = (
            dataframe.groupby(["campaign_id", "campaign_name", "user_status"], as_index=False)["deposit_amount"]
            .sum()
            .rename(columns={"deposit_amount": "depo_amount"})
        )
        qty = (
            dataframe.groupby(["campaign_id", "campaign_name", "user_status"], as_index=False)["email"]
            .nunique()
            .rename(columns={"email": "qty"})
        )
        merged = amount.merge(qty, on=["campaign_id", "campaign_name", "user_status"], how="outer").fillna(0)
        campaigns: dict[str, dict[str, object]] = {}
        for _, row in merged.iterrows():
            campaign_id = str(row["campaign_id"])
            status_key = str(row["user_status"]).strip().lower()
            if status_key not in {"new", "existing"}:
                continue
            campaign = campaigns.setdefault(
                campaign_id,
                {
                    "campaign_id": campaign_id,
                    "campaign_name": str(row["campaign_name"]),
                    "depo_amount": {"new": 0.0, "existing": 0.0},
                    "qty": {"new": 0, "existing": 0},
                    "aov": {"new": 0.0, "existing": 0.0},
                },
            )
            amount_value = float(row.get("depo_amount", 0) or 0)
            qty_value = float(row.get("qty", 0) or 0)
            campaign["depo_amount"][status_key] = round(amount_value, 2)
            campaign["qty"][status_key] = int(qty_value)
            campaign["aov"][status_key] = round(amount_value / qty_value, 2) if qty_value else 0.0
        return sorted(campaigns.values(), key=lambda item: str(item["campaign_name"]))

    def _build_deposit_method_summary(self, dataframe: pd.DataFrame) -> list[dict[str, object]]:
        method_order = ["close_with_consultant", "straight_to_deposit"]
        labels = {
            "close_with_consultant": "Close with Consultant",
            "straight_to_deposit": "Straight to Deposit",
        }
        base = {
            method: {
                "method": labels[method],
                "key": method,
                "deposit_qty": 0,
                "share_pct": 0.0,
                "deposit_amount": 0.0,
                "average_deposit": 0.0,
            }
            for method in method_order
        }
        if dataframe.empty:
            return [base[method] for method in method_order]

        grouped_df = dataframe.copy()
        grouped_df["deposit_method"] = grouped_df["time_to_closing"].fillna("").astype(str).str.strip().apply(
            lambda value: "straight_to_deposit" if value.startswith("-") else "close_with_consultant"
        )
        grouped_df["deposit_amount"] = pd.to_numeric(grouped_df["deposit_amount"], errors="coerce").fillna(0.0)
        grouped = (
            grouped_df.groupby("deposit_method", as_index=False)
            .agg(deposit_qty=("email", "nunique"), deposit_amount=("deposit_amount", "sum"))
        )
        total_qty = int(grouped["deposit_qty"].sum()) if not grouped.empty else 0
        for _, row in grouped.iterrows():
            method = str(row["deposit_method"])
            if method not in base:
                continue
            qty = int(row.get("deposit_qty", 0) or 0)
            amount = float(row.get("deposit_amount", 0) or 0)
            base[method]["deposit_qty"] = qty
            base[method]["share_pct"] = round((qty / total_qty) * 100, 2) if total_qty else 0.0
            base[method]["deposit_amount"] = round(amount, 2)
            base[method]["average_deposit"] = round(amount / qty, 2) if qty else 0.0
        return [base[method] for method in method_order]
