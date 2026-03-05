from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.utils
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.external_api import Campaign, DataDepo, FacebookAds, GoogleAds, TikTokAds

AdsModel = GoogleAds | FacebookAds | TikTokAds


class CampaignData:
    """Service object for loading, aggregating, and visualizing campaign data.

    This class centralizes:
        - data loading from ads/deposit tables,
        - metric aggregation across date ranges,
        - growth computation against previous periods,
        - Plotly payload construction for dashboard widgets.

    Attributes:
        session (AsyncSession): Active async DB session.
        from_date (date): Base start date used during initial load.
        to_date (date): Base end date used during initial load.
        df_google (pd.DataFrame): Cached Google Ads rows in base window.
        df_facebook (pd.DataFrame): Cached Facebook Ads rows in base window.
        df_tiktok (pd.DataFrame): Cached TikTok Ads rows in base window.
        df_depo (pd.DataFrame): Cached deposit rows in base window.
    """

    def __init__(self, session: AsyncSession, from_date: date, to_date: date) -> None:
        """Initialize campaign data container.

        Args:
            session (AsyncSession): Database session used for all queries.
            from_date (date): Start date for the default in-memory data window.
            to_date (date): End date for the default in-memory data window.
        """
        self.session = session
        self.from_date = from_date
        self.to_date = to_date
        self.df_google = pd.DataFrame()
        self.df_facebook = pd.DataFrame()
        self.df_tiktok = pd.DataFrame()
        self.df_depo = pd.DataFrame()

    @classmethod
    async def load_data(cls, session: AsyncSession, from_date: date, to_date: date) -> "CampaignData":
        """Construct and fully preload campaign datasets.

        Args:
            session (AsyncSession): Database session used for data fetching.
            from_date (date): Start date for preload window.
            to_date (date): End date for preload window.

        Returns:
            CampaignData: Instance with all source DataFrames populated.
        """
        instance = cls(session, from_date, to_date)
        await instance._fetch_data()
        return instance

    async def _fetch_data(self) -> None:
        """Load all default source tables into instance caches.

        Returns:
            None: Populates ``df_google``, ``df_facebook``, ``df_tiktok``,
            and ``df_depo`` as side effects.
        """
        self.df_google = await self._read_ads_db(GoogleAds)
        self.df_facebook = await self._read_ads_db(FacebookAds)
        self.df_tiktok = await self._read_ads_db(TikTokAds)
        self.df_depo = await self._read_depo_db()

    async def _read_ads_db(self, model: type[AdsModel]) -> pd.DataFrame:
        """Read aggregated ads data for one model using base date window.

        Args:
            model (type[AdsModel]): Ads ORM model to query.

        Returns:
            pd.DataFrame: Aggregated rows in ``self.from_date`` to ``self.to_date``.
        """
        return await self._read_ads_db_with_range(
            model=model,
            from_date=self.from_date,
            to_date=self.to_date,
        )

    async def _read_ads_db_with_range(
        self,
        model: type[AdsModel],
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Read aggregated ads data for one model in an arbitrary date range.

        Args:
            model (type[AdsModel]): Ads ORM model to query.
            from_date (date): Inclusive start date.
            to_date (date): Inclusive end date.

        Returns:
            pd.DataFrame: Aggregated ads rows with campaign metadata.
        """
        query = (
            select(
                model.date.label("date"),
                model.campaign_id.label("campaign_id"),
                model.campaign_name.label("campaign_name"),
                model.ad_group.label("ad_group"),
                model.ad_name.label("ad_name"),
                Campaign.ad_source.label("campaign_source"),
                Campaign.ad_type.label("campaign_type"),
                func.sum(model.cost).label("cost"),
                func.sum(model.impressions).label("impressions"),
                func.sum(model.clicks).label("clicks"),
                func.sum(model.leads).label("leads"),
            )
            .join(model.campaign)
            .filter(func.date(model.date).between(from_date, to_date))
            .group_by(
                model.date,
                model.campaign_id,
                model.campaign_name,
                model.ad_group,
                model.ad_name,
                Campaign.ad_source,
                Campaign.ad_type,
            )
        )

        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return self._empty_ads_frame()

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    async def _read_depo_db(self) -> pd.DataFrame:
        """Read deposit records joined with campaign metadata.

        Returns:
            pd.DataFrame: Deposit records for the base window.
        """
        query = (
            select(
                DataDepo.tanggal_regis.label("tanggal_regis"),
                DataDepo.campaign_id.label("campaign_id"),
                Campaign.campaign_name.label("campaign_name"),
                Campaign.ad_source.label("campaign_source"),
                Campaign.ad_type.label("campaign_type"),
                DataDepo.user_status.label("user_status"),
                DataDepo.email.label("email"),
                DataDepo.first_depo.label("first_depo"),
            )
            .join(DataDepo.campaign)
            .filter(DataDepo.tanggal_regis.between(self.from_date, self.to_date))
        )

        result = await self.session.execute(query)
        rows = result.fetchall()
        if not rows:
            return self._empty_depo_frame()

        df = pd.DataFrame(rows)
        df["tanggal_regis"] = pd.to_datetime(df["tanggal_regis"]).dt.date
        return df

    def _empty_ads_frame(self) -> pd.DataFrame:
        """Build fallback ads DataFrame when no rows are available.

        Returns:
            pd.DataFrame: Single-row placeholder dataset with zero metrics.
        """
        return pd.DataFrame(
            {
                "date": [self.to_date],
                "campaign_id": ["No data"],
                "campaign_name": ["No data"],
                "ad_group": ["No data"],
                "ad_name": ["No data"],
                "campaign_source": ["No data"],
                "campaign_type": ["No data"],
                "cost": [0],
                "impressions": [0],
                "clicks": [0],
                "leads": [0],
            }
        )

    def _empty_depo_frame(self) -> pd.DataFrame:
        """Build fallback deposit DataFrame when no rows are available.

        Returns:
            pd.DataFrame: Single-row placeholder dataset with default values.
        """
        return pd.DataFrame(
            {
                "tanggal_regis": [self.to_date],
                "campaign_id": ["No data"],
                "campaign_name": ["No data"],
                "campaign_source": ["No data"],
                "campaign_type": ["No data"],
                "user_status": ["No data"],
                "email": ["No data"],
                "first_depo": [0.0],
            }
        )
    
    def _ads_frame_map(self) -> dict[str, pd.DataFrame]:
        """Map logical source keys to in-memory ads DataFrames.

        Returns:
            dict[str, pd.DataFrame]: Source-to-dataframe mapping.
        """
        return {
            "google": self.df_google,
            "facebook": self.df_facebook,
            "tiktok": self.df_tiktok,
        }

    @staticmethod
    def _ads_model_map() -> dict[str, type[AdsModel]]:
        """Map logical source keys to SQLAlchemy ads models.

        Returns:
            dict[str, type[AdsModel]]: Source-to-model mapping.
        """
        return {
            "google": GoogleAds,
            "facebook": FacebookAds,
            "tiktok": TikTokAds,
        }

    @staticmethod
    def _previous_period_range(from_date: date, to_date: date) -> tuple[date, date]:
        """Compute previous date window with identical duration.

        Args:
            from_date (date): Inclusive start of current period.
            to_date (date): Inclusive end of current period.

        Returns:
            tuple[date, date]: ``(previous_from, previous_to)``.
        """
        period_days = (to_date - from_date).days + 1
        previous_to = from_date - timedelta(days=1)
        previous_from = previous_to - timedelta(days=period_days - 1)
        return previous_from, previous_to

    @staticmethod
    def _growth_percentage(current_value: float, previous_value: float) -> float | None:
        """Compute percentage growth against previous value.

        Args:
            current_value (float): Current period value.
            previous_value (float): Previous period value.

        Returns:
            float | None: Percentage growth rounded to 2 decimals.
        """
        if previous_value == 0:
            if current_value == 0:
                return 0.0
            return 100.0
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    def _build_leads_by_source_table(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Build source-level leads summary table for selected date range.

        Args:
            start_date (date): Inclusive start date.
            end_date (date): Inclusive end date.

        Returns:
            pd.DataFrame: Sorted table with ``ad_source``, ``leads``, ``share_pct``.
        """
        rows: list[dict[str, object]] = []
        for source, frame in self._ads_frame_map().items():
            if frame.empty:
                leads_total = 0
            else:
                filtered = frame.loc[(frame["date"] >= start_date) & (frame["date"] <= end_date)].copy()
                if filtered.empty:
                    leads_total = 0
                else:
                    filtered = filtered.loc[filtered["campaign_id"] != "No data"]
                    filtered["leads"] = pd.to_numeric(filtered["leads"], errors="coerce").fillna(0)
                    leads_total = int(filtered["leads"].sum())

            rows.append(
                {
                    "ad_source": source,
                    "leads": leads_total,
                }
            )

        result = pd.DataFrame(rows)
        grand_total = int(result["leads"].sum()) if not result.empty else 0
        if grand_total == 0:
            result["share_pct"] = 0.0
        else:
            result["share_pct"] = (result["leads"] / grand_total * 100).round(2)

        return result.sort_values("leads", ascending=False).reset_index(drop=True)

    @staticmethod
    def _serialize_daily_rows(daily: pd.DataFrame) -> list[dict[str, object]]:
        """Convert daily dataframe rows to JSON-serializable dictionaries.

        Args:
            daily (pd.DataFrame): Daily aggregated dataframe.

        Returns:
            list[dict[str, object]]: Record list with stringified ``date``.
        """
        if daily.empty:
            return []
        serializable = daily.copy()
        serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
        return serializable.to_dict(orient="records")

    async def _ads_daily_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Build daily source-level aggregates for cost/clicks/leads.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date): Inclusive start date.
            to_date (date): Inclusive end date.

        Returns:
            pd.DataFrame: Daily aggregated metrics including ratio columns.

        Raises:
            ValueError: If the source key is unsupported.
        """
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(
                model=self._ads_model_map()[source],
                from_date=from_date,
                to_date=to_date,
            )

        if df.empty:
            return pd.DataFrame(columns=["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"])

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        if filtered.empty:
            return pd.DataFrame(columns=["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"])

        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return pd.DataFrame(columns=["date", "cost", "clicks", "leads", "cost_leads", "clicks_leads"])

        for column in ("cost", "clicks", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = (
            filtered.groupby("date", as_index=False)[["cost", "clicks", "leads"]]
            .sum()
            .sort_values("date")
        )
        daily["cost_leads"] = daily.apply(
            lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        daily["clicks_leads"] = daily.apply(
            lambda row: round(float(row["clicks"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        return daily

    async def _ads_campaign_details_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Build campaign-level detailed rows for selected source and date range.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date): Inclusive start date.
            to_date (date): Inclusive end date.

        Returns:
            pd.DataFrame: Campaign detail table including ``cost_leads``.

        Raises:
            ValueError: If the source key is unsupported.
        """
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(
                model=self._ads_model_map()[source],
                from_date=from_date,
                to_date=to_date,
            )

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "date",
                    "campaign_source",
                    "campaign_name",
                    "impressions",
                    "clicks",
                    "spend",
                    "leads",
                    "cost_leads",
                ]
            )

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return pd.DataFrame(
                columns=[
                    "date",
                    "campaign_source",
                    "campaign_name",
                    "impressions",
                    "clicks",
                    "spend",
                    "leads",
                    "cost_leads",
                ]
            )

        for column in ("impressions", "clicks", "cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        details = (
            filtered.groupby(["date", "campaign_source", "campaign_name"], as_index=False)[
                ["impressions", "clicks", "cost", "leads"]
            ]
            .sum()
            .sort_values(["date", "campaign_name"])
        )
        details = details.rename(columns={"cost": "spend"})
        details["cost_leads"] = details.apply(
            lambda row: round(float(row["spend"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        return details

    async def ads_metrics(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, float]:
        """Calculate total ads metrics for a source in date window.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, float]: Metrics dictionary containing impressions, clicks,
            cost, leads, and cost_leads.

        Raises:
            ValueError: If source key is unsupported or date window is invalid.
        """
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        df = frames[source]
        # Loaded frames are initialized with the object's base range.
        # Re-query DB when requested range falls outside that range (e.g. previous period growth).
        if start_date < self.from_date or end_date > self.to_date:
            df = await self._read_ads_db_with_range(
                model=self._ads_model_map()[source],
                from_date=start_date,
                to_date=end_date,
            )

        if df.empty:
            return {
                "impressions": 0,
                "clicks": 0,
                "cost": 0.0,
                "leads": 0,
                "cost_leads": 0.0,
            }

        filtered = df.loc[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
        if filtered.empty:
            return {
                "impressions": 0,
                "clicks": 0,
                "cost": 0.0,
                "leads": 0,
                "cost_leads": 0.0,
            }

        for column in ("impressions", "clicks", "cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        totals = filtered[["impressions", "clicks", "cost", "leads"]].agg("sum")
        leads_total = float(totals["leads"])
        cost_total = float(totals["cost"])

        return {
            "impressions": int(totals["impressions"]),
            "clicks": int(totals["clicks"]),
            "cost": cost_total,
            "leads": int(leads_total),
            "cost_leads": round(cost_total / leads_total, 2) if leads_total else 0.0,
        }

    async def ads_metrics_with_growth(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Calculate metrics and growth against an equivalent previous period.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing:
                - ``current_period`` metrics.
                - ``previous_period`` metrics.
                - ``growth_percentage`` by metric key.

        Raises:
            ValueError: If date window is invalid.
        """
        current_from = from_date or self.from_date
        current_to = to_date or self.to_date
        if current_from > current_to:
            raise ValueError("from_date cannot be after to_date.")

        previous_from, previous_to = self._previous_period_range(current_from, current_to)
        current_metrics = await self.ads_metrics(data=data, from_date=current_from, to_date=current_to)
        previous_metrics = await self.ads_metrics(data=data, from_date=previous_from, to_date=previous_to)

        growth = {
            metric: self._growth_percentage(
                current_value=float(current_metrics[metric]),
                previous_value=float(previous_metrics[metric]),
            )
            for metric in ("impressions", "clicks", "cost", "leads", "cost_leads")
        }

        return {
            "current_period": {
                "from_date": current_from.isoformat(),
                "to_date": current_to.isoformat(),
                "metrics": current_metrics,
            },
            "previous_period": {
                "from_date": previous_from.isoformat(),
                "to_date": previous_to.isoformat(),
                "metrics": previous_metrics,
            },
            "growth_percentage": growth,
        }

    async def leads_by_source_table(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build Plotly table payload for leads grouped by ad source.

        Args:
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Table payload containing selected range, row data,
            total leads, and Plotly-compatible figure JSON.

        Raises:
            ValueError: If ``from_date`` is after ``to_date``.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        result = await asyncio.to_thread(self._build_leads_by_source_table, start_date, end_date)
        rows = await asyncio.to_thread(lambda: result.to_dict(orient="records"))
        total_leads = int(result["leads"].sum()) if not result.empty else 0

        table_figure = go.Figure(
            data=[
                go.Table(
                    header=dict(values=["Ad Source", "Leads", "Share %"]),
                    cells=dict(
                        values=[
                            result["ad_source"].tolist(),
                            result["leads"].tolist(),
                            result["share_pct"].tolist(),
                        ]
                    ),
                )
            ]
        )
        table_figure.update_layout(title="Leads by Source (Table)")

        table_json = await asyncio.to_thread(
            json.dumps,
            table_figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "total_leads": total_leads,
            "figure": json.loads(table_json),
        }

    async def ads_campaign_details_table(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build campaign-details table payload including cost/leads metric.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Table payload containing source identifier,
            selected range, raw rows, and Plotly figure JSON.

        Raises:
            ValueError: If date window is invalid.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details = await self._ads_campaign_details_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()

        if details.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"Ads Campaign Details - {source_label}",
                annotations=[
                    {
                        "text": "No campaign data for selected date range",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
            rows = []
        else:
            serializable = details.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

            date_values = serializable["date"].tolist()
            source_values = serializable["campaign_source"].tolist()
            campaign_values = serializable["campaign_name"].tolist()
            impression_values = [f"{int(value):,}" for value in serializable["impressions"].tolist()]
            click_values = [f"{int(value):,}" for value in serializable["clicks"].tolist()]
            spend_values = [f"Rp. {float(value):,.0f}" for value in serializable["spend"].tolist()]
            lead_values = [f"{int(value):,}" for value in serializable["leads"].tolist()]
            cpl_values = [f"Rp. {float(value):,.2f}" for value in serializable["cost_leads"].tolist()]

            figure = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=[
                                "date",
                                "campaign_source",
                                "campaign_name",
                                "spend",
                                "impressions",
                                "clicks",
                                "leads",
                                "cost/leads",
                            ]
                        ),
                        cells=dict(
                            values=[
                                date_values,
                                source_values,
                                campaign_values,
                                spend_values,
                                impression_values,
                                click_values,
                                lead_values,
                                cpl_values,
                            ]
                        ),
                    )
                ]
            )
            figure.update_layout(title=f"Ads Campaign Details - {source_label}")

        table_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(table_json),
        }

    async def leads_by_source_pie_chart(
        self,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build Plotly pie chart payload for leads distribution by source.

        Args:
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing selected range and pie figure JSON.
            When no data exists, payload contains an empty-state annotated figure.
        """
        table_payload = await self.leads_by_source_table(from_date=from_date, to_date=to_date)
        table = pd.DataFrame(table_payload.get("rows", []))
        non_zero = table.loc[table["leads"] > 0] if not table.empty else pd.DataFrame()

        if non_zero.empty:
            figure = go.Figure()
            figure.update_layout(
                title="Leads by Source",
                annotations=[
                    {
                        "text": "No leads data for selected date range",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
            chart_json = await asyncio.to_thread(
                json.dumps,
                figure,
                cls=plotly.utils.PlotlyJSONEncoder,
            )
            return {
                "from_date": table_payload.get("from_date"),
                "to_date": table_payload.get("to_date"),
                "figure": json.loads(chart_json),
            }

        figure = go.Figure(
            data=[
                go.Pie(
                    labels=non_zero["ad_source"],
                    values=non_zero["leads"],
                    hole=0.35,
                    textinfo="percent+label",
                )
            ]
        )
        figure.update_layout(title="Leads by Source")

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "from_date": table_payload.get("from_date"),
            "to_date": table_payload.get("to_date"),
            "figure": json.loads(chart_json),
        }

    async def cost_to_leads_chart(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build combo chart payload for daily cost and cost-per-lead.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing source, range, serialized rows,
            and Plotly figure JSON.

        Raises:
            ValueError: If date window is invalid.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()

        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"Cost To Leads - {source_label}",
                annotations=[
                    {
                        "text": "No leads data for selected date range",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
        else:
            date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
            cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()
            cpl_values = pd.to_numeric(daily["cost_leads"], errors="coerce").fillna(0).tolist()
            figure = go.Figure()
            figure.add_trace(
                go.Bar(
                    x=date_labels,
                    y=cost_values,
                    name="Cost",
                    text=[f"Rp. {float(value):,.0f}" for value in cost_values],
                    textposition="auto",
                    hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Scatter(
                    x=date_labels,
                    y=cpl_values,
                    mode="lines+markers",
                    name="Cost Per Leads",
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>Cost/Leads: %{y:,.2f}<extra></extra>",
                )
            )
            figure.update_layout(
                title=f"Cost To Leads - {source_label}",
                xaxis_title="Date",
                xaxis=dict(type="category"),
                yaxis=dict(title="Cost"),
                yaxis2=dict(
                    title="Cost To Leads",
                    overlaying="y",
                    side="right",
                ),
                legend=dict(orientation="h", y=1.1, x=0),
            )

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def leads_by_periods_chart(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build bar chart payload for daily leads totals.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing source, range, serialized rows,
            and Plotly figure JSON.

        Raises:
            ValueError: If date window is invalid.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()

        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"Leads By Periods - {source_label}",
                annotations=[
                    {
                        "text": "No leads data for selected date range",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
        else:
            date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
            leads_values = pd.to_numeric(daily["leads"], errors="coerce").fillna(0).astype(int).tolist()
            figure = go.Figure(
                data=[
                    go.Bar(
                        x=date_labels,
                        y=leads_values,
                        name="Leads",
                        text=leads_values,
                        textposition="auto",
                        hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>",
                    )
                ]
            )
            figure.update_layout(
                title=f"Leads By Periods - {source_label}",
                xaxis_title="Date",
                xaxis=dict(type="category"),
                yaxis=dict(title="Total Leads"),
                legend=dict(orientation="h", y=1.1, x=0),
            )

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def clicks_to_leads_chart(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build combo chart payload for daily clicks and clicks-per-lead.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing source, range, serialized rows,
            and Plotly figure JSON.

        Raises:
            ValueError: If date window is invalid.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()

        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"Clicks To Leads - {source_label}",
                annotations=[
                    {
                        "text": "No leads data for selected date range",
                        "xref": "paper",
                        "yref": "paper",
                        "x": 0.5,
                        "y": 0.5,
                        "showarrow": False,
                    }
                ],
            )
        else:
            date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
            click_values = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist()
            cpl_values = pd.to_numeric(daily["clicks_leads"], errors="coerce").fillna(0).tolist()
            figure = go.Figure()
            figure.add_trace(
                go.Bar(
                    x=date_labels,
                    y=click_values,
                    name="Clicks",
                    text=[f"{int(float(value)):,}" for value in click_values],
                    textposition="auto",
                    hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Scatter(
                    x=date_labels,
                    y=cpl_values,
                    mode="lines+markers",
                    name="Clicks Per Leads",
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>Clicks/Leads: %{y:,.2f}<extra></extra>",
                )
            )
            figure.update_layout(
                title=f"Clicks To Leads - {source_label}",
                xaxis_title="Date",
                xaxis=dict(type="category"),
                yaxis=dict(title="Clicks"),
                yaxis2=dict(
                    title="Clicks To Leads",
                    overlaying="y",
                    side="right",
                ),
                legend=dict(orientation="h", y=1.1, x=0),
            )

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }
