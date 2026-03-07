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

    async def _ads_performance_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
        dimension: str,
        ad_type: str | None = None,
    ) -> pd.DataFrame:
        """Build ads performance dataframe grouped by selected dimension."""
        base = await self._ads_base_details_dataframe(
            data=data,
            from_date=from_date,
            to_date=to_date,
            ad_type=ad_type,
        )
        columns = [
            "campaign_source",
            "dimension_name",
            "spend",
            "impressions",
            "clicks",
            "leads",
            "click_to_leads_pct",
            "ctr_pct",
            "cpc",
            "cpm",
            "cost_leads",
        ]
        if base.empty:
            return pd.DataFrame(columns=columns)

        base[dimension] = base[dimension].fillna("N/A").replace("", "N/A")
        grouped = (
            base.groupby(["campaign_source", dimension], as_index=False)[
                ["impressions", "clicks", "spend", "leads"]
            ]
            .sum()
            .sort_values("spend", ascending=False)
        )
        grouped = grouped.rename(columns={dimension: "dimension_name"})
        grouped["click_to_leads_pct"] = grouped.apply(
            lambda row: round((float(row["leads"]) / float(row["clicks"])) * 100, 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )
        grouped["ctr_pct"] = grouped.apply(
            lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        grouped["cpc"] = grouped.apply(
            lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )
        grouped["cpm"] = grouped.apply(
            lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        grouped["cost_leads"] = grouped.apply(
            lambda row: round(float(row["spend"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0,
            axis=1,
        )
        return grouped[columns]

    async def user_acquisition_spend_vs_leads_chart(
        self,
        data: str,
        dimension: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build UA scatter chart mapping spend against leads for one source/dimension."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details = await self._ads_performance_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
            dimension=dimension,
            ad_type="user_acquisition",
        )
        if details.empty:
            details = await self._ads_performance_dataframe(
                data=data,
                from_date=start_date,
                to_date=end_date,
                dimension=dimension,
            )
        source_label = data.strip().replace("_", " ").title()
        if details.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} Spend vs Leads",
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
            rows: list[dict[str, object]] = []
        else:
            marker_sizes = (pd.to_numeric(details["clicks"], errors="coerce").fillna(0) / 40).clip(lower=8, upper=30)
            figure = go.Figure(
                data=[
                    go.Scatter(
                        x=details["spend"],
                        y=details["leads"],
                        mode="markers",
                        text=details["dimension_name"].astype(str),
                        marker=dict(
                            size=marker_sizes,
                            color=details["ctr_pct"].astype(float),
                            colorscale="Viridis",
                            showscale=True,
                            colorbar=dict(title="CTR %"),
                            line=dict(width=1),
                        ),
                        customdata=details[["clicks", "ctr_pct"]].to_numpy(),
                        hovertemplate=(
                            "<b>%{text}</b><br>"
                            "Spend: Rp %{x:,.0f}<br>"
                            "Leads: %{y:,}<br>"
                            "Clicks: %{customdata[0]:,.0f}<br>"
                            "CTR: %{customdata[1]:,.2f}%<extra></extra>"
                        ),
                    )
                ]
            )
            figure.update_layout(
                title=f"{source_label} Spend vs Leads",
                xaxis_title="Spend (Rp)",
                yaxis_title="Leads",
            )
            rows = await asyncio.to_thread(lambda: details.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "dimension": dimension,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def user_acquisition_top_leads_chart(
        self,
        data: str,
        dimension: str,
        top_n: int = 10,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build UA top-N leads ranking chart for one source/dimension."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        details = await self._ads_performance_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
            dimension=dimension,
            ad_type="user_acquisition",
        )
        if details.empty:
            details = await self._ads_performance_dataframe(
                data=data,
                from_date=start_date,
                to_date=end_date,
                dimension=dimension,
            )
        source_label = data.strip().replace("_", " ").title()
        if details.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"Top {top_n} {source_label} by Leads",
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
            rows: list[dict[str, object]] = []
        else:
            ranked = (
                details.sort_values("leads", ascending=False)
                .head(top_n)
                .copy()
                .sort_values("leads", ascending=True)
            )
            ranked["short_label"] = ranked["dimension_name"].astype(str).apply(
                lambda value: value if len(value) <= 38 else f"{value[:35]}..."
            )
            figure = go.Figure(
                data=[
                    go.Bar(
                        x=ranked["leads"],
                        y=ranked["short_label"],
                        orientation="h",
                        text=[f"{int(value):,}" for value in ranked["leads"]],
                        textposition="auto",
                        customdata=ranked["spend"],
                        hovertemplate=(
                            "<b>%{y}</b><br>"
                            "Leads: %{x:,}<br>"
                            "Spend: Rp %{customdata:,.0f}<extra></extra>"
                        ),
                    )
                ]
            )
            figure.update_layout(
                title=f"Top {top_n} {source_label} by Leads",
                xaxis_title="Leads",
                yaxis_title="",
            )
            rows = await asyncio.to_thread(lambda: ranked.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "dimension": dimension,
            "top_n": top_n,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def _user_acquisition_daily_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Build daily UA aggregates for spend/leads with fallback when campaign_type is missing."""
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

        columns = ["date", "cost", "leads"]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        ua_filtered = filtered.loc[filtered["campaign_type"] == "user_acquisition"]
        if not ua_filtered.empty:
            filtered = ua_filtered

        for column in ("cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = (
            filtered.groupby("date", as_index=False)[["cost", "leads"]]
            .sum()
            .sort_values("date")
        )
        return daily[columns]

    async def _user_acquisition_daily_dimension_dataframe(
        self,
        data: str,
        dimension: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Build daily UA dataframe grouped by dimension with spend/leads metrics."""
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

        columns = ["date", "dimension_name", "cost", "impressions", "clicks", "leads"]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        ua_filtered = filtered.loc[filtered["campaign_type"] == "user_acquisition"]
        if not ua_filtered.empty:
            filtered = ua_filtered

        filtered[dimension] = filtered[dimension].fillna("N/A").replace("", "N/A")
        for column in ("cost", "impressions", "clicks", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = (
            filtered.groupby(["date", dimension], as_index=False)[["cost", "impressions", "clicks", "leads"]]
            .sum()
            .sort_values(["date", dimension])
            .rename(columns={dimension: "dimension_name"})
        )
        return daily[columns]

    async def user_acquisition_ratio_trend_chart(
        self,
        data: str,
        dimension: str,
        metric: str,
        top_n: int = 6,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build UA trend chart for derived lead-efficiency metrics by selected dimension."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")
        metric_key = metric.strip().lower()
        if metric_key not in {"cost_per_lead", "click_per_lead", "click_through_lead"}:
            raise ValueError("metric must be one of: cost_per_lead, click_per_lead, click_through_lead.")

        daily = await self._user_acquisition_daily_dimension_dataframe(
            data=data,
            dimension=dimension,
            from_date=start_date,
            to_date=end_date,
        )
        source_label = data.strip().replace("_", " ").title()
        metric_title = {
            "cost_per_lead": "Cost per Lead",
            "click_per_lead": "Click per Lead",
            "click_through_lead": "Click Through Lead",
        }[metric_key]
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} {metric_title} Trend",
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
            rows: list[dict[str, object]] = []
        else:
            daily["cost"] = pd.to_numeric(daily["cost"], errors="coerce").fillna(0)
            daily["impressions"] = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0)
            daily["clicks"] = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0)
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = (
                daily.groupby("dimension_name", as_index=False)["leads"]
                .sum()
                .sort_values("leads", ascending=False)
                .head(top_n)["dimension_name"]
                .tolist()
            )
            selected = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()
            figure = go.Figure()
            for dimension_name in top_dimensions:
                subset = selected.loc[selected["dimension_name"] == dimension_name].sort_values("date").copy()
                if subset.empty:
                    continue
                if metric_key == "cost_per_lead":
                    subset["metric_value"] = subset.apply(
                        lambda row: round(float(row["cost"]) / float(row["leads"]), 2)
                        if float(row["leads"])
                        else 0.0,
                        axis=1,
                    )
                elif metric_key == "click_per_lead":
                    subset["metric_value"] = subset.apply(
                        lambda row: round(float(row["clicks"]) / float(row["leads"]), 2)
                        if float(row["leads"])
                        else 0.0,
                        axis=1,
                    )
                else:
                    subset["metric_value"] = subset.apply(
                        lambda row: round((float(row["leads"]) / float(row["clicks"])) * 100, 2)
                        if float(row["clicks"])
                        else 0.0,
                        axis=1,
                    )

                short_label = str(dimension_name)
                if len(short_label) > 28:
                    short_label = f"{short_label[:25]}..."
                date_labels = pd.to_datetime(subset["date"]).dt.strftime("%b %d\n%Y").tolist()
                hover_suffix = "%" if metric_key == "click_through_lead" else ""
                figure.add_trace(
                    go.Scatter(
                        x=date_labels,
                        y=subset["metric_value"],
                        mode="lines+markers",
                        name=short_label,
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "<b>%{x}</b><br>"
                            f"{metric_title}: "
                            "%{y:,.2f}"
                            f"{hover_suffix}<extra></extra>"
                        ),
                    )
                )

            figure.update_layout(
                title=f"{source_label} {metric_title} Trend ({dimension.replace('_', ' ').title()})",
                xaxis_title="Date",
                xaxis=dict(type="category"),
                yaxis_title=metric_title,
                legend=dict(orientation="h", y=1.12, x=0),
                showlegend=False if dimension == "campaign_id" else True,
            )
            serializable = selected.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "dimension": dimension,
            "metric": metric_key,
            "top_n": top_n,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def user_acquisition_cumulative_chart(
        self,
        data: str,
        dimension: str,
        top_n: int = 6,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build cumulative leads vs cumulative spend chart by selected dimension."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        daily = await self._user_acquisition_daily_dimension_dataframe(
            data=data,
            dimension=dimension,
            from_date=start_date,
            to_date=end_date,
        )
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} Cumulative Leads vs Spend ({dimension.replace('_', ' ').title()})",
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
            rows: list[dict[str, object]] = []
        else:
            daily["cost"] = pd.to_numeric(daily["cost"], errors="coerce").fillna(0)
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = (
                daily.groupby("dimension_name", as_index=False)["leads"]
                .sum()
                .sort_values("leads", ascending=False)
                .head(top_n)["dimension_name"]
                .tolist()
            )
            selected = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()
            figure = go.Figure()
            for dimension_name in top_dimensions:
                subset = selected.loc[selected["dimension_name"] == dimension_name].sort_values("date").copy()
                if subset.empty:
                    continue
                subset["cumulative_cost"] = subset["cost"].cumsum()
                subset["cumulative_leads"] = subset["leads"].cumsum()
                short_label = str(dimension_name)
                if len(short_label) > 30:
                    short_label = f"{short_label[:27]}..."
                figure.add_trace(
                    go.Scatter(
                        x=subset["cumulative_cost"],
                        y=subset["cumulative_leads"],
                        mode="lines+markers",
                        name=short_label,
                        hovertemplate=(
                            "<b>%{fullData.name}</b><br>"
                            "Cum Spend: Rp %{x:,.0f}<br>"
                            "Cum Leads: %{y:,.0f}<extra></extra>"
                        ),
                    )
                )
            figure.update_layout(
                title=f"{source_label} Cumulative Leads vs Spend ({dimension.replace('_', ' ').title()})",
                xaxis_title="Cumulative Spend",
                yaxis_title="Cumulative Leads",
                legend=dict(orientation="h", y=1.12, x=0),
            )
            serializable = selected.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "dimension": dimension,
            "top_n": top_n,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def user_acquisition_daily_mix_chart(
        self,
        data: str,
        dimension: str,
        top_n: int = 6,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build stacked daily mix chart for UA leads by selected dimension."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        daily = await self._user_acquisition_daily_dimension_dataframe(
            data=data,
            dimension=dimension,
            from_date=start_date,
            to_date=end_date,
        )
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} Daily Mix ({dimension.replace('_', ' ').title()})",
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
            rows: list[dict[str, object]] = []
        else:
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = (
                daily.groupby("dimension_name", as_index=False)["leads"]
                .sum()
                .sort_values("leads", ascending=False)
                .head(top_n)["dimension_name"]
                .tolist()
            )
            filtered = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()

            pivot = (
                filtered.pivot_table(
                    index="date",
                    columns="dimension_name",
                    values="leads",
                    aggfunc="sum",
                    fill_value=0,
                )
                .reset_index()
            )
            all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
            base_dates = pd.DataFrame({"date": all_dates.date})
            mix = base_dates.merge(pivot, on="date", how="left").fillna(0)

            figure = go.Figure()
            date_labels = pd.to_datetime(mix["date"]).dt.strftime("%b %d\n%Y").tolist()
            for dimension_name in top_dimensions:
                if dimension_name not in mix.columns:
                    continue
                short_label = str(dimension_name)
                if len(short_label) > 30:
                    short_label = f"{short_label[:27]}..."
                figure.add_trace(
                    go.Bar(
                        x=date_labels,
                        y=mix[dimension_name],
                        name=short_label,
                        hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>",
                    )
                )
            figure.update_layout(
                title=f"{source_label} Daily Mix ({dimension.replace('_', ' ').title()})",
                xaxis_title="Date",
                yaxis_title="Leads",
                barmode="stack",
                xaxis=dict(type="category"),
                legend=dict(orientation="h", y=1.12, x=0),
            )
            serializable = mix.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "source": data.strip().lower(),
            "dimension": dimension,
            "top_n": top_n,
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
            "figure": json.loads(chart_json),
        }

    async def _ads_base_details_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
        ad_type: str | None = None,
    ) -> pd.DataFrame:
        """Build base detailed dataframe used for frontend grouping/filtering."""
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

        columns = [
            "campaign_source",
            "campaign_id",
            "campaign_name",
            "ad_group",
            "ad_name",
            "spend",
            "impressions",
            "clicks",
            "leads",
        ]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        if ad_type:
            filtered = filtered.loc[filtered["campaign_type"] == ad_type]
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        for column in ("impressions", "clicks", "cost", "leads"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        for dim_col in ("campaign_name", "ad_group", "ad_name"):
            filtered[dim_col] = filtered[dim_col].fillna("N/A").replace("", "N/A")

        grouped = (
            filtered.groupby(["campaign_source", "campaign_id", "campaign_name", "ad_group", "ad_name"], as_index=False)[
                ["impressions", "clicks", "cost", "leads"]
            ]
            .sum()
            .sort_values("cost", ascending=False)
        )
        grouped = grouped.rename(columns={"cost": "spend"})
        return grouped[columns]

    async def _ads_performance_table_payload(
        self,
        details: pd.DataFrame,
        title: str,
        dimension_header: str,
    ) -> dict[str, object]:
        """Convert grouped performance dataframe to Plotly table payload."""
        if details.empty:
            figure = go.Figure()
            figure.update_layout(
                title=title,
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
            rows: list[dict[str, object]] = []
        else:
            serializable = details.copy()
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

            source_values = [
                str(value).replace("_", " ").title() if value not in ("", None) else "N/A"
                for value in serializable["campaign_source"].tolist()
            ]
            dimension_values = serializable["dimension_name"].tolist()
            spend_values = [f"Rp{float(value):,.0f}" for value in serializable["spend"].tolist()]
            impression_values = [f"{int(value):,}" for value in serializable["impressions"].tolist()]
            click_values = [f"{int(value):,}" for value in serializable["clicks"].tolist()]
            lead_values = [f"{int(value):,}" for value in serializable["leads"].tolist()]
            ctl_values = [f"{float(value):,.2f}%" for value in serializable["click_to_leads_pct"].tolist()]
            ctr_values = [f"{float(value):,.2f}%" for value in serializable["ctr_pct"].tolist()]
            cpc_values = [f"Rp{float(value):,.0f}" for value in serializable["cpc"].tolist()]
            cpm_values = [f"Rp{float(value):,.0f}" for value in serializable["cpm"].tolist()]
            cpl_values = [f"Rp{float(value):,.0f}" for value in serializable["cost_leads"].tolist()]

            figure = go.Figure(
                data=[
                    go.Table(
                        header=dict(
                            values=[
                                "Ads Source",
                                dimension_header,
                                "Cost",
                                "Impressions",
                                "Clicks",
                                "Leads",
                                "Avg. Click to Leads",
                                "Avg. CTR",
                                "CPC",
                                "CPM",
                                "Cost per Leads",
                            ]
                        ),
                        cells=dict(
                            values=[
                                source_values,
                                dimension_values,
                                spend_values,
                                impression_values,
                                click_values,
                                lead_values,
                                ctl_values,
                                ctr_values,
                                cpc_values,
                                cpm_values,
                                cpl_values,
                            ]
                        ),
                    )
                ]
            )
            figure.update_layout(title=title)

        table_json = await asyncio.to_thread(
            json.dumps,
            figure,
            cls=plotly.utils.PlotlyJSONEncoder,
        )
        return {
            "rows": rows,
            "figure": json.loads(table_json),
        }

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

    async def _brand_awareness_daily_dataframe(
        self,
        data: str,
        from_date: date,
        to_date: date,
    ) -> pd.DataFrame:
        """Build daily Brand Awareness dataframe by source."""
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

        columns = ["date", "cost", "impressions", "clicks", "ctr", "cpm", "cpc"]
        if df.empty:
            return pd.DataFrame(columns=columns)

        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        filtered = filtered.loc[filtered["campaign_type"] == "brand_awareness"]
        if filtered.empty:
            return pd.DataFrame(columns=columns)

        for column in ("cost", "impressions", "clicks"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)

        daily = (
            filtered.groupby("date", as_index=False)[["cost", "impressions", "clicks"]]
            .sum()
            .sort_values("date")
        )
        daily["ctr"] = daily.apply(
            lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        daily["cpm"] = daily.apply(
            lambda row: round((float(row["cost"]) / float(row["impressions"])) * 1000, 2)
            if float(row["impressions"])
            else 0.0,
            axis=1,
        )
        daily["cpc"] = daily.apply(
            lambda row: round(float(row["cost"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
            axis=1,
        )
        return daily[columns]

    async def brand_awareness_metrics(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, float]:
        """Calculate Brand Awareness metrics by source and date range."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._brand_awareness_daily_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
        )
        if daily.empty:
            return {
                "cost": 0.0,
                "impressions": 0,
                "clicks": 0,
                "ctr": 0.0,
                "cpm": 0.0,
                "cpc": 0.0,
            }

        totals = daily[["cost", "impressions", "clicks"]].agg("sum")
        cost_total = float(totals["cost"])
        impressions_total = float(totals["impressions"])
        clicks_total = float(totals["clicks"])
        ctr = round((clicks_total / impressions_total) * 100, 2) if impressions_total else 0.0
        cpm = round((cost_total / impressions_total) * 1000, 2) if impressions_total else 0.0
        cpc = round(cost_total / clicks_total, 2) if clicks_total else 0.0

        return {
            "cost": cost_total,
            "impressions": int(impressions_total),
            "clicks": int(clicks_total),
            "ctr": ctr,
            "cpm": cpm,
            "cpc": cpc,
        }

    async def brand_awareness_metrics_with_growth(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Calculate Brand Awareness metrics and growth vs previous period."""
        current_from = from_date or self.from_date
        current_to = to_date or self.to_date
        if current_from > current_to:
            raise ValueError("from_date cannot be after to_date.")

        previous_from, previous_to = self._previous_period_range(current_from, current_to)
        current_metrics = await self.brand_awareness_metrics(
            data=data,
            from_date=current_from,
            to_date=current_to,
        )
        previous_metrics = await self.brand_awareness_metrics(
            data=data,
            from_date=previous_from,
            to_date=previous_to,
        )
        growth = {
            metric: self._growth_percentage(
                current_value=float(current_metrics[metric]),
                previous_value=float(previous_metrics[metric]),
            )
            for metric in ("cost", "impressions", "clicks", "ctr", "cpm", "cpc")
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

    async def brand_awareness_spend_chart(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build Brand Awareness spend bar chart payload."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._brand_awareness_daily_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
        )
        source_label = data.strip().replace("_", " ").title()

        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} - Brand Awareness Spend",
                annotations=[
                    {
                        "text": "No brand awareness data for selected date range",
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
            figure = go.Figure(
                data=[
                    go.Bar(
                        x=date_labels,
                        y=cost_values,
                        name="Spend",
                        text=[f"Rp. {float(value):,.0f}" for value in cost_values],
                        textposition="inside",
                        hovertemplate="<b>%{x}</b><br>Spend: Rp. %{y:,.0f}<extra></extra>",
                    )
                ]
            )
            figure.update_layout(
                title=f"{source_label} - Brand Awareness Spend",
                xaxis_title="Date",
                yaxis_title="Spend",
                xaxis=dict(type="category"),
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

    async def brand_awareness_performance_chart(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build Brand Awareness performance combo chart payload."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._brand_awareness_daily_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
        )
        source_label = data.strip().replace("_", " ").title()

        if daily.empty:
            figure = go.Figure()
            figure.update_layout(
                title=f"{source_label} - Brand Awareness Performance",
                annotations=[
                    {
                        "text": "No brand awareness data for selected date range",
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
            impression_values = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0).tolist()
            click_values = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist()
            cpc_values = pd.to_numeric(daily["cpc"], errors="coerce").fillna(0).tolist()
            cpm_values = pd.to_numeric(daily["cpm"], errors="coerce").fillna(0).tolist()
            ctr_values = pd.to_numeric(daily["ctr"], errors="coerce").fillna(0).tolist()

            figure = go.Figure()
            figure.add_trace(
                go.Bar(
                    x=date_labels,
                    y=click_values,
                    name="Clicks",
                    hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Bar(
                    x=date_labels,
                    y=impression_values,
                    name="Impressions",
                    hovertemplate="<b>%{x}</b><br>Impressions: %{y:,}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Scatter(
                    x=date_labels,
                    y=cpc_values,
                    mode="lines+markers",
                    name="Cost Per Clicks",
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>CPC: Rp. %{y:,.2f}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Scatter(
                    x=date_labels,
                    y=cpm_values,
                    mode="lines+markers",
                    name="Cost Per Impressions",
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>CPM: Rp. %{y:,.2f}<extra></extra>",
                )
            )
            figure.add_trace(
                go.Scatter(
                    x=date_labels,
                    y=ctr_values,
                    mode="lines+markers",
                    name="Click Through Rate",
                    yaxis="y2",
                    hovertemplate="<b>%{x}</b><br>CTR: %{y:,.2f}%<extra></extra>",
                )
            )
            figure.update_layout(
                title=f"{source_label} - Brand Awareness Performance",
                xaxis_title="Date",
                xaxis=dict(type="category"),
                yaxis=dict(title="Clicks / Impressions"),
                yaxis2=dict(title="CPC / CPM / CTR", overlaying="y", side="right"),
                barmode="group",
                legend=dict(orientation="h", y=1.12, x=0),
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

    async def ads_campaign_details_table(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build raw details rows payload for frontend-side grouping/filtering.

        Args:
            data (str): Source key (`google`, `facebook`, `tiktok`).
            from_date (date | None): Optional inclusive start date.
            to_date (date | None): Optional inclusive end date.

        Returns:
            dict[str, object]: Payload containing source identifier, selected range,
            and raw detailed rows.

        Raises:
            ValueError: If date window is invalid.
        """
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details_df = await self._ads_base_details_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
        )
        rows = await asyncio.to_thread(lambda: details_df.to_dict(orient="records"))
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
        }

    async def brand_awareness_details_table(
        self,
        data: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, object]:
        """Build Brand Awareness detail rows payload for frontend grouping/filtering."""
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details_df = await self._ads_base_details_dataframe(
            data=data,
            from_date=start_date,
            to_date=end_date,
            ad_type="brand_awareness",
        )
        if not details_df.empty:
            details_df["ctr"] = details_df.apply(
                lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2)
                if float(row["impressions"])
                else 0.0,
                axis=1,
            )
            details_df["cpm"] = details_df.apply(
                lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2)
                if float(row["impressions"])
                else 0.0,
                axis=1,
            )
            details_df["cpc"] = details_df.apply(
                lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0,
                axis=1,
            )

        rows = await asyncio.to_thread(lambda: details_df.to_dict(orient="records"))
        return {
            "source": data.strip().lower(),
            "from_date": start_date.isoformat(),
            "to_date": end_date.isoformat(),
            "rows": rows,
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
