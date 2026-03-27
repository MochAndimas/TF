"""Brand-awareness builders for campaign services."""

from __future__ import annotations

import asyncio
import json
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import plotly.utils


class BrandAwarenessCampaignMixin:
    async def _brand_awareness_daily_dataframe(self, data: str, from_date: date, to_date: date) -> pd.DataFrame:
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=from_date, to_date=to_date)

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

        daily = filtered.groupby("date", as_index=False)[["cost", "impressions", "clicks"]].sum().sort_values("date")
        daily["ctr"] = daily.apply(lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0, axis=1)
        daily["cpm"] = daily.apply(lambda row: round((float(row["cost"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0, axis=1)
        daily["cpc"] = daily.apply(lambda row: round(float(row["cost"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0, axis=1)
        return daily[columns]

    async def brand_awareness_metrics(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, float]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        daily = await self._brand_awareness_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        if daily.empty:
            return {"cost": 0.0, "impressions": 0, "clicks": 0, "ctr": 0.0, "cpm": 0.0, "cpc": 0.0}

        totals = daily[["cost", "impressions", "clicks"]].agg("sum")
        cost_total = float(totals["cost"])
        impressions_total = float(totals["impressions"])
        clicks_total = float(totals["clicks"])
        return {
            "cost": cost_total,
            "impressions": int(impressions_total),
            "clicks": int(clicks_total),
            "ctr": round((clicks_total / impressions_total) * 100, 2) if impressions_total else 0.0,
            "cpm": round((cost_total / impressions_total) * 1000, 2) if impressions_total else 0.0,
            "cpc": round(cost_total / clicks_total, 2) if clicks_total else 0.0,
        }

    async def brand_awareness_metrics_with_growth(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        current_from = from_date or self.from_date
        current_to = to_date or self.to_date
        if current_from > current_to:
            raise ValueError("from_date cannot be after to_date.")
        previous_from, previous_to = self._previous_period_range(current_from, current_to)
        current_metrics = await self.brand_awareness_metrics(data=data, from_date=current_from, to_date=current_to)
        previous_metrics = await self.brand_awareness_metrics(data=data, from_date=previous_from, to_date=previous_to)
        growth = {metric: self._growth_percentage(float(current_metrics[metric]), float(previous_metrics[metric])) for metric in ("cost", "impressions", "clicks", "ctr", "cpm", "cpc")}
        return {
            "current_period": {"from_date": current_from.isoformat(), "to_date": current_to.isoformat(), "metrics": current_metrics},
            "previous_period": {"from_date": previous_from.isoformat(), "to_date": previous_to.isoformat(), "metrics": previous_metrics},
            "growth_percentage": growth,
        }

    async def brand_awareness_spend_chart(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        daily = await self._brand_awareness_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} - Brand Awareness Spend", annotations=[{"text": "No brand awareness data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        else:
            cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()
            figure = go.Figure(data=[go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=cost_values, name="Spend", text=[f"Rp. {float(value):,.0f}" for value in cost_values], textposition="inside", hovertemplate="<b>%{x}</b><br>Spend: Rp. %{y:,.0f}<extra></extra>")])
            figure.update_layout(title=f"{source_label} - Brand Awareness Spend", xaxis_title="Date", yaxis_title="Spend", xaxis=dict(type="category"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def brand_awareness_performance_chart(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        daily = await self._brand_awareness_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} - Brand Awareness Performance", annotations=[{"text": "No brand awareness data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        else:
            date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
            impression_values = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0).tolist()
            click_values = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist()
            cpc_values = pd.to_numeric(daily["cpc"], errors="coerce").fillna(0).tolist()
            cpm_values = pd.to_numeric(daily["cpm"], errors="coerce").fillna(0).tolist()
            ctr_values = pd.to_numeric(daily["ctr"], errors="coerce").fillna(0).tolist()
            figure = go.Figure()
            figure.add_trace(go.Bar(x=date_labels, y=click_values, name="Clicks", hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>"))
            figure.add_trace(go.Bar(x=date_labels, y=impression_values, name="Impressions", hovertemplate="<b>%{x}</b><br>Impressions: %{y:,}<extra></extra>"))
            figure.add_trace(go.Scatter(x=date_labels, y=cpc_values, mode="lines+markers", name="Cost Per Clicks", yaxis="y2", hovertemplate="<b>%{x}</b><br>CPC: Rp. %{y:,.0f}<extra></extra>"))
            figure.add_trace(go.Scatter(x=date_labels, y=cpm_values, mode="lines+markers", name="Cost Per Impressions", yaxis="y2", hovertemplate="<b>%{x}</b><br>CPM: Rp. %{y:,.2f}<extra></extra>"))
            figure.add_trace(go.Scatter(x=date_labels, y=ctr_values, mode="lines+markers", name="Click Through Rate", yaxis="y2", hovertemplate="<b>%{x}</b><br>CTR: %{y:,.2f}%<extra></extra>"))
            figure.update_layout(title=f"{source_label} - Brand Awareness Performance", xaxis_title="Date", xaxis=dict(type="category"), yaxis=dict(title="Clicks / Impressions"), yaxis2=dict(title="CPC / CPM / CTR", overlaying="y", side="right"), barmode="group", legend=dict(orientation="h", y=1.12, x=0))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def brand_awareness_details_table(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        details_df = await self._ads_base_details_dataframe(data=data, from_date=start_date, to_date=end_date, ad_type="brand_awareness")
        if not details_df.empty:
            details_df["ctr"] = details_df.apply(lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0, axis=1)
            details_df["cpm"] = details_df.apply(lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0, axis=1)
            details_df["cpc"] = details_df.apply(lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0, axis=1)
        rows = await asyncio.to_thread(lambda: details_df.to_dict(orient="records"))
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows}

    async def _brand_awareness_daily_dimension_dataframe(self, data: str, dimension: str, from_date: date, to_date: date) -> pd.DataFrame:
        source = data.strip().lower()
        valid_dimensions = {"campaign_id", "ad_group", "ad_name"}
        if dimension not in valid_dimensions:
            supported = ", ".join(sorted(valid_dimensions))
            raise ValueError(f"Unsupported dimension '{dimension}'. Supported dimensions: {supported}.")
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")
        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=from_date, to_date=to_date)
        columns = ["date", "dimension_name", "cost", "impressions", "clicks"]
        if df.empty:
            return pd.DataFrame(columns=columns)
        filtered = df.loc[(df["date"] >= from_date) & (df["date"] <= to_date)].copy()
        filtered = filtered.loc[filtered["campaign_id"] != "No data"]
        filtered = filtered.loc[filtered["campaign_type"] == "brand_awareness"]
        if filtered.empty:
            return pd.DataFrame(columns=columns)
        filtered[dimension] = filtered[dimension].fillna("N/A").replace("", "N/A")
        for column in ("cost", "impressions", "clicks"):
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").fillna(0)
        daily = filtered.groupby(["date", dimension], as_index=False)[["cost", "impressions", "clicks"]].sum().sort_values(["date", dimension]).rename(columns={dimension: "dimension_name"})
        return daily[columns]

    async def brand_awareness_ratio_trend_chart(self, data: str, dimension: str, metric: str, top_n: int = 6, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")
        metric_key = metric.strip().lower()
        if metric_key not in {"ctr", "cpm", "cpc"}:
            raise ValueError("metric must be one of: ctr, cpm, cpc.")
        daily = await self._brand_awareness_daily_dimension_dataframe(data=data, dimension=dimension, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        metric_title = {"ctr": "CTR", "cpm": "CPM", "cpc": "CPC"}[metric_key]
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} {metric_title} Trend", annotations=[{"text": "No brand awareness data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows: list[dict[str, object]] = []
        else:
            daily["cost"] = pd.to_numeric(daily["cost"], errors="coerce").fillna(0)
            daily["impressions"] = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0)
            daily["clicks"] = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0)
            top_dimensions = daily.groupby("dimension_name", as_index=False)["clicks"].sum().sort_values("clicks", ascending=False).head(top_n)["dimension_name"].tolist()
            selected = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()
            figure = go.Figure()
            for dimension_name in top_dimensions:
                subset = selected.loc[selected["dimension_name"] == dimension_name].sort_values("date").copy()
                if subset.empty:
                    continue
                if metric_key == "ctr":
                    subset["metric_value"] = subset.apply(lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0, axis=1)
                elif metric_key == "cpm":
                    subset["metric_value"] = subset.apply(lambda row: round((float(row["cost"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0, axis=1)
                else:
                    subset["metric_value"] = subset.apply(lambda row: round(float(row["cost"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0, axis=1)
                short_label = str(dimension_name)
                if len(short_label) > 28:
                    short_label = f"{short_label[:25]}..."
                hover_suffix = "%" if metric_key == "ctr" else ""
                figure.add_trace(go.Scatter(x=pd.to_datetime(subset["date"]).dt.strftime("%b %d\n%Y").tolist(), y=subset["metric_value"], mode="lines+markers", name=short_label, hovertemplate="<b>%{fullData.name}</b><br><b>%{x}</b><br>" f"{metric_title}: " "%{y:,.2f}" f"{hover_suffix}<extra></extra>"))
            figure.update_layout(title=f"{source_label} {metric_title} Trend ({dimension.replace('_', ' ').title()})", xaxis_title="Date", xaxis=dict(type="category"), yaxis_title=metric_title, legend=dict(orientation="h", y=1.12, x=0), showlegend=False if dimension == "campaign_id" else True)
            serializable = selected.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))
        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "metric": metric_key, "top_n": top_n, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}
