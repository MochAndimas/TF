"""User-acquisition chart builders for campaign services."""

from __future__ import annotations

import asyncio
import json
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import plotly.utils


class UserAcquisitionCampaignMixin:
    async def user_acquisition_spend_vs_leads_chart(self, data: str, dimension: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        details = await self._ads_performance_dataframe(data=data, from_date=start_date, to_date=end_date, dimension=dimension, ad_type="user_acquisition")
        if details.empty:
            details = await self._ads_performance_dataframe(data=data, from_date=start_date, to_date=end_date, dimension=dimension)
        source_label = data.strip().replace("_", " ").title()
        if details.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} Spend vs Leads", annotations=[{"text": "No campaign data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
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
                        marker=dict(size=marker_sizes, color=details["ctr_pct"].astype(float), colorscale="Viridis", showscale=True, colorbar=dict(title="CTR %"), line=dict(width=1)),
                        customdata=details[["clicks", "ctr_pct"]].to_numpy(),
                        hovertemplate="<b>%{text}</b><br>Spend: Rp %{x:,.0f}<br>Leads: %{y:,}<br>Clicks: %{customdata[0]:,.0f}<br>CTR: %{customdata[1]:,.2f}%<extra></extra>",
                    )
                ]
            )
            figure.update_layout(title=f"{source_label} Spend vs Leads", xaxis_title="Spend (Rp)", yaxis_title="Leads")
            rows = await asyncio.to_thread(lambda: details.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def user_acquisition_top_leads_chart(self, data: str, dimension: str, top_n: int = 10, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        details = await self._ads_performance_dataframe(data=data, from_date=start_date, to_date=end_date, dimension=dimension, ad_type="user_acquisition")
        if details.empty:
            details = await self._ads_performance_dataframe(data=data, from_date=start_date, to_date=end_date, dimension=dimension)
        source_label = data.strip().replace("_", " ").title()
        if details.empty:
            figure = go.Figure()
            figure.update_layout(title=f"Top {top_n} {source_label} by Leads", annotations=[{"text": "No campaign data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows: list[dict[str, object]] = []
        else:
            ranked = details.sort_values("leads", ascending=False).head(top_n).copy().sort_values("leads", ascending=True)
            ranked["short_label"] = ranked["dimension_name"].astype(str).apply(lambda value: value if len(value) <= 38 else f"{value[:35]}...")
            figure = go.Figure(data=[go.Bar(x=ranked["leads"], y=ranked["short_label"], orientation="h", text=[f"{int(value):,}" for value in ranked["leads"]], textposition="auto", customdata=ranked["spend"], hovertemplate="<b>%{y}</b><br>Leads: %{x:,}<br>Spend: Rp %{customdata:,.0f}<extra></extra>")])
            figure.update_layout(title=f"Top {top_n} {source_label} by Leads", xaxis_title="Leads", yaxis_title="")
            rows = await asyncio.to_thread(lambda: ranked.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "top_n": top_n, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def _user_acquisition_daily_dimension_dataframe(self, data: str, dimension: str, from_date: date, to_date: date) -> pd.DataFrame:
        source = data.strip().lower()
        frames = self._ads_frame_map()
        if source not in frames:
            supported_sources = ", ".join(sorted(frames.keys()))
            raise ValueError(f"Unsupported ads source '{data}'. Supported sources: {supported_sources}.")

        df = frames[source]
        if from_date < self.from_date or to_date > self.to_date:
            df = await self._read_ads_db_with_range(model=self._ads_model_map()[source], from_date=from_date, to_date=to_date)

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

    async def user_acquisition_ratio_trend_chart(self, data: str, dimension: str, metric: str, top_n: int = 6, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")
        metric_key = metric.strip().lower()
        if metric_key not in {"cost_per_lead", "click_per_lead", "click_through_lead"}:
            raise ValueError("metric must be one of: cost_per_lead, click_per_lead, click_through_lead.")

        daily = await self._user_acquisition_daily_dimension_dataframe(data=data, dimension=dimension, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        metric_title = {"cost_per_lead": "Cost per Lead", "click_per_lead": "Click per Lead", "click_through_lead": "Click Through Lead"}[metric_key]
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} {metric_title} Trend", annotations=[{"text": "No campaign data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows: list[dict[str, object]] = []
        else:
            daily["cost"] = pd.to_numeric(daily["cost"], errors="coerce").fillna(0)
            daily["impressions"] = pd.to_numeric(daily["impressions"], errors="coerce").fillna(0)
            daily["clicks"] = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0)
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = daily.groupby("dimension_name", as_index=False)["leads"].sum().sort_values("leads", ascending=False).head(top_n)["dimension_name"].tolist()
            selected = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()
            figure = go.Figure()
            for dimension_name in top_dimensions:
                subset = selected.loc[selected["dimension_name"] == dimension_name].sort_values("date").copy()
                if subset.empty:
                    continue
                if metric_key == "cost_per_lead":
                    subset["metric_value"] = subset.apply(lambda row: round(float(row["cost"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0, axis=1)
                elif metric_key == "click_per_lead":
                    subset["metric_value"] = subset.apply(lambda row: round(float(row["clicks"]) / float(row["leads"]), 2) if float(row["leads"]) else 0.0, axis=1)
                else:
                    subset["metric_value"] = subset.apply(lambda row: round((float(row["leads"]) / float(row["clicks"])) * 100, 2) if float(row["clicks"]) else 0.0, axis=1)

                short_label = str(dimension_name)
                if len(short_label) > 28:
                    short_label = f"{short_label[:25]}..."
                hover_suffix = "%" if metric_key == "click_through_lead" else ""
                figure.add_trace(go.Scatter(x=pd.to_datetime(subset["date"]).dt.strftime("%b %d\n%Y").tolist(), y=subset["metric_value"], mode="lines+markers", name=short_label, hovertemplate="<b>%{fullData.name}</b><br><b>%{x}</b><br>" f"{metric_title}: " "%{y:,.2f}" f"{hover_suffix}<extra></extra>"))

            figure.update_layout(title=f"{source_label} {metric_title} Trend ({dimension.replace('_', ' ').title()})", xaxis_title="Date", xaxis=dict(type="category"), yaxis_title=metric_title, legend=dict(orientation="h", y=1.12, x=0), showlegend=False if dimension == "campaign_id" else True)
            serializable = selected.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "metric": metric_key, "top_n": top_n, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def user_acquisition_cumulative_chart(self, data: str, dimension: str, top_n: int = 6, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        daily = await self._user_acquisition_daily_dimension_dataframe(data=data, dimension=dimension, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} Cumulative Leads vs Spend ({dimension.replace('_', ' ').title()})", annotations=[{"text": "No campaign data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows: list[dict[str, object]] = []
        else:
            daily["cost"] = pd.to_numeric(daily["cost"], errors="coerce").fillna(0)
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = daily.groupby("dimension_name", as_index=False)["leads"].sum().sort_values("leads", ascending=False).head(top_n)["dimension_name"].tolist()
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
                figure.add_trace(go.Scatter(x=subset["cumulative_cost"], y=subset["cumulative_leads"], mode="lines+markers", name=short_label, hovertemplate="<b>%{fullData.name}</b><br>Cum Spend: Rp %{x:,.0f}<br>Cum Leads: %{y:,.0f}<extra></extra>"))
            figure.update_layout(title=f"{source_label} Cumulative Leads vs Spend ({dimension.replace('_', ' ').title()})", xaxis_title="Cumulative Spend", yaxis_title="Cumulative Leads", legend=dict(orientation="h", y=1.12, x=0))
            serializable = selected.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "top_n": top_n, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def user_acquisition_daily_mix_chart(self, data: str, dimension: str, top_n: int = 6, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        if top_n <= 0:
            raise ValueError("top_n must be greater than zero.")

        daily = await self._user_acquisition_daily_dimension_dataframe(data=data, dimension=dimension, from_date=start_date, to_date=end_date)
        source_label = data.strip().replace("_", " ").title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"{source_label} Daily Mix ({dimension.replace('_', ' ').title()})", annotations=[{"text": "No campaign data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
            rows: list[dict[str, object]] = []
        else:
            daily["leads"] = pd.to_numeric(daily["leads"], errors="coerce").fillna(0)
            top_dimensions = daily.groupby("dimension_name", as_index=False)["leads"].sum().sort_values("leads", ascending=False).head(top_n)["dimension_name"].tolist()
            filtered = daily.loc[daily["dimension_name"].isin(top_dimensions)].copy()
            pivot = filtered.pivot_table(index="date", columns="dimension_name", values="leads", aggfunc="sum", fill_value=0).reset_index()
            all_dates = pd.date_range(start=start_date, end=end_date, freq="D")
            mix = pd.DataFrame({"date": all_dates.date}).merge(pivot, on="date", how="left").fillna(0)
            figure = go.Figure()
            date_labels = pd.to_datetime(mix["date"]).dt.strftime("%b %d\n%Y").tolist()
            for dimension_name in top_dimensions:
                if dimension_name not in mix.columns:
                    continue
                short_label = str(dimension_name)
                if len(short_label) > 30:
                    short_label = f"{short_label[:27]}..."
                figure.add_trace(go.Bar(x=date_labels, y=mix[dimension_name], name=short_label, hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>"))
            figure.update_layout(title=f"{source_label} Daily Mix ({dimension.replace('_', ' ').title()})", xaxis_title="Date", yaxis_title="Leads", barmode="stack", xaxis=dict(type="category"), legend=dict(orientation="h", y=1.12, x=0))
            serializable = mix.copy()
            serializable["date"] = pd.to_datetime(serializable["date"]).dt.date.astype(str)
            rows = await asyncio.to_thread(lambda: serializable.to_dict(orient="records"))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        return {"source": data.strip().lower(), "dimension": dimension, "top_n": top_n, "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def cost_to_leads_chart(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")

        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"Cost To Leads - {source_label}", annotations=[{"text": "No leads data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        else:
            date_labels = pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist()
            cost_values = pd.to_numeric(daily["cost"], errors="coerce").fillna(0).tolist()
            cpl_values = pd.to_numeric(daily["cost_leads"], errors="coerce").fillna(0).tolist()
            figure = go.Figure()
            figure.add_trace(go.Bar(x=date_labels, y=cost_values, name="Cost", text=[f"Rp. {float(value):,.0f}" for value in cost_values], textposition="auto", hovertemplate="<b>%{x}</b><br>Cost: Rp. %{y:,.0f}<extra></extra>"))
            figure.add_trace(go.Scatter(x=date_labels, y=cpl_values, mode="lines+markers", name="Cost Per Leads", yaxis="y2", hovertemplate="<b>%{x}</b><br>Cost/Leads: %{y:,.2f}<extra></extra>"))
            figure.update_layout(title=f"Cost To Leads - {source_label}", xaxis_title="Date", xaxis=dict(type="category"), yaxis=dict(title="Cost"), yaxis2=dict(title="Cost To Leads", overlaying="y", side="right"), legend=dict(orientation="h", y=1.1, x=0))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def leads_by_periods_chart(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"Leads By Periods - {source_label}", annotations=[{"text": "No leads data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        else:
            leads_values = pd.to_numeric(daily["leads"], errors="coerce").fillna(0).astype(int).tolist()
            figure = go.Figure(data=[go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=leads_values, name="Leads", text=leads_values, textposition="auto", hovertemplate="<b>%{x}</b><br>Leads: %{y:,}<extra></extra>")])
            figure.update_layout(title=f"Leads By Periods - {source_label}", xaxis_title="Date", xaxis=dict(type="category"), yaxis=dict(title="Total Leads"), legend=dict(orientation="h", y=1.1, x=0))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}

    async def clicks_to_leads_chart(self, data: str, from_date: date | None = None, to_date: date | None = None) -> dict[str, object]:
        start_date = from_date or self.from_date
        end_date = to_date or self.to_date
        if start_date > end_date:
            raise ValueError("from_date cannot be after to_date.")
        daily = await self._ads_daily_dataframe(data=data, from_date=start_date, to_date=end_date)
        source_label = data.strip().title()
        if daily.empty:
            figure = go.Figure()
            figure.update_layout(title=f"Clicks To Leads - {source_label}", annotations=[{"text": "No leads data for selected date range", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}])
        else:
            click_values = pd.to_numeric(daily["clicks"], errors="coerce").fillna(0).tolist()
            cpl_values = pd.to_numeric(daily["clicks_leads"], errors="coerce").fillna(0).tolist()
            figure = go.Figure()
            figure.add_trace(go.Bar(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=click_values, name="Clicks", text=[f"{int(float(value)):,}" for value in click_values], textposition="auto", hovertemplate="<b>%{x}</b><br>Clicks: %{y:,}<extra></extra>"))
            figure.add_trace(go.Scatter(x=pd.to_datetime(daily["date"]).dt.strftime("%b %d\n%Y").tolist(), y=cpl_values, mode="lines+markers", name="Clicks Per Leads", yaxis="y2", hovertemplate="<b>%{x}</b><br>Clicks/Leads: %{y:,.2f}<extra></extra>"))
            figure.update_layout(title=f"Clicks To Leads - {source_label}", xaxis_title="Date", xaxis=dict(type="category"), yaxis=dict(title="Clicks"), yaxis2=dict(title="Clicks To Leads", overlaying="y", side="right"), legend=dict(orientation="h", y=1.1, x=0))

        chart_json = await asyncio.to_thread(json.dumps, figure, cls=plotly.utils.PlotlyJSONEncoder)
        rows = await asyncio.to_thread(self._serialize_daily_rows, daily)
        return {"source": data.strip().lower(), "from_date": start_date.isoformat(), "to_date": end_date.isoformat(), "rows": rows, "figure": json.loads(chart_json)}
