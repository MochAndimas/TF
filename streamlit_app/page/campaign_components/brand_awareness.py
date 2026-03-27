"""Brand-awareness page helpers."""

from __future__ import annotations

import textwrap

import pandas as pd
import streamlit as st


def build_performance_dataframe(detail_rows: list[dict], level_column: str) -> pd.DataFrame:
    """Aggregate Brand Awareness detail rows by selected performance level."""
    if not detail_rows:
        return pd.DataFrame()
    details_df = pd.DataFrame(detail_rows)
    if details_df.empty:
        return pd.DataFrame()

    for column in ("spend", "impressions", "clicks"):
        details_df[column] = pd.to_numeric(details_df.get(column, 0), errors="coerce").fillna(0)
    details_df[level_column] = details_df.get(level_column, "N/A").fillna("N/A").replace("", "N/A")
    details_df["campaign_source"] = details_df.get("campaign_source", "N/A").fillna("N/A").replace("", "N/A")
    grouped = details_df.groupby(["campaign_source", level_column], as_index=False)[["spend", "impressions", "clicks"]].sum().sort_values("spend", ascending=False)
    grouped["ctr"] = grouped.apply(lambda row: round((float(row["clicks"]) / float(row["impressions"])) * 100, 2) if float(row["impressions"]) else 0.0, axis=1)
    grouped["cpm"] = grouped.apply(lambda row: round((float(row["spend"]) / float(row["impressions"])) * 1000, 2) if float(row["impressions"]) else 0.0, axis=1)
    grouped["cpc"] = grouped.apply(lambda row: round(float(row["spend"]) / float(row["clicks"]), 2) if float(row["clicks"]) else 0.0, axis=1)
    return grouped


def format_performance_display(df: pd.DataFrame, level_label: str) -> pd.DataFrame:
    """Format Brand Awareness dataframe values for table display."""
    if df.empty:
        return df
    formatted = df.copy()
    if level_label in formatted.columns:
        formatted[level_label] = formatted[level_label].astype(str).apply(lambda value: textwrap.fill(value, width=46, break_long_words=False))
    for col in ("Cost", "CPM", "CPC"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"Rp {float(v):,.0f}")
    for col in ("Impressions", "Clicks"):
        if col in formatted.columns:
            formatted[col] = formatted[col].apply(lambda v: f"{int(float(v)):,}")
    if "CTR" in formatted.columns:
        formatted["CTR"] = formatted["CTR"].apply(lambda v: f"{float(v):,.2f}%")
    return formatted


def build_performance_table(detail_rows: list[dict]):
    """Build BA performance table data and return the active level selection."""
    level_options = {
        "Ad Campaign Performance": ("campaign_id", "Campaign ID"),
        "Ad Group Performance": ("ad_group", "Ad Group"),
        "Ad Name Performance": ("ad_name", "Ad Name"),
    }
    selected_level = st.selectbox("Performance Table", options=list(level_options.keys()), key="brand_awareness_performance_level")
    level_column, level_label = level_options[selected_level]
    performance_df = build_performance_dataframe(detail_rows=detail_rows, level_column=level_column)
    if performance_df.empty:
        return selected_level, None, None, None

    display_df = performance_df.rename(columns={"campaign_source": "Ads Source", level_column: level_label, "spend": "Cost", "impressions": "Impressions", "clicks": "Clicks", "ctr": "CTR", "cpm": "CPM", "cpc": "CPC"})
    display_columns = ["Ads Source", level_label, "Cost", "Impressions", "Clicks", "CTR", "CPM", "CPC"]
    display_df = display_df[[column for column in display_columns if column in display_df.columns]]
    display_df = format_performance_display(display_df, level_label)

    return selected_level, level_column, level_label, display_df


def render_performance_table(level_label: str, display_df: pd.DataFrame) -> None:
    """Render the prepared BA performance table."""
    if display_df is None or display_df.empty:
        return

    with st.container(border=True):
        st.dataframe(
            display_df,
            width="stretch",
            hide_index=True,
            row_height=58,
            column_config={
                "Ads Source": st.column_config.TextColumn("Ads Source", width="small"),
                level_label: st.column_config.TextColumn(level_label, width="large"),
                "Cost": st.column_config.TextColumn("Cost", width="small"),
                "Impressions": st.column_config.TextColumn("Impressions", width="small"),
                "Clicks": st.column_config.TextColumn("Clicks", width="small"),
                "CTR": st.column_config.TextColumn("CTR", width="small"),
                "CPM": st.column_config.TextColumn("CPM", width="small"),
                "CPC": st.column_config.TextColumn("CPC", width="small"),
            },
        )
