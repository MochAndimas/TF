"""Chart payload helpers shared across Streamlit pages."""

from __future__ import annotations

import plotly.graph_objects as go


def campaign_figure_from_payload(payload: dict | None, title: str) -> go.Figure:
    """Convert serialized Plotly payload into a display-ready figure object."""
    if payload and isinstance(payload, dict):
        figure = go.Figure(payload)
        if figure.data and isinstance(figure.data[0], go.Table):
            table_trace = figure.data[0]
            header_values = [str(value).strip().lower() for value in (table_trace.header.values or [])]
            text_only_headers = {"ad name", "ad group", "campaign", "campaign_name"}
            formatted_columns = []
            for column_index, column_values in enumerate(table_trace.cells.values or []):
                header_name = header_values[column_index] if column_index < len(header_values) else ""
                formatted_values = []
                for value in column_values:
                    if header_name in text_only_headers:
                        formatted_values.append(value)
                        continue
                    try:
                        number = float(value)
                        if "share" in header_name:
                            formatted_values.append(f"{number:,.2f}".rstrip("0").rstrip("."))
                        else:
                            formatted_values.append(f"{int(number):,}" if number.is_integer() else f"{number:,.2f}")
                    except (TypeError, ValueError):
                        formatted_values.append(value)
                formatted_columns.append(formatted_values)

            figure.update_traces(
                header=dict(fill_color="#1f2937", font=dict(color="#f8fafc", size=14), align="center"),
                cells=dict(
                    values=formatted_columns,
                    fill_color="#0f172a",
                    font=dict(color="#e2e8f0", size=13),
                    align="center",
                    height=34,
                ),
                domain=dict(y=[0.24, 0.76]),
                selector=dict(type="table"),
            )
            figure.update_layout(margin=dict(l=0, r=0, t=56, b=0), height=430)
        return figure

    figure = go.Figure()
    figure.update_layout(
        title=title,
        annotations=[{"text": "No data available", "xref": "paper", "yref": "paper", "x": 0.5, "y": 0.5, "showarrow": False}],
    )
    return figure
