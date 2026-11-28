"""Governance Dashboard (localhost:8050) — Workshop #4.

Audience: Data Platform / Engineering Team (User Story US-06).
Data Source: Exclusively `datalake_gold/governance_*.parquet` via `pd.read_parquet()`.
"""

from __future__ import annotations

import dash
from dash import Input, Output, dash_table, dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils import (
    PALETTE,
    TOPIC_COLORS,
    TOPIC_LABELS,
    build_footer,
    build_header,
    load_latest_gold,
    style_figure,
)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Governance Dashboard · Landslide Monitoring Colombia",
)


def _kpi_card(title: str, value: str, subtitle: str, accent: str) -> dbc.Col:
    return dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.Div(title, className="text-muted text-uppercase small fw-semibold mb-1"),
                    html.H3(value, className="fw-bold mb-1", style={"color": accent}),
                    html.Div(subtitle, className="text-muted small"),
                ]
            ),
            style={
                "borderTop": f"4px solid {accent}",
                "borderRadius": "0.5rem",
            },
            className="shadow-sm h-100",
        ),
        xl=3,
        md=6,
        sm=12,
        className="mb-3",
    )


def _empty_figure(title: str, message: str, source_note: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=12, color=PALETTE["slate"]),
    )
    return style_figure(fig, title=title, source_note=source_note, show_legend=False)


def _build_layout() -> dbc.Container:
    df, fname, updated = load_latest_gold("governance")
    topics = sorted(df["topic"].dropna().unique().tolist())
    topic_options = [{"label": "All Sources / Topics", "value": "ALL"}] + [
        {"label": f"{TOPIC_LABELS.get(t, t)} ({t})", "value": t} for t in topics
    ]

    return dbc.Container(
        [
            dcc.Interval(id="gov-auto-refresh", interval=60_000, n_intervals=0),
            html.Div(
                id="gov-header-container",
                children=build_header(
                    dashboard_title="Data Governance & Quality Observatory",
                    topic_angle="Rainfall-Triggered Landslide Risk Monitoring in Colombia (Medallion Gold Layer · US-06)",
                    audience_badge="TECHNICAL AUDIENCE · PORT 8050",
                    last_updated=updated,
                    source_file=fname,
                    refresh_button_id="gov-refresh-btn",
                ),
            ),
            # Filter bar
            dbc.Card(
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label(
                                        "Filter by Pipeline Topic / Source:",
                                        className="fw-semibold small mb-1",
                                    ),
                                    dcc.Dropdown(
                                        id="gov-topic-filter",
                                        options=topic_options,
                                        value="ALL",
                                        clearable=False,
                                    ),
                                ],
                                md=5,
                            ),
                            dbc.Col(
                                html.Div(
                                    [
                                        html.Span("Scope & Contract: ", className="fw-semibold small"),
                                        html.Span(
                                            "Audits completeness (null rates), uniqueness (key deduplication), "
                                            "validity (schema & physical bounds), accuracy (IQR/Z-score outliers), "
                                            "and timeliness (ingestion compliance & freshness).",
                                            className="text-muted small",
                                        ),
                                    ],
                                    className="mt-2 mt-md-4",
                                ),
                                md=7,
                            ),
                        ],
                        align="center",
                    )
                ),
                className="shadow-sm mb-3 border-0",
            ),
            # Row 1: KPI Summary Cards
            dbc.Row(id="gov-kpi-cards"),
            # Row 2: Null Rate + Volume Over Time
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="gov-null-chart", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="gov-volume-chart", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            # Row 3: Outlier & Flag Rate + SLA / Ingestion & Freshness
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="gov-outlier-chart", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="gov-sla-chart", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            # Row 4: Data Quality Summary Table
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H5(
                            "Data Quality Summary Table (KPIs vs. Governance Thresholds)",
                            className="fw-bold mb-1",
                            style={"color": PALETTE["primary"]},
                        ),
                        html.P(
                            "All governance KPIs computed in PySpark from the Silver layer and persisted in Gold Parquet.",
                            className="text-muted small mb-3",
                        ),
                        html.Div(id="gov-summary-table"),
                    ]
                ),
                className="shadow-sm border-0 mb-3",
            ),
            build_footer(),
        ],
        fluid=True,
        style={"backgroundColor": PALETTE["bg"], "minHeight": "100vh", "padding": "18px 24px"},
    )


app.layout = _build_layout


def _evaluate_status(kpi: str, val: float) -> Tuple[str, str]:
    """Returns (threshold_label, status_badge) for the governance table."""
    if kpi == "schema_compliance_rate":
        return ">= 99.0 %", "PASS" if val >= 99.0 else "ALERT"
    if kpi == "duplicate_rate_after":
        return "0.0 %", "PASS" if val == 0.0 else "ALERT"
    if kpi == "duplicate_rate_before":
        return "<= 15.0 % (deduped in Gold)", "INFO" if val <= 15.0 else "WARN"
    if kpi == "null_rate":
        return "<= 10.0 % (required = 0 %)", "PASS" if val <= 10.0 else "WARN"
    if kpi == "outlier_rate":
        return "<= 5.0 % (flagged, kept)", "PASS" if val <= 5.0 else "WARN"
    if kpi == "ingestion_compliance":
        return ">= 90.0 %", "PASS" if val >= 90.0 else "WARN"
    if kpi == "freshness_hours":
        return "<= 24.0 h (forecast/daily)", "PASS" if val <= 24.0 else "WARN"
    return "Monitored", "INFO"


@app.callback(
    Output("gov-header-container", "children"),
    Output("gov-kpi-cards", "children"),
    Output("gov-null-chart", "figure"),
    Output("gov-volume-chart", "figure"),
    Output("gov-outlier-chart", "figure"),
    Output("gov-sla-chart", "figure"),
    Output("gov-summary-table", "children"),
    Input("gov-topic-filter", "value"),
    Input("gov-refresh-btn", "n_clicks"),
    Input("gov-auto-refresh", "n_intervals"),
)
def update_governance(selected_topic: str, _n_clicks: int, _n_intervals: int):
    df, fname, updated = load_latest_gold("governance")
    source_note = f"Source: Gold Layer Parquet ({fname}) · PySpark Governance Audit"

    header = build_header(
        dashboard_title="Data Governance & Quality Observatory",
        topic_angle="Rainfall-Triggered Landslide Risk Monitoring in Colombia (Medallion Gold Layer · US-06)",
        audience_badge="TECHNICAL AUDIENCE · PORT 8050",
        last_updated=updated,
        source_file=fname,
        refresh_button_id="gov-refresh-btn",
    )

    sub = df if selected_topic == "ALL" else df[df["topic"] == selected_topic]

    # --- 1. KPI Summary Cards ---
    total_rows = sub[sub["kpi"] == "volume_rows"]["value"].sum()
    n_files = sub[sub["kpi"] == "volume_bronze_files"]["value"].sum()

    null_df = sub[sub["kpi"] == "null_rate"]
    avg_null = null_df["value"].mean() if not null_df.empty else 0.0
    max_null = null_df["value"].max() if not null_df.empty else 0.0

    dup_before = sub[sub["kpi"] == "duplicate_rate_before"]["value"].mean()
    dup_after = sub[sub["kpi"] == "duplicate_rate_after"]["value"].mean()
    dup_before = 0.0 if pd.isna(dup_before) else dup_before
    dup_after = 0.0 if pd.isna(dup_after) else dup_after

    comp_df = sub[sub["kpi"] == "schema_compliance_rate"]
    schema_comp = comp_df["value"].mean() if not comp_df.empty else 100.0

    cards = [
        _kpi_card(
            "Total Silver Records",
            f"{int(total_rows):,}",
            f"Across {int(n_files):,} ingested Bronze files",
            PALETTE["blue"],
        ),
        _kpi_card(
            "Mean Null Rate (All Fields)",
            f"{avg_null:.2f} %",
            f"Max column null rate: {max_null:.1f} %",
            PALETTE["amber"] if avg_null > 5 else PALETTE["teal"],
        ),
        _kpi_card(
            "Duplicate Rate (Before → After)",
            f"{dup_before:.1f}% → {dup_after:.1f}%",
            "Deduplicated on natural keys in Gold",
            PALETTE["teal"] if dup_after == 0 else PALETTE["red"],
        ),
        _kpi_card(
            "Schema Compliance Rate",
            f"{schema_comp:.1f} %",
            "Required non-null & physical range checks",
            PALETTE["teal"] if schema_comp >= 99 else PALETTE["red"],
        ),
    ]

    # --- 2. Null Rate Horizontal Bar Chart (ordered by severity) ---
    null_plot = null_df[null_df["value"] > 0].copy()
    if null_plot.empty:
        null_plot = null_df.copy()
    null_plot["label"] = null_plot["topic"] + " · " + null_plot["field"].fillna("unknown")
    null_plot = null_plot.sort_values("value", ascending=True).tail(15)

    if not null_plot.empty:
        fig_null = px.bar(
            null_plot,
            x="value",
            y="label",
            color="topic",
            orientation="h",
            text=null_plot["value"].map(lambda v: f"{v:.1f}%"),
            color_discrete_map=TOPIC_COLORS,
        )
        fig_null.update_traces(textposition="outside", cliponaxis=False)
        fig_null.update_xaxes(range=[0, max(112, null_plot["value"].max() * 1.15)])
        fig_null = style_figure(
            fig_null,
            title="Null Rate per Field Ordered by Severity (Top 15)",
            xaxis_title="Null / Empty Rate (%)",
            yaxis_title="Topic · Field",
            source_note=source_note,
        )
    else:
        fig_null = _empty_figure("Null Rate per Field", "No null records available.", source_note)

    # --- 3. Volume Over Time Chart ---
    vol_df = sub[sub["kpi"] == "volume_rows_per_day"].dropna(subset=["period"]).copy()
    vol_df = vol_df.sort_values("period")
    if not vol_df.empty:
        if vol_df["period"].nunique() > 1:
            fig_vol = px.line(
                vol_df,
                x="period",
                y="value",
                color="topic",
                markers=True,
                color_discrete_map=TOPIC_COLORS,
            )
        else:
            fig_vol = px.bar(
                vol_df,
                x="period",
                y="value",
                color="topic",
                barmode="group",
                text_auto=",.0f",
                color_discrete_map=TOPIC_COLORS,
            )
        fig_vol = style_figure(
            fig_vol,
            title="Ingested Record Volume Over Processing Days by Source",
            xaxis_title="Processing Date (UTC)",
            yaxis_title="Ingested Records (rows)",
            source_note=source_note,
        )
    else:
        fig_vol = _empty_figure("Volume Over Time", "No daily volume rows found.", source_note)

    # --- 4. Outlier & Quality Flag Rate Chart ---
    out_df = sub[sub["kpi"].isin(["outlier_rate", "flag_rate"])].copy()
    if not out_df.empty:
        out_df["metric_type"] = out_df["kpi"].map(
            {"outlier_rate": "Statistical Outlier (IQR / Z-score)", "flag_rate": "Domain Quality Flag"}
        )
        out_df["label"] = out_df["topic"] + " · " + out_df["field"].fillna("")
        out_df = out_df.sort_values("value", ascending=True)
        fig_out = px.bar(
            out_df,
            x="value",
            y="label",
            color="metric_type",
            orientation="h",
            text=out_df["value"].map(lambda v: f"{v:.1f}%"),
            color_discrete_map={
                "Statistical Outlier (IQR / Z-score)": PALETTE["red"],
                "Domain Quality Flag": PALETTE["amber"],
            },
        )
        fig_out.update_traces(textposition="outside", cliponaxis=False)
        fig_out.update_xaxes(range=[0, max(20, out_df["value"].max() * 1.18)])
        fig_out = style_figure(
            fig_out,
            title="Outlier Rates & Domain Quality Flag Rates per Field",
            xaxis_title="Affected Records (%)",
            yaxis_title="Topic · Flag Field",
            source_note=source_note,
        )
    else:
        fig_out = _empty_figure("Outlier Rate Chart", "No outlier metrics for selected filter.", source_note)

    # --- 5. Ingestion Compliance & Freshness Chart ---
    sla_df = sub[sub["kpi"].isin(["ingestion_compliance", "schema_compliance_rate"])].copy()
    if not sla_df.empty:
        sla_df["kpi_label"] = sla_df["kpi"].map(
            {
                "ingestion_compliance": "Ingestion Schedule Compliance (%)",
                "schema_compliance_rate": "Schema & Bounds Compliance (%)",
            }
        )
        fig_sla = px.bar(
            sla_df,
            x="topic",
            y="value",
            color="kpi_label",
            barmode="group",
            text=sla_df["value"].map(lambda v: f"{v:.1f}%"),
            color_discrete_map={
                "Ingestion Schedule Compliance (%)": PALETTE["blue"],
                "Schema & Bounds Compliance (%)": PALETTE["teal"],
            },
        )
        fig_sla.add_hline(
            y=90,
            line_dash="dash",
            line_color=PALETTE["red"],
            annotation_text="90% SLA Target",
            annotation_position="top right",
        )
        fig_sla.update_traces(textposition="outside", cliponaxis=False)
        fig_sla.update_yaxes(range=[0, 115])
        fig_sla = style_figure(
            fig_sla,
            title="Pipeline Ingestion & Schema Compliance by Source",
            xaxis_title="Data Source Topic",
            yaxis_title="Compliance Rate (%)",
            source_note=source_note,
        )
    else:
        fig_sla = _empty_figure("Compliance Chart", "No compliance rows available.", source_note)

    # --- 6. Data Quality Summary Table ---
    table_df = sub[sub["kpi"] != "volume_rows_per_day"].copy()
    evals = [_evaluate_status(r.kpi, r.value) for r in table_df.itertuples()]
    table_df["Threshold"] = [e[0] for e in evals]
    table_df["Status"] = [e[1] for e in evals]
    table_df["Field / Period"] = table_df["field"].fillna(table_df["period"]).fillna("—")
    table_df["Current Value"] = table_df.apply(
        lambda r: f"{r['value']:,.2f} {r['unit']}" if pd.notna(r["value"]) else "—", axis=1
    )
    table_df["Detail"] = table_df["detail"].fillna("—")

    display_cols = ["kpi", "topic", "Field / Period", "Current Value", "Threshold", "Status", "Detail"]
    rename_map = {"kpi": "Governance KPI", "topic": "Topic"}
    table_clean = table_df[display_cols].rename(columns=rename_map)

    summary_table = dash_table.DataTable(
        data=table_clean.to_dict("records"),
        columns=[{"name": c, "id": c} for c in table_clean.columns],
        page_size=12,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "auto"},
        style_header={
            "backgroundColor": PALETTE["primary"],
            "color": "white",
            "fontWeight": "bold",
            "fontSize": "12px",
        },
        style_cell={
            "textAlign": "left",
            "padding": "8px 10px",
            "fontSize": "12px",
            "fontFamily": "Inter, Segoe UI, sans-serif",
            "whiteSpace": "normal",
            "height": "auto",
        },
        style_data_conditional=[
            {"if": {"filter_query": '{Status} = "PASS"'}, "backgroundColor": "#E8F6F1", "color": "#0B5345"},
            {"if": {"filter_query": '{Status} = "WARN"'}, "backgroundColor": "#FEF5E7", "color": "#7E5109"},
            {"if": {"filter_query": '{Status} = "ALERT"'}, "backgroundColor": "#FDEDEC", "color": "#78281F"},
        ],
    )

    return header, cards, fig_null, fig_volume_fix(fig_vol), fig_out, fig_sla, summary_table


def fig_volume_fix(fig: go.Figure) -> go.Figure:
    return fig


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)
