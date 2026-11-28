"""Shared utilities for Workshop #4 Plotly Dash applications.

Reads exclusively from Gold-layer Parquet files (`datalake_gold/governance_*.parquet`
and `datalake_gold/storytelling_*.parquet`) using `pd.read_parquet()`.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

import dash_bootstrap_components as dbc
from dash import html
import pandas as pd
import plotly.graph_objects as go

# --------------------------------------------------------------------------- paths
REPO_ROOT = Path(__file__).resolve().parents[1]
DATALAKE_ROOT = Path(os.environ.get("DATALAKE_ROOT", str(REPO_ROOT)))
GOLD_DIR = DATALAKE_ROOT / "datalake_gold"
if not GOLD_DIR.exists():
    GOLD_DIR = REPO_ROOT / "datalake_gold"

# --------------------------------------------------------------------------- colorblind-safe palette (Okabe-Ito inspired)
PALETTE = {
    "primary": "#1F3A5F",      # Deep Andean Navy
    "blue": "#0072B2",         # Okabe-Ito Blue
    "sky": "#56B4E9",          # Okabe-Ito Sky Blue
    "teal": "#009E73",         # Okabe-Ito Bluish Green (Low Risk / Positive)
    "amber": "#E69F00",        # Okabe-Ito Orange/Amber (Medium Risk / Neutral)
    "red": "#D55E00",          # Okabe-Ito Vermilion (High Risk / Negative)
    "purple": "#CC79A7",       # Okabe-Ito Reddish Purple
    "slate": "#5C6B73",        # Neutral Slate
    "bg": "#F4F6F9",           # Clean light canvas
    "card": "#FFFFFF",
    "border": "#DCE2EA",
}

RISK_COLORS = {
    "LOW": PALETTE["teal"],
    "MEDIUM": PALETTE["amber"],
    "HIGH": PALETTE["red"],
}

TOPIC_COLORS = {
    "rainfall": PALETTE["blue"],
    "rainfall_historical": PALETTE["sky"],
    "landslides": PALETTE["red"],
    "lhasa_hazard": PALETTE["purple"],
}

TOPIC_LABELS = {
    "rainfall": "Open-Meteo Forecast (API)",
    "rainfall_historical": "Open-Meteo ERA5 History (API)",
    "landslides": "NASA Global Landslide Catalog (CSV)",
    "lhasa_hazard": "NASA LHASA Nowcast (Scraping)",
}


# --------------------------------------------------------------------------- data loading
def load_latest_gold(prefix: str) -> Tuple[pd.DataFrame, str, str]:
    """Loads the newest `<prefix>_*.parquet` file from `datalake_gold/`.

    Returns:
        (dataframe, filename, formatted_computed_at_utc)
    """
    files = sorted(GOLD_DIR.glob(f"{prefix}_*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No '{prefix}_*.parquet' file found in {GOLD_DIR}. "
            "Run `gold_processing_dag` in Airflow or `python -m src.gold.transformations` first."
        )
    latest = files[-1]
    df = pd.read_parquet(latest)
    if "computed_at_utc" in df.columns and not df["computed_at_utc"].dropna().empty:
        ts = pd.to_datetime(df["computed_at_utc"].dropna().iloc[0])
        updated_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        updated_str = latest.stem.replace(f"{prefix}_", "") + " UTC"
    return df, latest.name, updated_str


# --------------------------------------------------------------------------- layout components
def build_header(
    dashboard_title: str,
    topic_angle: str,
    audience_badge: str,
    last_updated: str,
    source_file: str,
    refresh_button_id: str,
) -> dbc.Card:
    """Standardized header required across both dashboards."""
    return dbc.Card(
        dbc.CardBody(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    dbc.Badge(
                                        audience_badge,
                                        color="info",
                                        className="me-2 text-dark fw-semibold",
                                    ),
                                    dbc.Badge(
                                        "Team 7 · Universidad Distrital Francisco José de Caldas",
                                        color="light",
                                        text_color="dark",
                                    ),
                                ],
                                className="mb-2",
                            ),
                            html.H3(
                                dashboard_title,
                                className="mb-1 fw-bold text-white",
                            ),
                            html.P(
                                topic_angle,
                                className="mb-0 text-white-50 small",
                            ),
                        ],
                        lg=8,
                        md=12,
                    ),
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Div(
                                        [
                                            html.Span("Last Gold Update: ", className="text-white-50 small"),
                                            html.Span(last_updated, className="text-white fw-semibold small"),
                                        ]
                                    ),
                                    html.Div(
                                        [
                                            html.Span("Gold Source: ", className="text-white-50 small"),
                                            html.Code(source_file, className="text-warning small"),
                                        ],
                                        className="mb-2",
                                    ),
                                    dbc.Button(
                                        "Refresh Gold Data",
                                        id=refresh_button_id,
                                        color="light",
                                        size="sm",
                                        className="fw-semibold",
                                    ),
                                ],
                                className="text-lg-end mt-3 mt-lg-0",
                            )
                        ],
                        lg=4,
                        md=12,
                    ),
                ],
                align="center",
            )
        ),
        style={"backgroundColor": PALETTE["primary"], "border": "none"},
        className="shadow-sm mb-3 rounded-3",
    )


def build_footer() -> html.Footer:
    """Attribution footer aligned with Workshop #1 source contracts."""
    return html.Footer(
        html.Div(
            [
                html.Span(
                    "Data Sources (Gold Layer Exclusively): NASA Global Landslide Catalog (GLC), "
                    "NASA LHASA NRT Hazard Rasters, and Weather Data by Open-Meteo.com (CC BY 4.0). ",
                    className="text-muted small",
                ),
                html.Span(
                    "Data Analysis Programming · Semester 2026-I · Team 7.",
                    className="text-muted small fw-semibold",
                ),
            ],
            className="text-center py-3 border-top mt-4",
        )
    )


# --------------------------------------------------------------------------- chart styling
def style_figure(
    fig: go.Figure,
    title: str,
    xaxis_title: str = "",
    yaxis_title: str = "",
    source_note: str = "Source: Gold Layer Parquet",
    height: int = 390,
    show_legend: bool = True,
) -> go.Figure:
    """Applies consistent typography, margins, axis labels, and source annotation."""
    fig.update_layout(
        title=dict(
            text=f"<b>{title}</b>",
            font=dict(size=13.5, color=PALETTE["primary"]),
            x=0.01,
            y=0.97,
            xanchor="left",
            yanchor="top",
        ),
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        height=height,
        margin=dict(l=55, r=25, t=82, b=75),
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor="#FAFBFD",
        font=dict(family="Inter, Segoe UI, sans-serif", size=11, color="#222222"),
        showlegend=show_legend,
        legend=dict(
            title_text="",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
            bgcolor="rgba(255,255,255,0.85)",
        ),
        hovermode="closest",
    )
    fig.update_xaxes(showgrid=True, gridcolor="#E6ECF2", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E6ECF2", zeroline=False)
    fig.add_annotation(
        text=source_note,
        xref="paper",
        yref="paper",
        x=0.0,
        y=-0.22,
        xanchor="left",
        yanchor="top",
        showarrow=False,
        font=dict(size=9.5, color=PALETTE["slate"]),
    )
    return fig
