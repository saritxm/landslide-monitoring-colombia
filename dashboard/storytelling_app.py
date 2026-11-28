"""Storytelling Dashboard (localhost:8051) — Workshop #4.

Audience: Functional User — UNGRD Emergency Management Analyst (User Stories US-01 to US-04).
Data Source: Exclusively `datalake_gold/storytelling_*.parquet` via `pd.read_parquet()`.
"""

from __future__ import annotations

import dash
from dash import Input, Output, dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils import (
    PALETTE,
    RISK_COLORS,
    build_footer,
    build_header,
    load_latest_gold,
    style_figure,
)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Landslide Risk Storytelling · Colombia UNGRD",
)


def _extract_station_table(df: pd.DataFrame) -> pd.DataFrame:
    """Pivots `station_risk` rows into one wide row per Andean monitoring point."""
    sr = df[df["section"] == "station_risk"].copy()
    if sr.empty:
        return pd.DataFrame()
    piv = sr.pivot_table(
        index=["entity", "department", "latitude", "longitude", "period"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()
    levels = (
        sr[sr["metric"] == "score"][["entity", "text"]]
        .drop_duplicates("entity")
        .rename(columns={"text": "risk_level"})
    )
    piv = piv.merge(levels, on="entity", how="left")
    return piv.sort_values("score", ascending=False)


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
    df, fname, updated = load_latest_gold("storytelling")
    st_df = _extract_station_table(df)

    departments = sorted(st_df["department"].dropna().unique().tolist()) if not st_df.empty else []
    stations = st_df["entity"].tolist() if not st_df.empty else []

    return dbc.Container(
        [
            html.Div(
                id="story-header-container",
                children=build_header(
                    dashboard_title="Andean Landslide Risk Monitor — Colombia",
                    topic_angle="Operational Decision Support for UNGRD Analysts: Current Rainfall vs. 10-Year Climatology & Historical Landslides",
                    audience_badge="FUNCTIONAL USER · UNGRD ANALYST · PORT 8051",
                    last_updated=updated,
                    source_file=fname,
                    refresh_button_id="story-refresh-btn",
                ),
            ),
            # Narrative Summary Card (Key Insight for Functional User)
            dbc.Card(
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    dbc.Badge(
                                        "KEY OPERATIONAL INSIGHT (US-01)",
                                        color="warning",
                                        text_color="dark",
                                        className="mb-2 fw-bold",
                                    ),
                                    html.H5(
                                        "Current Situation Summary",
                                        className="fw-bold mb-1",
                                        style={"color": PALETTE["primary"]},
                                    ),
                                    html.P(
                                        id="story-narrative-text",
                                        className="mb-0 fs-6",
                                        style={"color": "#1E293B"},
                                    ),
                                ],
                                md=12,
                            )
                        ]
                    )
                ),
                style={
                    "borderLeft": f"6px solid {PALETTE['amber']}",
                    "backgroundColor": "#FFFDF7",
                },
                className="shadow-sm mb-3 rounded-3",
            ),
            # Interactive Controls
            dbc.Card(
                dbc.CardBody(
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.Label("Department:", className="fw-semibold small mb-1"),
                                    dcc.Dropdown(
                                        id="story-dept-filter",
                                        options=[{"label": "All Departments", "value": "ALL"}]
                                        + [{"label": d, "value": d} for d in departments],
                                        value="ALL",
                                        clearable=False,
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label("Alert Level:", className="fw-semibold small mb-1"),
                                    dcc.Dropdown(
                                        id="story-risk-filter",
                                        options=[
                                            {"label": "All Alert Levels", "value": "ALL"},
                                            {"label": "LOW (Green · < 0.40)", "value": "LOW"},
                                            {"label": "MEDIUM (Amber · 0.40–0.69)", "value": "MEDIUM"},
                                            {"label": "HIGH (Red · >= 0.70)", "value": "HIGH"},
                                        ],
                                        value="ALL",
                                        clearable=False,
                                    ),
                                ],
                                md=4,
                            ),
                            dbc.Col(
                                [
                                    html.Label(
                                        "Focus Station (Rainfall Timeline):",
                                        className="fw-semibold small mb-1",
                                    ),
                                    dcc.Dropdown(
                                        id="story-station-filter",
                                        options=[{"label": s, "value": s} for s in stations],
                                        value=stations[0] if stations else None,
                                        clearable=False,
                                    ),
                                ],
                                md=4,
                            ),
                        ]
                    )
                ),
                className="shadow-sm mb-3 border-0",
            ),
            # Row 1: Risk Level Donut Chart + Interactive Spatial Map
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-risk-donut", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=4,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-risk-map", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=8,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            # Row 2: Hourly Rainfall Trend + Daily Rainfall vs Historical P90
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-hourly-trend", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-daily-history", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            # Row 3: Multi-Source Comparison (API vs Scraping vs Catalog) + Top Keywords by Severity
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-source-comparison", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-keywords-bar", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=6,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            # Row 4: Historical Landslide Volume Activity Over Time + Trigger Breakdown
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-volume-activity", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=7,
                        md=12,
                        className="mb-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(dcc.Graph(id="story-trigger-chart", config={"displayModeBar": True})),
                            className="shadow-sm border-0 h-100",
                        ),
                        lg=5,
                        md=12,
                        className="mb-3",
                    ),
                ]
            ),
            build_footer(),
        ],
        fluid=True,
        style={"backgroundColor": PALETTE["bg"], "minHeight": "100vh", "padding": "18px 24px"},
    )


app.layout = _build_layout


@app.callback(
    Output("story-header-container", "children"),
    Output("story-narrative-text", "children"),
    Output("story-risk-donut", "figure"),
    Output("story-risk-map", "figure"),
    Output("story-hourly-trend", "figure"),
    Output("story-daily-history", "figure"),
    Output("story-source-comparison", "figure"),
    Output("story-keywords-bar", "figure"),
    Output("story-volume-activity", "figure"),
    Output("story-trigger-chart", "figure"),
    Input("story-dept-filter", "value"),
    Input("story-risk-filter", "value"),
    Input("story-station-filter", "value"),
    Input("story-refresh-btn", "n_clicks"),
)
def update_storytelling(dept: str, risk_lvl: str, focus_station: str, _n_clicks: int):
    df, fname, updated = load_latest_gold("storytelling")
    source_note = f"Source: Gold Layer Parquet ({fname}) · Open-Meteo, NASA LHASA & NASA GLC"

    header = build_header(
        dashboard_title="Andean Landslide Risk Monitor — Colombia",
        topic_angle="Operational Decision Support for UNGRD Analysts: Current Rainfall vs. 10-Year Climatology & Historical Landslides",
        audience_badge="FUNCTIONAL USER · UNGRD ANALYST · PORT 8051",
        last_updated=updated,
        source_file=fname,
        refresh_button_id="story-refresh-btn",
    )

    st_df = _extract_station_table(df)
    if dept != "ALL" and not st_df.empty:
        st_df = st_df[st_df["department"] == dept]
    if risk_lvl != "ALL" and not st_df.empty:
        st_df = st_df[st_df["risk_level"] == risk_lvl]

    active_stations = st_df["entity"].tolist() if not st_df.empty else []
    if focus_station not in active_stations and active_stations:
        focus_station = active_stations[0]

    # --- 1. Narrative Summary Card ---
    narr_rows = df[df["section"] == "narrative"]
    base_insight = narr_rows["text"].iloc[0] if not narr_rows.empty else "No narrative insight available."
    if not st_df.empty:
        top_row = st_df.iloc[0]
        narrative = (
            f"{base_insight} Under current view ({len(st_df)} monitoring point(s)), "
            f"highest alert is {top_row['entity']} ({top_row['department']}) with Landslide Risk Index = "
            f"{top_row['score']:.2f} ({top_row['risk_level']}), {top_row['p24h_mm']:.1f} mm expected in 24h, "
            f"and {int(top_row['events_30km'])} documented historical landslides within 30 km."
        )
    else:
        narrative = f"{base_insight} (No monitoring points match the active department/risk filter.)"

    # --- 2. Risk Level Distribution Donut Chart (Green=LOW, Amber=MEDIUM, Red=HIGH) ---
    if not st_df.empty:
        dist = (
            st_df["risk_level"]
            .value_counts()
            .reindex(["LOW", "MEDIUM", "HIGH"], fill_value=0)
            .reset_index()
        )
        dist.columns = ["Risk Level", "Stations"]
        fig_donut = px.pie(
            dist,
            names="Risk Level",
            values="Stations",
            hole=0.52,
            color="Risk Level",
            color_discrete_map=RISK_COLORS,
        )
        fig_donut.update_traces(textinfo="label+value+percent", textposition="outside")
        fig_donut = style_figure(
            fig_donut,
            title="Alert Level Distribution Across Monitoring Points (US-01)",
            source_note=source_note,
        )
    else:
        fig_donut = _empty_figure("Alert Level Distribution", "No stations in current filter.", source_note)

    # --- 3. Interactive Map of Monitoring Points + Historical Landslides within 30 km ---
    if not st_df.empty:
        fig_map = px.scatter_mapbox(
            st_df,
            lat="latitude",
            lon="longitude",
            color="risk_level",
            size="score",
            size_max=22,
            hover_name="entity",
            hover_data={
                "department": True,
                "score": ":.2f",
                "p24h_mm": ":.1f",
                "p_ant_mm": ":.1f",
                "h_lhasa": ":.2f",
                "events_30km": True,
                "latitude": ":.3f",
                "longitude": ":.3f",
            },
            color_discrete_map=RISK_COLORS,
            zoom=5.2,
            center={"lat": 5.5, "lon": -74.5},
        )
        near = df[
            (df["section"] == "landslides_near_station")
            & (df["entity"].isin(active_stations))
        ].dropna(subset=["latitude", "longitude"])
        if not near.empty:
            fig_map.add_trace(
                go.Scattermapbox(
                    lat=near["latitude"],
                    lon=near["longitude"],
                    mode="markers",
                    marker=dict(size=6, color="#2C3E50", opacity=0.55),
                    name="Historical Landslides (<=30 km)",
                    text=near["period"] + " · " + near["text"].fillna(""),
                    hoverinfo="text",
                )
            )
        fig_map.update_layout(mapbox_style="open-street-map")
        fig_map = style_figure(
            fig_map,
            title="Spatial Risk Map: Andean Monitoring Points & Nearby Landslides (US-01 / US-04)",
            source_note=source_note,
        )
    else:
        fig_map = _empty_figure("Spatial Risk Map", "No coordinates to display.", source_note)

    # --- 4. Hourly Rainfall Trend Line Chart (Observed vs Forecast) ---
    hr_df = df[(df["section"] == "hourly_rainfall") & (df["entity"] == focus_station)].copy()
    hr_df = hr_df.sort_values("period")
    if not hr_df.empty:
        hr_df["Series"] = hr_df["text"].str.capitalize()
        fig_hr = px.line(
            hr_df,
            x="period",
            y="value",
            color="Series",
            markers=True,
            color_discrete_map={"Observed": PALETTE["blue"], "Forecast": PALETTE["red"]},
        )
        fig_hr = style_figure(
            fig_hr,
            title=f"Hourly Rainfall Trend (Observed vs. Forecast) — {focus_station} (US-02)",
            xaxis_title="Timestamp (UTC)",
            yaxis_title="Precipitation (mm/h)",
            source_note=source_note,
        )
    else:
        fig_hr = _empty_figure("Hourly Rainfall Trend", "No hourly data for selected station.", source_note)

    # --- 5. Daily Rainfall vs Historical 90th Percentile ---
    daily_df = df[(df["section"] == "daily_rain_vs_history") & (df["entity"] == focus_station)].copy()
    daily_df = daily_df.sort_values("period")
    if not daily_df.empty:
        p90_txt = daily_df["text"].iloc[0] if "text" in daily_df.columns else ""
        p90_val = None
        if ":" in str(p90_txt):
            try:
                p90_val = float(str(p90_txt).split(":")[1].replace("mm", "").strip())
            except ValueError:
                p90_val = None

        fig_daily = px.bar(
            daily_df,
            x="period",
            y="value",
            text=daily_df["value"].map(lambda v: f"{v:.1f} mm"),
            color_discrete_sequence=[PALETTE["sky"]],
        )
        fig_daily.update_traces(textposition="outside", cliponaxis=False, name="Daily Total (mm)")
        if p90_val is not None:
            fig_daily.add_hline(
                y=p90_val,
                line_dash="dash",
                line_color=PALETTE["red"],
                annotation_text=f"Historical Monthly P90 ({p90_val:.1f} mm/day)",
                annotation_position="top left",
            )
        fig_daily = style_figure(
            fig_daily,
            title=f"Daily Rainfall vs. 10-Year Historical P90 — {focus_station} (US-03)",
            xaxis_title="Date (UTC)",
            yaxis_title="Daily Precipitation (mm/day)",
            source_note=source_note,
            show_legend=False,
        )
    else:
        fig_daily = _empty_figure("Daily Rainfall vs. History", "No daily comparison data.", source_note)

    # --- 6. Source Comparison Chart (API Rainfall Ratio vs Scraping LHASA Hazard) ---
    if not st_df.empty:
        comp = st_df[["entity", "ratio_vs_p90", "h_lhasa"]].copy()
        comp["Open-Meteo API (5d Rain / Hist P90)"] = comp["ratio_vs_p90"].fillna(0.0)
        comp["NASA LHASA Scraping (Hazard Index 0-1)"] = comp["h_lhasa"].fillna(0.0)
        comp_long = comp.melt(
            id_vars="entity",
            value_vars=[
                "Open-Meteo API (5d Rain / Hist P90)",
                "NASA LHASA Scraping (Hazard Index 0-1)",
            ],
            var_name="Data Source Signal",
            value_name="Normalized Signal",
        )
        fig_src = px.bar(
            comp_long,
            x="entity",
            y="Normalized Signal",
            color="Data Source Signal",
            barmode="group",
            color_discrete_map={
                "Open-Meteo API (5d Rain / Hist P90)": PALETTE["blue"],
                "NASA LHASA Scraping (Hazard Index 0-1)": PALETTE["amber"],
            },
        )
        fig_src.update_xaxes(tickangle=-30)
        fig_src = style_figure(
            fig_src,
            title="Source Comparison: Open-Meteo API Rainfall Anomaly vs. NASA LHASA Scraping",
            xaxis_title="Andean Monitoring Point",
            yaxis_title="Normalized Hazard Signal (ratio / index)",
            source_note=source_note,
        )
    else:
        fig_src = _empty_figure("Source Comparison", "No stations in active filter.", source_note)

    # --- 7. Top Keywords Horizontal Bar Chart Colored by Associated Severity ---
    kw_df = df[df["section"] == "top_keywords"].copy()
    if not kw_df.empty:
        kw_piv = kw_df.pivot_table(index="entity", columns="metric", values="value", aggfunc="first").reset_index()
        kw_piv = kw_piv.sort_values("freq", ascending=False).head(12).sort_values("freq", ascending=True)
        kw_piv["avg_fatalities"] = kw_piv["avg_fatalities"].fillna(0.0)
        fig_kw = px.bar(
            kw_piv,
            x="freq",
            y="entity",
            orientation="h",
            color="avg_fatalities",
            text=kw_piv["freq"].map(lambda v: f"{int(v)}"),
            color_continuous_scale=[PALETTE["teal"], PALETTE["amber"], PALETTE["red"]],
            labels={"avg_fatalities": "Mean Fatalities / Event"},
        )
        fig_kw.update_traces(textposition="outside", cliponaxis=False)
        fig_kw = style_figure(
            fig_kw,
            title="Top Landslide Narrative Keywords Colored by Mean Fatalities (US-04)",
            xaxis_title="Mention Frequency in Narratives (count)",
            yaxis_title="Keyword",
            source_note=source_note,
            show_legend=False,
        )
    else:
        fig_kw = _empty_figure("Top Keywords", "No keyword aggregations found.", source_note)

    # --- 8. Volume Activity Chart (Historical Landslides per Year) ---
    yr_df = df[(df["section"] == "landslides_by_year") & (df["metric"] == "events")].copy()
    yr_df = yr_df.dropna(subset=["entity"]).sort_values("entity")
    if not yr_df.empty:
        fig_yr = px.bar(
            yr_df,
            x="entity",
            y="value",
            text=yr_df["value"].map(lambda v: f"{int(v)}"),
            color_discrete_sequence=[PALETTE["primary"]],
        )
        fig_yr.update_traces(textposition="outside", cliponaxis=False)
        fig_yr = style_figure(
            fig_yr,
            title="Historical Landslide Activity Over Time in Colombia (2007–2017 · US-04)",
            xaxis_title="Year",
            yaxis_title="Recorded Landslide Events (count)",
            source_note=source_note,
            show_legend=False,
        )
    else:
        fig_yr = _empty_figure("Historical Landslide Activity", "No yearly data available.", source_note)

    # --- 9. Landslides by Trigger ---
    tr_df = df[(df["section"] == "landslides_by_trigger") & (df["metric"] == "events")].copy()
    tr_df["entity"] = tr_df["entity"].fillna("unknown")
    tr_df = tr_df.sort_values("value", ascending=True)
    if not tr_df.empty:
        fig_tr = px.bar(
            tr_df,
            x="value",
            y="entity",
            orientation="h",
            text=tr_df["value"].map(lambda v: f"{int(v)}"),
            color_discrete_sequence=[PALETTE["red"]],
        )
        fig_tr.update_traces(textposition="outside", cliponaxis=False)
        fig_tr = style_figure(
            fig_tr,
            title="Historical Landslides by Reported Trigger (US-04)",
            xaxis_title="Recorded Events (count)",
            yaxis_title="Trigger Category",
            source_note=source_note,
            show_legend=False,
        )
    else:
        fig_tr = _empty_figure("Landslides by Trigger", "No trigger breakdown available.", source_note)

    return (
        header,
        narrative,
        fig_donut,
        fig_map,
        fig_hr,
        fig_daily,
        fig_src,
        fig_kw,
        fig_yr,
        fig_tr,
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8051)
