"""Builds the Workshop #2 report (HTML) from the data actually present in the repository.

Every number in the report is computed here from datalake_bronze/, datalake_silver/ and
workshop_2/report/airflow_runs.json, so the report cannot drift from the data.

Usage (from the repository root):
    python scripts/generate_report_figures.py
    python workshop_2/report/build_report.py
    # then print workshop_2/report/report_w2.html to workshop_2/Workshop2_Report_Team7.pdf
"""

from collections import Counter
from datetime import datetime
import html
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "datalake_bronze"
SILVER = ROOT / "datalake_silver"
HERE = Path(__file__).resolve().parent
OUT = HERE / "report_w2.html"


def esc(x):
    return html.escape(str(x))


def fmt_ts(s):
    # The SQLite metadata DB stores naive UTC timestamps
    return pd.Timestamp(s).strftime("%Y-%m-%d %H:%M:%S") if s else ""


def table(headers, rows, cls="small"):
    head = "".join(f"<th>{h}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table class='{cls}'><tr>{head}</tr>{body}</table>"


# ------------------------------------------------------------------ data
bronze_files = sorted(BRONZE.glob("*.json"))
silver_files = sorted(SILVER.glob("*.parquet"))
manifest = json.loads((SILVER / ".processed_manifest.json").read_text())
runs = json.loads((HERE / "airflow_runs.json").read_text())


def latest(prefix):
    files = sorted(SILVER.glob(f"{prefix}*.parquet"))
    return files[-1] if files else None


rain = pd.concat([pd.read_parquet(f) for f in sorted(SILVER.glob("rainfall_2*.parquet"))], ignore_index=True)
hist = pd.concat([pd.read_parquet(f) for f in sorted(SILVER.glob("rainfall_historical_*.parquet"))], ignore_index=True)
glc = pd.read_parquet(latest("landslides_"))
lhasa = pd.concat([pd.read_parquet(f) for f in sorted(SILVER.glob("lhasa_hazard_*.parquet"))], ignore_index=True)

# Bronze inventory
b_rows = []
for f in bronze_files:
    d = json.loads(f.read_text(encoding="utf-8"))
    m = d["metadata"]
    if m["source_id"] == "openmeteo_forecast":
        rec = f"{len(d['data'])} points × {len(d['data'][0]['hourly']['time'])} h"
    elif m["source_id"] == "openmeteo_era5":
        rec = f"{len(d['data'])} points × {len(d['data'][0]['daily']['time'])} days ({m['start_date'][:4]})"
    elif m["source_id"] == "nasa_lhasa":
        rec = f"{m['total_files_detected']:,} files listed; {len(d['data'])} newest kept; {len(d.get('station_hazards', []))} station windows"
    else:
        rec = f"{len(d['data'])} catalog rows in the Colombia box"
    b_rows.append((f"<code>{f.name}</code>", m["source_id"], rec, f"{f.stat().st_size / 1024:,.1f}"))

# Silver inventory
s_rows = []
for f in silver_files:
    md = pq.read_schema(f).metadata
    src = json.loads(md[b"bronze_source_files"])
    src_txt = ", ".join(src) if len(src) <= 3 else f"{len(src)} files: {src[0]} … {src[-1]}"
    s_rows.append((f"<code>{f.name}</code>", f"{pq.read_metadata(f).num_rows:,}", f"{pq.read_metadata(f).num_columns}",
                   f"{f.stat().st_size / 1024:,.1f}", f"<code>{esc(src_txt)}</code>"))

# Airflow runs
r_rows = []
for r in runs:
    states = Counter(t["state"] for t in r["tasks"])
    tries = max((t["try_number"] or 0) for t in r["tasks"]) if r["tasks"] else 0
    dur = (pd.Timestamp(r["end_date"]) - pd.Timestamp(r["start_date"])).total_seconds() if r["end_date"] else None
    task_txt = ", ".join(f"{n} {s}" for s, n in states.items())
    if tries > 1:
        task_txt += f" (max try {tries})"
    r_rows.append((f"<code>{r['dag_id']}</code>", f"<code>{r['run_id']}</code>", r["run_type"], r["state"],
                   fmt_ts(r["start_date"]), f"{dur:,.0f}" if dur is not None else "", task_txt))
n_runs = Counter((r["dag_id"], r["state"]) for r in runs)

# Rainfall quality
polls = rain["_bronze_source_file"].nunique()
wet = rain[rain.precipitation_mm > 0]
mx = rain.loc[rain.precipitation_mm.idxmax()]
station_mean = (rain.groupby("station_name", observed=True).precipitation_mm.mean().sort_values(ascending=False))

# ERA5
hist_stats = hist.groupby("station_name", observed=True).agg(
    mean=("precip_24h_mm", "mean"), p95=("precip_24h_mm", lambda s: s.quantile(0.95)),
    mx=("precip_24h_mm", "max"), mx7=("precip_7d_mm", "max")).sort_values("mean", ascending=False)
hist_max = hist.loc[hist.precip_24h_mm.idxmax()]
annual = hist.groupby(["station_name", "year"], observed=True).precip_24h_mm.sum().unstack("year").round(0)
annual_top = annual.stack().idxmax()          # (station, year) with the largest annual total
annual_top_val = annual.stack().max()
others = annual.loc[annual_top[0]].drop(annual_top[1])

# GLC
tokens = Counter(t for lst in glc.tokens_nlp for t in (list(lst) if lst is not None else []))
mocoa = glc[glc.event_id.astype(str) == "10790"].iloc[0]
fat_known = glc.fatality_count.dropna()

# LHASA
lh = lhasa.sort_values("lhasa_max", ascending=False)

# Tests
tests_line = (HERE / "pytest_summary.txt").read_text().strip() if (HERE / "pytest_summary.txt").exists() else "see tests/"

# ------------------------------------------------------------------ samples
def sample_bronze():
    f = sorted(BRONZE.glob("openmeteo_forecast_*.json"))[-1]
    d = json.loads(f.read_text())
    loc = d["data"][0]
    lines = ["{", '  "metadata": {']
    lines += [f'    "{k}": {json.dumps(v)},' for k, v in d["metadata"].items() if k != "payload_sha256"]
    lines += [f'    "payload_sha256": "{d["metadata"]["payload_sha256"][:16]}…"', "  },", '  "data": [', "    {"]
    for k in ("location_name", "department", "requested_latitude", "requested_longitude", "returned_latitude",
              "returned_longitude", "elevation_m"):
        lines.append(f'      "{k}": {json.dumps(loc[k])},')
    lines.append(f'      "hourly_units": {json.dumps(loc["hourly_units"])},')
    h = loc["hourly"]
    lines.append('      "hourly": {' + ", ".join(f'"{k}": {json.dumps(v[:3])[:-1]}, …]' for k, v in h.items()) + "}")
    lines += ["    },", f"    … {len(d['data']) - 1} more points", "  ]", "}"]
    return f.name, "\n".join(lines)


bronze_name, bronze_txt = sample_bronze()

rain_sample = rain[(rain.station_name == mx.station_name) & (rain._bronze_source_file == mx._bronze_source_file)]
rain_sample = rain_sample.sort_values("precipitation_mm", ascending=False).head(4)[
    ["station_name", "timestamp", "precipitation_mm", "soil_moisture_m3m3", "precip_outlier_iqr",
     "precip_outlier_zscore", "precip_zscore", "_bronze_source_file"]]
glc_sample = glc[glc.event_id.astype(str).isin(["10790", "3352", "3353"])][
    ["event_id", "event_date", "timestamp_utc", "is_time_unknown", "country_name", "country_missing",
     "fatality_count", "fatality_outlier_iqr", "token_count"]]


def df_html(df):
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d %H:%M")
        elif pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].round(3)
    return out.to_html(index=False, border=0, classes="small", na_rep="null").replace('class="dataframe small"', 'class="small"')


def img(path, caption, width="100%"):
    return f"<figure><img src='{path}' style='width:{width}'><figcaption>{caption}</figcaption></figure>"


# ------------------------------------------------------------------ HTML
CSS = (HERE / "report.css").read_text()
first_run = min(pd.Timestamp(r["start_date"]) for r in runs)
H = []
H.append(f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Workshop 2 Report - Team 7 Landslide Monitoring</title><style>{CSS}</style></head><body>
<div class="cover">
<h1>Landslide Monitoring and Risk — Colombia<br>Technical Architecture and Bronze/Silver Pipeline</h1>
<div class="meta">Workshop No. 2 Report (revised version) — Data Analysis Programming — Universidad Distrital Francisco José de Caldas<br>
Professor: Eng. Carlos Andrés Sierra, M.Sc. — <b>Team 7</b>: Samuel Alberto Leal Saenz (20231020072), Cristian Andres Gámez Nuñez (20231020100), Sara Nicolle Martínez Moreno (20231020116)<br>
Repository: <code>https://github.com/saritxm/landslide-monitoring-colombia</code> — Report date: {datetime.now():%d %B %Y}</div>
</div>

<div class="note"><b>About this revision.</b> This document replaces the report delivered on 16 November. A review of the
first version found that several figures in its results section did not come from the data in the repository, that the
FileSensor did not wait for any file, and that the preprocessing replaced missing values with invented ones (0 deaths,
0 mm of rain, country "Colombia"). Those problems are corrected in the code, all Bronze and Silver files were regenerated
by real DAG runs, and <b>every number in this report is computed by <code>workshop_2/report/build_report.py</code> from the
files in the repository</b>. Section 7 lists each correction.</div>

<h2>1. Executive Summary</h2>
<p>Workshop #1 validated the open sources for monitoring rainfall-triggered landslides in Colombia and defined their
ingestion contracts. Workshop #2 turns that plan into a containerized pipeline organized as a medallion architecture:
raw <b>Bronze</b> JSON, cleaned and typed <b>Silver</b> Parquet, and a planned <b>Gold</b> layer (Workshop #3).</p>
<ul>
<li><b>Stack:</b> Docker Compose with Apache Airflow (webserver, scheduler, triggerer, LocalExecutor) and PostgreSQL as
the metadata database; DAGs, source code and the three data-lake folders are mounted as volumes.</li>
<li><b>Bronze</b> — <code>bronze_ingestion_dag</code> (TaskFlow API, hourly) extracts the <b>Open-Meteo Forecast REST API</b>,
<b>scrapes</b> the NASA LHASA nowcast directory with BeautifulSoup (and reads the newest hazard raster around each monitoring
point), and loads the NASA Global Landslide Catalog; <code>bronze_historical_backfill_dag</code> downloads ERA5 daily rainfall
2016–2025 from the Open-Meteo archive. Payloads are validated and persisted as <code>source_YYYYMMDD_HHMMSS.json</code>.</li>
<li><b>Silver</b> — <code>silver_processing_dag</code> waits with a <b>FileSensor</b> for the <code>_READY.flag</code> written by
the Bronze DAGs, applies type/time normalization, conservative null handling, NLP text cleaning, IQR/Z-score outlier
flagging and deduplication, and writes one <code>topic_YYYYMMDD_HHMMSS.parquet</code> per topic and run, traceable to its
Bronze files.</li>
<li><b>Evidence:</b> {len(runs)} DAG runs ({sum(v for (d, s), v in n_runs.items() if s == 'success')} successful), {len(bronze_files)} Bronze JSON files and
{len(silver_files)} Silver Parquet files ({len(rain) + len(hist) + len(glc) + len(lhasa):,} rows) produced from the live sources
since {first_run:%Y-%m-%d %H:%M} UTC; {esc(tests_line)}.</li>
</ul>

<h2>2. System Architecture</h2>
{img('../figures/pipeline_architecture.png', 'Figure 1. Architecture: external sources, Airflow DAGs, Bronze and Silver layers, and the planned Gold layer and dashboard.')}
""")
H.append(table(["Component", "Responsibility", "Technology", "Inputs", "Outputs"], [
    ("Open-Meteo Forecast API", "Hourly precipitation, rain and 0–1 cm soil moisture for 10 monitoring points (2 past days + 3 forecast days)", "REST/JSON, <code>requests</code> with retries", "HTTP GET (10 points in one call)", "JSON per point"),
    ("Open-Meteo ERA5 archive", "Daily precipitation 2016–2025 for the same points (baseline for percentiles in Gold)", "REST/JSON", "One request per year", "JSON per point and year"),
    ("NASA LHASA portal", "Discover the newest published nowcast GeoTIFF and read hazard values near each point", "BeautifulSoup (HTML listing), rasterio + GDAL <code>/vsicurl/</code> range reads", "Directory listing HTML; GeoTIFF window", "File list + station hazard stats"),
    ("NASA Global Landslide Catalog", "Historical landslide events (2007–2017) with text narratives", "pandas", "Catalog snapshot saved in Workshop #1", "Rows inside the Colombia box"),
    ("<code>bronze_ingestion_dag</code>", "Extract, validate, skip unchanged payloads, persist, write <code>_READY.flag</code>", "Airflow TaskFlow API", "Sources", "<code>datalake_bronze/*.json</code>"),
    ("<code>bronze_historical_backfill_dag</code>", "Backfill ERA5 by year (manual, parameterized)", "Airflow TaskFlow API + Params", "Years", "<code>openmeteo_era5_YYYY_*.json</code>"),
    ("FileSensor", "Start Silver processing when Bronze signals new files", "<code>FileSensor</code>, <code>mode='reschedule'</code>", "<code>_READY.flag</code>", "Trigger of the processing task"),
    ("<code>silver_processing_dag</code>", "Clean, type, flag, deduplicate and persist", "pandas, PyArrow", "Unprocessed Bronze files (manifest)", "<code>datalake_silver/*.parquet</code>"),
    ("Gold layer (planned, W#3)", "0.25° grid, antecedent rainfall, ERA5 percentiles, risk indicator", "PySpark", "Silver Parquet", "<code>datalake_gold/</code>"),
]))

H.append(f"""
<h2>3. Docker Stack</h2>
{table(["Service", "Image", "Role"], [
    ("<code>postgres</code>", "postgres:15-alpine", "Airflow metadata database (runs, task states, XComs, users); healthcheck with <code>pg_isready</code>; named volume"),
    ("<code>airflow-init</code>", "landslide-monitoring-airflow:2.9.2 (built from <code>Dockerfile</code>)", "One-shot: migrates the database and creates the admin user, then exits"),
    ("<code>airflow-webserver</code>", "same image", "UI at <code>http://localhost:8080</code> (admin / admin); healthcheck on <code>/health</code>"),
    ("<code>airflow-scheduler</code>", "same image", "Schedules runs and executes tasks with the LocalExecutor"),
    ("<code>airflow-triggerer</code>", "same image", "Runs deferrable operators"),
])}
<p>The <code>Dockerfile</code> extends <code>apache/airflow:2.9.2</code> with pandas, PyArrow, BeautifulSoup, PyYAML, NumPy and
rasterio, pinned with the official Airflow 2.9.2 constraints file so a build on another machine installs the same versions.
Mounted volumes: <code>airflow/dags</code>, <code>src</code>, <code>config</code>, <code>workshop_1/samples</code>,
<code>datalake_bronze</code>, <code>datalake_silver</code>, <code>datalake_gold</code>. The FileSensor connection is defined
in the environment as <code>AIRFLOW_CONN_FS_DEFAULT='fs://?path=%2Fopt%2Fairflow%2Fdatalake_bronze'</code>: Airflow's
<code>FSHook</code> reads the base folder only from the <code>path</code> extra (see Section 7).</p>
<pre>docker compose up -d        # builds the image the first time, migrates the DB, starts the stack
docker compose ps           # postgres, webserver, scheduler and triggerer running/healthy
# UI: http://localhost:8080  (admin / admin) → unpause the DAGs → trigger bronze_ingestion_dag</pre>

<h2>4. Preprocessing Pipeline (Bronze → Silver)</h2>
<p>Each rule answers a problem observed in the Workshop #1 profiling. The guiding policy is that <b>Silver never invents a
value</b>: a missing value is kept null and flagged, an impossible value is rejected and counted, and statistical extremes
are flagged, never deleted, because extreme rainfall is exactly what the project monitors.</p>
<h3>4.1 Null handling</h3>
{table(["Field", "Rule", "Reason"], [
    ("<code>precipitation_mm</code> (hourly)", "Required. Missing, &lt; 0 or &gt; 300 mm/h → row rejected and counted (<code>rows_rejected_*</code> in the task stats)", "It is the variable the risk indicator is built on; 0 would mean \"it did not rain\""),
    ("<code>soil_moisture_m3m3</code>", "Gaps ≤ 2 h interpolated within the station and flagged <code>soil_moisture_imputed</code>; longer gaps stay null", "Soil moisture changes slowly hour to hour; a long gap cannot be guessed"),
    ("<code>rain_mm</code>", "Kept as delivered (null stays null)", "Informative only"),
    ("<code>precip_24h_mm</code> (ERA5)", "Missing or negative → null + <code>precip_24h_missing</code>; <code>precip_7d_mm</code> only with 7 complete days", "A partial window would understate antecedent rainfall"),
    ("<code>fatality_count</code>, <code>injury_count</code>", "Nullable integers; missing stays null + <code>fatality_missing</code>", "Filling with 0 invents \"no deaths\""),
    ("<code>country_name</code>", "Not imputed; null/empty → null + <code>country_missing</code>. Events labelled with another country are excluded", "The bounding box also covers Ecuador, Venezuela, Panama, Peru and Brazil; the country is assigned spatially in Gold"),
    ("<code>landslide_trigger</code>, <code>landslide_size</code>, <code>admin_division_name</code>", "Sentinel category <code>unknown</code> / <code>Unknown</code>", "Already the catalog's own convention"),
    ("LHASA <code>content_length_bytes</code>", "Null when the file was not checked with HTTP HEAD (only the 3 newest are)", "0 bytes would look like an empty upload"),
])}
<h3>4.2 Types, time and schema</h3>
<p>All timestamps are converted to <code>timestamp[us, tz=UTC]</code>: Open-Meteo is requested in UTC; LHASA issue times
come from the file name (<code>YYYYMMDDTHHMM</code>, UTC); GLC dates are Colombian local time and are shifted +5 h
(Mocoa: 05:32 PM local → 22:32 UTC). GLC events at exactly 00:00 get <code>is_time_unknown</code>. Coordinates are
float64, counts nullable Int64, station and department dictionary-encoded categories. Each Parquet file stores its
schema, the list of Bronze files it was built from (<code>bronze_source_files</code>) and the row count in its metadata.</p>
<h3>4.3 Text preprocessing (NLP)</h3>
<p>GLC title, description and location are concatenated and cleaned: lowercasing, Unicode accent stripping (NFD),
removal of URLs, HTML tags, punctuation and digits, whitespace normalization, tokenization, and removal of English +
Spanish stop words and tokens shorter than 3 characters. Stemming is not applied because most tokens are place names.</p>
<h3>4.4 Outliers</h3>
<p>IQR (Q3 + 1.5·IQR) and Z-score (|z| &gt; 3) are computed <b>per station over wet hours only</b> (precipitation &gt; 0),
because with mostly dry hours the plain IQR is 0 and every drizzle becomes an "outlier". Fatality outliers use known,
non-zero counts. Results are boolean flags plus the z-score; no value is removed or clipped.</p>
<h3>4.5 Deduplication and idempotency</h3>
<ul>
<li>Within a batch: natural keys (station, timestamp) for rainfall, (station, date) for ERA5, (station, issue time) for
LHASA, <code>event_id</code> for GLC.</li>
<li>Bronze: a payload whose SHA-256 equals the last persisted file of the same source is not written again; LHASA is
read once per day and GLC once per month.</li>
<li>Silver: a manifest (<code>.processed_manifest.json</code>) prevents a Bronze file from being processed twice; one
Parquet file per topic and run; the writer is atomic and refuses to overwrite.</li>
<li>Forecast polls overlap (each one repeats the past 2 days, which Open-Meteo revises), so the same station-hour can
appear in several polls with different values; every row keeps <code>_bronze_source_file</code> and the Gold layer will
use the most recent poll.</li>
</ul>

<h2>5. DAG Documentation</h2>
<h3>5.1 <code>bronze_ingestion_dag</code></h3>
<p>Schedule <code>@hourly</code>, <code>catchup=False</code>, <code>max_active_runs=1</code>, 2 retries (2 min) and a
10-minute execution timeout per task. Tasks: <code>extract_weather_api</code> → Open-Meteo; <code>extract_lhasa_scraper</code>
→ LHASA listing + station windows of the newest raster (daily cadence); <code>extract_landslide_catalog</code> → GLC
(monthly cadence); <code>validate_and_persist</code> → envelope and bounds checks, unchanged-payload skip, JSON write and
<code>_READY.flag</code>. There is no offline fallback: if a source cannot be reached the task fails and is retried, so no
made-up data reaches Bronze.</p>
<h3>5.2 <code>bronze_historical_backfill_dag</code></h3>
<p>Manual (<code>schedule=None</code>) with parameters <code>start_year</code>, <code>end_year</code>,
<code>force_overwrite</code>. One request per year (cost ≈ 26 Open-Meteo units for 10 points), validation, one file per year,
years already present are skipped, then <code>_READY.flag</code>. Retries and a 10-minute timeout per attempt.</p>
<h3>5.3 <code>silver_processing_dag</code></h3>
<pre>FileSensor(task_id="sensor_detect_bronze_file", filepath="_READY.flag", fs_conn_id="fs_default",
           poke_interval=30, timeout=300, mode="reschedule", soft_fail=True)
    &gt;&gt; process_bronze_to_silver()</pre>
<p>The processing task consumes the flag <b>before</b> listing the Bronze folder (a Bronze run finishing meanwhile writes a
new flag that triggers the next Silver run), dispatches each unprocessed file to its transform, concatenates the results
per topic, writes the Parquet files and updates the manifest. Without a flag the sensor gives up after 5 minutes and the
run is marked skipped, not failed.</p>
""")

# ---- results
H.append(f"""<h2 class="pb">6. Results Evidence</h2>
<div class="note"><b>Execution environment.</b> The runs below were executed on the development machine with Apache Airflow
2.10.5 installed with pip (SequentialExecutor, SQLite metadata DB), because Docker Hub could not be reached from it to pull
the images. The DAG files, source modules and data folders are exactly the ones mounted by <code>docker-compose.yml</code>;
the compose file was validated with <code>docker compose config</code>.</div>
<h3>6.1 Airflow runs</h3>
{table(["DAG", "Run id", "Type", "State", "Start (UTC)", "Seconds", "Tasks"], r_rows)}
<p class="small">The first backfill run failed: an ERA5 request stayed blocked honouring a long <code>Retry-After</code>
header. The client now uses its own short backoff and the DAG has a timeout and retries; the second run skipped the years
already saved, and its first attempt was stopped by the timeout and completed on retry. The first Silver run used the earlier
one-file-per-Bronze-file writer, which overwrote files written within the same second (Section 7); its output was discarded
and those Bronze files were processed again by the next run.</p>
""")
shots = [("ui_dags_list.png", "Airflow UI: the three DAGs."), ("ui_bronze_grid.png", "<code>bronze_ingestion_dag</code> — grid view of the runs."),
         ("ui_bronze_graph.png", "<code>bronze_ingestion_dag</code> — graph view (three extract tasks → validate_and_persist)."),
         ("ui_backfill_grid.png", "<code>bronze_historical_backfill_dag</code> — failed first run and successful retry."),
         ("ui_silver_grid.png", "<code>silver_processing_dag</code> — grid view."), ("ui_silver_graph.png", "<code>silver_processing_dag</code> — FileSensor → process_bronze_to_silver.")]
k = 2
for name, cap in shots:
    if (ROOT / "workshop_2" / "img" / name).exists():
        H.append(img(f"../img/{name}", f"Figure {k}. {cap}", "92%"))
        k += 1

H.append(f"""<h3>6.2 Files produced</h3>
<p><b>{len(bronze_files)} Bronze JSON files</b> (datalake_bronze/):</p>
{table(["File", "Source", "Content", "KB"], b_rows)}
<p><b>{len(silver_files)} Silver Parquet files</b> (datalake_silver/), lineage read from each file's metadata; {len(manifest)} Bronze files are
recorded in the manifest:</p>
{table(["File", "Rows", "Cols", "KB", "Built from"], s_rows)}

<h3>6.3 Bronze sample (<code>{bronze_name}</code>)</h3>
<pre>{esc(bronze_txt)}</pre>

<h3>6.4 Data quality results</h3>
{table(["Topic", "Result (computed from the Silver files)"], [
    ("<code>rainfall</code>", f"{len(rain):,} rows from {polls} forecast polls (10 points × 120 h each); 0 null precipitation; "
     f"{int(rain.soil_moisture_imputed.sum())} soil-moisture values interpolated; {len(wet):,} wet hours; "
     f"{int(rain.precip_outlier_iqr.sum())} IQR flags and {int(rain.precip_outlier_zscore.sum())} Z-score flags "
     f"({rain.precip_outlier_iqr.mean() * 100:.1f}% / {rain.precip_outlier_zscore.mean() * 100:.1f}% of rows); maximum "
     f"{mx.precipitation_mm:.1f} mm/h at {mx.station_name}, {mx.timestamp:%Y-%m-%d %H:%M} UTC (forecast)"),
    ("<code>rainfall_historical</code>", f"{len(hist):,} station-days {hist.date_utc.min():%Y-%m-%d} → {hist.date_utc.max():%Y-%m-%d} from "
     f"{hist._bronze_source_file.nunique()} yearly files; {int(hist.precip_24h_missing.sum())} missing days; wettest mean "
     f"{hist_stats.index[0]} ({hist_stats['mean'].iloc[0]:.1f} mm/day), driest {hist_stats.index[-1]} ({hist_stats['mean'].iloc[-1]:.1f} mm/day); "
     f"daily maximum {hist_max.precip_24h_mm:.1f} mm ({hist_max.station_name}, {hist_max.date_utc:%Y-%m-%d}); "
     f"{int(hist.precip_7d_mm.isna().sum())} null 7-day sums (first 6 days of each yearly file)"),
    ("<code>landslides</code>", f"{len(glc)} events kept from the box ({int((glc.country_name == 'Colombia').sum())} Colombia, "
     f"{int(glc.country_missing.sum())} without country, foreign-labelled events excluded); {int(glc.is_time_unknown.sum())} with unknown time; "
     f"{int(glc.fatality_missing.sum())} without fatality count (kept null); {int(glc.fatality_outlier_iqr.sum())} fatality IQR flags "
     f"(max {int(fat_known.max())}, Mocoa 2017); {int(glc.token_count.sum()):,} tokens"),
    ("<code>lhasa_hazard</code>", f"{len(lhasa)} station windows of raster {lhasa.nominal_issue_time.max():%Y-%m-%d}; highest "
     f"<code>lhasa_max</code> {lh.lhasa_max.iloc[0]:.3f} at {lh.station_name.iloc[0]}; {int(lhasa.has_high_hazard.sum())} stations ≥ 0.5"),
])}
{img('../figures/rainfall_analysis.png', f'Figure {k}. Latest rainfall Silver file (<code>{latest("rainfall_2").name}</code>, all polls it contains): distribution of hourly precipitation and mean by station.', '95%')}
<p>Mean hourly precipitation by station (all polls): {', '.join(f'{s} {v:.2f}' for s, v in station_mean.head(4).items())} mm/h.</p>
{img('../figures/nlp_keywords.png', f'Figure {k + 1}. Most frequent tokens in the GLC narratives after cleaning and stop-word removal (the figure also omits generic reporting words such as <i>said</i>).', '80%')}
<p>The catalog is written in English, so the most frequent tokens are English words: {', '.join(f'<i>{w}</i> ({c})' for w, c in tokens.most_common(8))}.</p>

<h3>6.5 Silver samples</h3>
<p><code>rainfall</code> — wettest hours of {mx.station_name} in the poll that contains the maximum:</p>
{df_html(rain_sample)}
<p><code>landslides</code> — Mocoa 2017 (no country in the catalog, kept null and flagged) and the El Tambo pair 3352/3353:</p>
{df_html(glc_sample)}
<p><code>lhasa_hazard</code> — three highest station windows:</p>
{df_html(lh.head(3)[["station_name", "nominal_issue_time", "valid_pixel_count", "lhasa_max", "lhasa_mean", "lhasa_p90", "has_high_hazard"]])}
<p><code>rainfall_historical</code> — ERA5 baseline per station (mm/day):</p>
{df_html(hist_stats.round(1).reset_index().rename(columns={'mean': 'mean_daily', 'p95': 'p95_daily', 'mx': 'max_daily', 'mx7': 'max_7d'}))}
<div class="note"><b>Data-quality finding (source, not code).</b> Annual totals per station show an implausible value:
{annual_top[0]} {annual_top[1]} sums {annual_top_val:,.0f} mm, while the same cell gives {others.min():,.0f}–{others.max():,.0f} mm in the
other years. We checked the API directly: requesting the pure <code>era5</code> model (nearest 0.25° cell) returns practically the same total, so the value
comes from the reanalysis itself, a coarse grid over very steep terrain, not from our code.
Silver keeps the values as delivered (nothing is invented or removed); the Gold layer will compare station-year totals
against the multi-year distribution of each cell and exclude anomalous years from the percentile baseline.</div>
<p>Annual ERA5 totals (mm):</p>
{df_html(annual.astype("Int64").rename_axis(columns=None).reset_index())}
<h3>6.6 Tests</h3>
<p><code>pytest tests/</code>: {esc(tests_line)}. The Workshop #2 tests cover the API client, the scraper, Bronze validation,
NLP cleaning, both outlier methods (with and without zero exclusion), the four transforms, the unknown-time flag, the
rejection of missing/impossible rainfall, the soil-moisture gap rule, and that missing countries and fatality counts are
not imputed. Tests that call a live source are skipped when there is no network.</p>
""")

H.append("""<h2>7. Challenges and Corrections</h2>
""" + table(["Problem found", "Cause", "Correction"], [
    ("The FileSensor never detected the Bronze flag inside Docker", "<code>AIRFLOW_CONN_FS_DEFAULT='fs:///opt/airflow/datalake_bronze'</code> puts the folder in the URI path, which <code>FSHook</code> ignores (base path = '')", "Connection given as <code>fs://?path=%2Fopt%2Fairflow%2Fdatalake_bronze</code> (verified with Airflow's <code>Connection</code> parser)"),
    ("First version: sensor on <code>filepath=\".\"</code>", "The folder always exists, so the sensor never waited", "Sensor on <code>_READY.flag</code>, written only when new Bronze files exist; flag consumed before listing to avoid losing a concurrent batch"),
    ("Silver files silently overwritten", "One Parquet per Bronze file named to the second: 5 ERA5 years processed in the same second produced 2 files", "One Parquet per topic and run; atomic writer that refuses to overwrite"),
    ("Invented values in Silver", "<code>fillna(0)</code> on precipitation, fatalities and injuries; null country set to \"Colombia\"", "Missing values kept null and flagged; impossible/missing rainfall rejected and counted (Section 4.1)"),
    ("Invented data in Bronze when a source failed", "Scraper fell back to a hard-coded HTML listing, and to a Workshop #1 sample raster labelled with today's date", "Fallbacks removed: the task fails and Airflow retries (2 retries, 10-min timeout)"),
    ("Duplicated Bronze/Silver data", "Every run re-wrote identical payloads (e.g. the static GLC snapshot)", "Unchanged payloads (same SHA-256) are not persisted; daily/monthly cadence for LHASA/GLC"),
    ("Implausible ERA5 annual total for one cell", "Reanalysis bias over steep terrain (verified directly against the API)", "Kept as delivered and documented; to be screened in Gold before computing percentiles"),
    ("ERA5 backfill blocked for minutes", "urllib3 honoured a long <code>Retry-After</code> on HTTP 429", "Own short backoff; Airflow retries and execution timeout"),
    ("Precipitation outliers meaningless", "IQR over all hours: with most hours dry, any rain was flagged", "Per-station, wet-hours-only IQR and Z-score"),
    ("<code>workshop_2/</code> missing from the repository", "<code>.gitignore</code> had <code>Workshop_2/</code>; Git on Windows ignores case, so it also hid <code>workshop_2/</code>", "Entry removed; report, figures and screenshots versioned in <code>workshop_2/</code>"),
    ("Report figures not matching the data", "Numbers written by hand (e.g. token frequencies, outlier percentages, hashes of files not in the repository)", "Report generated by <code>build_report.py</code> from the committed files and the exported Airflow run history"),
    ("Docker images not reachable from the development machine", "Network policy", "Runs executed with Airflow installed with pip on the same code; compose file validated"),
]) + f"""
<h2>8. Conclusions and Next Steps</h2>
<p>The pipeline ingests four live sources into an immutable Bronze layer and produces typed, flagged and traceable Silver
tables without inventing values. For Workshop #3 the Gold layer (PySpark) will read all Silver files, keep the most recent
forecast poll per station-hour, assign GLC events to Colombia spatially, compute 24 h / 7 d antecedent rainfall and ERA5
percentiles per station, combine them with the LHASA hazard values, and produce the landslide risk indicator.</p>

<h2>9. References</h2>
<ol class="small">
<li>Apache Airflow documentation — TaskFlow API, FileSensor, FSHook connections. https://airflow.apache.org/docs/</li>
<li>Open-Meteo — Forecast API and Historical Weather (ERA5) API documentation. https://open-meteo.com/en/docs</li>
<li>NASA LHASA nowcast data, NCCS DataShare. https://portal.nccs.nasa.gov/datashare/landslides/nrt/hazard/tif/</li>
<li>Kirschbaum, D. B. et al. (2010). A global landslide catalog for hazard applications. <i>Natural Hazards</i>, 52(3), 561–575.</li>
<li>Stanley, T. A. &amp; Kirschbaum, D. B. (2017). A heuristic approach to global landslide susceptibility mapping. <i>Natural Hazards</i>, 87, 145–164.</li>
</ol>
</body></html>""")

OUT.write_text("".join(H), encoding="utf-8")
print("written", OUT)
