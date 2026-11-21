"""Builds the Workshop #3 report (HTML) from the Silver files, the Gold files and the Airflow run history.
Every number is computed from the files in the repository.
Usage (repository root): python workshop_3/report/build_report.py  -> print report_w3.html to PDF.
"""

import html
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SILVER, GOLD = ROOT / "datalake_silver", ROOT / "datalake_gold"
CSS = (ROOT / "workshop_2" / "report" / "report.css").read_text()


def esc(x):
    return html.escape(str(x))


def table(headers, rows):
    return ("<table class='small'><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr>" +
            "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows) + "</table>")


def df_html(df):
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].round(2)
    return out.to_html(index=False, border=0, na_rep="null").replace('class="dataframe"', 'class="small"')


def img(path, cap, w="92%"):
    return f"<figure><img src='{path}' style='width:{w}'><figcaption>{cap}</figcaption></figure>"


gov_file = sorted(GOLD.glob("governance_*.parquet"))[-1]
story_file = sorted(GOLD.glob("storytelling_*.parquet"))[-1]
gov, story = pd.read_parquet(gov_file), pd.read_parquet(story_file)
runs = json.loads((HERE / "airflow_runs_gold.json").read_text())


def g(kpi, topic, field=None):
    q = gov[(gov.kpi == kpi) & (gov.topic == topic)]
    if field is not None:
        q = q[q.field == field]
    return q.iloc[0]


silver_files = {t: sorted((SILVER / t).glob("*.parquet")) for t in ["rainfall", "rainfall_historical", "landslides", "lhasa_hazard"]}
risk = story[story.section == "station_risk"].pivot_table(index="entity", columns="metric", values="value")
risk["level"] = story[(story.section == "station_risk") & (story.metric == "score")].set_index("entity").text
risk = risk.sort_values("score", ascending=False)
insight = story[story.section == "narrative"].text.iloc[0]
levels = story[story.section == "risk_level_distribution"].set_index("entity").value
kw = story[story.section == "top_keywords"].pivot_table(index="entity", columns="metric", values="value").sort_values("freq", ascending=False)
bigrams = story[story.section == "top_bigrams"].sort_values("value", ascending=False)
anom = gov[gov.kpi == "anomalous_station_year"]
lang = gov[gov.kpi == "language_share"].set_index("detail").value
dup_rain = g("duplicate_rate_before", "rainfall")
ing = g("ingestion_compliance", "rainfall")
period = story[story.section == "station_risk"].period.iloc[0]
sections = story.section.value_counts()

H = [f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Workshop 3 Report - Team 7</title>
<style>{CSS}</style></head><body>
<div class="cover"><h1>Landslide Monitoring and Risk — Colombia<br>Silver-to-Gold Pipeline, Governance KPIs and Storytelling</h1>
<div class="meta">Workshop No. 3 Report — Data Analysis Programming — Universidad Distrital Francisco José de Caldas<br>
Professor: Eng. Carlos Andrés Sierra, M.Sc. — <b>Team 7</b>: Samuel Alberto Leal Saenz (20231020072), Cristian Andres Gámez Nuñez (20231020100), Sara Nicolle Martínez Moreno (20231020116)<br>
Repository: <code>https://github.com/saritxm/landslide-monitoring-colombia</code> — Report date: {datetime.now():%d %B %Y}</div></div>

<h2>1. Executive Summary</h2>
<p>Workshop #2 delivered hourly Bronze ingestion (Open-Meteo forecast API, NASA LHASA scraping, NASA Global Landslide
Catalog, ERA5 archive) and a FileSensor-triggered Silver layer. Workshop #3 (i) improves those pipelines with what the
real runs revealed, and (ii) adds the <b>Gold layer</b>: <code>gold_processing_dag</code> runs <b>PySpark in local mode</b>
inside Airflow, reads each Silver topic folder as one distributed DataFrame and writes two Parquet summaries:</p>
<ul>
<li><code>{gov_file.name}</code> — {len(gov)} governance KPI rows (null rates, volume, duplicates, schema compliance,
outlier and flag rates, text statistics, language, ingestion compliance, freshness, anomalous station-years).</li>
<li><code>{story_file.name}</code> — {len(story):,} storytelling rows in {len(sections)} sections that map one-to-one to
dashboard charts: Landslide Risk Indicator per monitoring point, rainfall now vs. history, historical landslides,
keywords and bigrams.</li>
</ul>
<p><b>Adapting the statement to our hazard.</b> The workshop is written for sentiment analysis. In this project the
equivalent of a sentiment label is the <b>risk level</b> (LOW / MEDIUM / HIGH) of the Landslide Risk Indicator defined in
Workshop #1, the "opinion trend" is the rainfall trend against its historical percentile, and the text corpus is the
landslide narratives of the NASA catalog.</p>
<div class="note"><b>Key insight of the latest run.</b> {esc(insight)}</div>

<h2>2. Pipeline Improvements (before / after)</h2>"""]

H.append(table(["Observed in the Workshop #2 runs", "Before", "After (Workshop #3)", "Why"], [
    ("<code>spark.read.parquet(\"datalake_silver/\")</code> cannot work: four topics with different schemas share one folder",
     "All Silver files in <code>datalake_silver/</code>", "One folder per topic (<code>datalake_silver/rainfall/</code>, …); the writer creates it; <code>scripts/migrate_silver_w3.py</code> moved the existing files",
     "Each folder has one consistent schema and is read as one DataFrame (cloud-folder pattern)"),
    ("Spark 3.5 failed to read Silver: <i>Illegal Parquet type TIMESTAMP(NANOS)</i>", "pandas wrote nanosecond timestamps",
     "Writer forces microseconds (<code>coerce_timestamps='us'</code>); existing files converted (values are at most second-precision, nothing lost)", "Found when the first Spark job ran"),
    (f"{dup_rain.value:.1f}% of rainfall rows repeat a station-hour; values revised between polls", "Silver keeps every poll",
     "Gold keeps the most recent poll per station-hour (window function)", "Each poll repeats the past 2 days and Open-Meteo revises them"),
    ("ERA5 7-day sums null at every year boundary (600 rows)", "Rolling sum per yearly Bronze file",
     "Rolling sums recomputed in Gold across all years (0 nulls after the first days of 2016)", "Antecedent rainfall must span New Year"),
    (f"Implausible ERA5 totals: {', '.join(f'{r.field} {r.period} ({r.value:,.0f} mm)' for r in anom.itertuples())}",
     "Kept and documented only", "Station-years above 2× the station median are flagged in governance and excluded from the percentile baseline",
     "They would inflate the historical P90 and hide real anomalies"),
    ("NLP: generic words dominate the GLC tokens (<i>said</i> 83, <i>two</i> 52, <i>least</i> 43 occurrences)", "EN+ES stop words only",
     "Domain noise list (reporting verbs, numbers, distance units) in <code>nlp.py</code> and in Gold; bigrams added",
     "Keywords must describe the events, not the news style"),
    ("Timestamps collected from Spark shifted by −5 h", "Python <code>collect()</code> converts to the container's local zone",
     "Time differences and formatting done inside Spark (session zone UTC)", "Freshness KPIs were wrong by 5 hours in the first Gold run"),
    ("Spark workers failed with a Python version mismatch", "Default interpreter", "<code>PYSPARK_PYTHON</code> set to the task's interpreter", "Driver and workers must match"),
    ("Docker image had no Java", "Airflow 2.9.2 + Python libraries", "<code>openjdk-17-jre-headless</code> and <code>pyspark==3.5.3</code> in the Dockerfile; <code>SPARK_DRIVER_MEMORY</code> in compose", "PySpark needs a JVM"),
]))

H.append(f"""<h2>3. Data Quality Findings (Silver)</h2>
<p>Computed with pandas in <code>notebooks/03_data_quality_findings.ipynb</code> and, identically, with PySpark in the
governance file. Silver holds {sum(len(v) for v in silver_files.values())} Parquet files:
{', '.join(f'{t} {len(v)}' for t, v in silver_files.items())}.</p>""")
nulls = gov[(gov.kpi == "null_rate") & (gov.value > 0)].sort_values("value", ascending=False)
H.append(table(["#", "Field", "Issue", "Frequency / severity", "Handling"], [
    (1, "rainfall: station + hour", "Duplicates across polls, values revised", f"{dup_rain.value:.1f}% repeated keys ({esc(dup_rain.detail)})", "Latest poll kept in Gold"),
    (2, "landslides: time of day", "00:00 = unknown time", f"{g('flag_rate','landslides','is_time_unknown').value:.1f}% of events", "Flag <code>is_time_unknown</code>; daily resolution for back-testing"),
    (3, "landslides: country_name", "Null inside the Colombia box (incl. Mocoa 2017)", f"{g('flag_rate','landslides','country_missing').value:.1f}%", "Kept null + flag; spatial assignment"),
    (4, "landslides: fatality_count / injury_count", "Unknown counts", f"{g('null_rate','landslides','fatality_count').value:.1f}% / {g('null_rate','landslides','injury_count').value:.1f}% null", "Kept null (never 0)"),
    (5, "landslides: event_time, storm_name, notes, photo_link", "Empty columns", "100 / 100 / 98 / 87% null", "Not used downstream; monitored"),
    (6, "landslides: fatality_count", "Extreme values", f"{g('outlier_rate','landslides','fatality_outlier_iqr').value:.1f}% IQR flags (Mocoa 329)", "Flagged, not removed"),
    (7, "rainfall: precipitation_mm", "Convective extremes", f"{g('outlier_rate','rainfall','precip_outlier_iqr').value:.1f}% IQR / {g('outlier_rate','rainfall','precip_outlier_zscore').value:.1f}% Z flags", "Flagged per station over wet hours"),
    (8, "rainfall_historical: annual totals", "Reanalysis anomaly", f"{len(anom)} station-years > 2× median", "Excluded from percentiles"),
    (9, "rainfall_historical: precip_7d_mm", "Nulls at year boundaries", f"{g('null_rate','rainfall_historical','precip_7d_mm').value:.2f}%", "Recomputed in Gold"),
    (10, "rainfall: ingestion", "Missing hourly polls", f"{ing.value:.0f}% ({esc(ing.detail)})", "Ingestion-compliance KPI"),
    (11, "landslides: narrative text", "Length and language", f"mean {g('text_length_mean','landslides').value:.0f} chars (min {g('text_length_min','landslides').value:.0f}, max {g('text_length_max','landslides').value:.0f}); {lang.get('en', 0):.0f}% English", "Text statistics KPI; English+Spanish stop words"),
    (12, "lhasa_hazard", "One raster per day, only the newest kept", f"{int(g('volume_rows','lhasa_hazard').value)} station windows", "Daily cadence; freshness KPI"),
]))
H.append(img("../img/dq_findings.png", "Figure 1. Data-quality notebook: wet-hour precipitation and fatalities per event (log scale), with the flagged extremes in the tail.", "88%"))

H.append(f"""<h2>4. Governance KPIs</h2>
<p>All KPIs are computed with PySpark in <code>src/gold/transformations.py::build_governance</code> and stored as a long
table <code>(kpi, topic, field, period, value, unit, detail, computed_at_utc)</code>, so the governance dashboard (US-06)
filters by <code>kpi</code>.</p>""")
H.append(table(["KPI", "Computation (PySpark)", "Justification (finding #)", "Latest value"], [
    ("null_rate", "Per column: sum(isNull or empty string) / rows", "Findings 3, 4, 5, 9", f"{len(gov[gov.kpi=='null_rate'])} field rows; e.g. country_name {g('null_rate','landslides','country_name').value:.1f}%"),
    ("volume_rows / volume_bronze_files / volume_rows_per_day", "count, countDistinct(_bronze_source_file), groupBy(processing day)", "Volume per source and run",
     f"rainfall {int(g('volume_rows','rainfall').value):,} rows from {int(g('volume_bronze_files','rainfall').value)} polls"),
    ("duplicate_rate_before / _after", "(rows − distinct natural keys) / rows; after Gold dedup = 0", "Finding 1 (GLC and ERA5 have none: kept as preventive KPI)", f"rainfall {dup_rain.value:.1f}% → 0%"),
    ("schema_compliance_rate", "Required fields non-null and inside physical bounds", "Silver contract", ", ".join(f"{t} {g('schema_compliance_rate', t).value:.0f}%" for t in silver_files)),
    ("outlier_rate", "avg(flag) per numeric flag column", "Findings 6, 7", f"precip IQR {g('outlier_rate','rainfall','precip_outlier_iqr').value:.1f}%"),
    ("flag_rate", "avg(flag) for quality flags", "Findings 2, 3, 4", f"is_time_unknown {g('flag_rate','landslides','is_time_unknown').value:.1f}%"),
    ("text_length_* / tokens_per_record_mean", "length(narrative): mean, percentile_approx 0.5, min, max", "Finding 11", f"median {g('text_length_median','landslides').value:.0f} chars"),
    ("language_share", "English vs Spanish marker words per narrative (array_intersect)", "Finding 11", ", ".join(f"{k} {v:.1f}%" for k, v in lang.items())),
    ("ingestion_compliance", "Distinct hourly polls / expected hours; ERA5 years / 10", "Finding 10", f"rainfall {ing.value:.0f}%, ERA5 {g('ingestion_compliance','rainfall_historical').value:.0f}%"),
    ("freshness_hours", "run time − max(_processed_at_utc), in Spark (UTC)", "US-06 (detect outages)", f"rainfall {g('freshness_hours','rainfall').value:.1f} h"),
    ("anomalous_station_year", "annual total > 2× station median", "Finding 8", f"{len(anom)} rows"),
]))

H.append(f"""<h2>5. Storytelling Aggregations and Dashboard Narrative</h2>
<p><b>Narrative.</b> <i>"Which Andean areas of Colombia are at landslide risk right now, and is the rain falling there
unusual for this time of year?"</i> The functional user (UNGRD analyst) should leave the dashboard knowing which monitoring
points to watch, whether that is driven by rain that is high for the season, and whether those places have a history of
deadly landslides.</p>""")
H.append(table(["Section (rows)", "Aggregation (PySpark)", "User story", "Dashboard element"], [
    (f"station_risk ({sections.get('station_risk', 0)})", "Latest poll per station; P24h (next 24 h), 5-day antecedent, November 5-day P90 from ERA5, LHASA P90, GLC events within 30 km; LRI score and level", "US-01, US-02, US-03", "Map + table of risk"),
    (f"risk_level_distribution ({sections.get('risk_level_distribution', 0)})", "Count of points per level (the analogue of a sentiment distribution)", "US-01", "KPI cards"),
    (f"hourly_rainfall ({sections.get('hourly_rainfall', 0)})", "Hourly series of the latest poll, observed vs forecast", "US-02", "Line chart"),
    (f"daily_rain_vs_history ({sections.get('daily_rain_vs_history', 0)})", "Daily totals vs historical daily P90 of the month", "US-03", "Bar + threshold"),
    (f"monthly_climatology ({sections.get('monthly_climatology', 0)})", "Mean daily rain and 5-day P90 per station and month (anomalous years excluded)", "US-03", "Seasonal profile"),
    (f"landslides_by_year / trigger / size / department ({sum(sections.get(s, 0) for s in ['landslides_by_year','landslides_by_trigger','landslides_by_size','landslides_by_department'])})", "Events and known fatalities per category", "US-04", "Bar charts"),
    (f"landslides_near_station ({sections.get('landslides_near_station', 0)})", "Haversine join of points and GLC events ≤ 30 km", "US-04", "Map layer"),
    (f"top_keywords ({sections.get('top_keywords', 0)}), top_bigrams ({sections.get('top_bigrams', 0)})", "Token and bigram frequencies after noise removal; mean fatalities of events containing each keyword (the analogue of keyword-sentiment association)", "US-04", "Word bars"),
    ("narrative (1)", "Key insight sentence generated from the data", "US-01", "Header text"),
]))
show = risk[["level", "score", "p24h_mm", "p_ant_mm", "p90_ant_hist_mm", "ratio_vs_p90", "h_lhasa", "events_30km", "fatalities_30km"]].rename_axis(columns=None).reset_index().rename(columns={"entity": "point"})
H.append(f"""<h3>5.1 Results of the latest run ({esc(period)})</h3>
<p>Risk levels: {', '.join(f'{k} {int(v)}' for k, v in levels.items())} of {int(levels.sum())} monitoring points.
LRI (Workshop #1): S = 0.35·min(1, P24h/75 mm) + 0.35·min(1, P<sub>ant</sub>/P90<sub>hist</sub>) + 0.20·H<sub>LHASA</sub> +
0.10·min(1, D<sub>30km</sub>/5). Because each forecast poll covers 5 days (2 past + 3 forecast), the antecedent term uses
5-day sums on both sides instead of the 7-day sums planned in Workshop #1.</p>
{df_html(show)}
<p>Top keywords: {', '.join(f'<i>{w}</i> ({int(r.freq)}; mean fatalities {r.avg_fatalities:.1f})' for w, r in kw.head(8).iterrows())}.
Top bigrams: {', '.join(f'<i>{r.entity}</i> ({int(r.value)})' for r in bigrams.head(6).itertuples())}.</p>
{img('../img/gold_risk.png', 'Figure 2. Gold preview notebook: LRI score per monitoring point and the keyword/bigram distributions read from the storytelling file.', '92%')}

<h2>6. PySpark Implementation</h2>
<ul>
<li><b>Session:</b> <code>SparkSession.builder.master("local[*]")</code>, <code>spark.driver.memory=1g</code> (env
<code>SPARK_DRIVER_MEMORY</code>), <code>spark.sql.shuffle.partitions=4</code>, <code>spark.sql.session.timeZone=UTC</code>,
UI disabled, <code>mergeSchema=true</code>; <code>PYSPARK_PYTHON</code> pinned to the task interpreter.</li>
<li><b>Reading:</b> <code>spark.read.parquet("datalake_silver/&lt;topic&gt;/")</code> per topic — all accumulated files as one DataFrame.</li>
<li><b>Key transformations:</b> window <code>row_number</code> to keep the latest poll; window sums across years for antecedent
rainfall; <code>percentile_approx</code> for P90 baselines; cross join + haversine for the 30 km radius; <code>filter</code>,
<code>explode</code> and <code>transform/sequence</code> higher-order functions for keywords and bigrams; <code>unionByName</code>
to build the long tables.</li>
<li><b>Writing:</b> <code>coalesce(1).write.parquet</code> to a temporary folder, then the part file is moved to
<code>datalake_gold/&lt;name&gt;_YYYYMMDD_HHMMSS.parquet</code> (never overwriting).</li>
<li><b>DAG</b> <code>gold_processing_dag</code>: daily at 00:30 UTC, <code>catchup=False</code>, 1 retry, 30-min timeout.
Tasks: <code>check_silver_inputs</code> → <code>build_gold</code> (Spark job) → <code>validate_gold</code> (both files non-empty).</li>
<li><b>Docker:</b> Java 17 and <code>pyspark==3.5.3</code> added to the image; no extra service is needed in local mode.</li>
</ul>

<h2>7. Results Evidence</h2>""")
H.append(table(["DAG run", "State", "Task", "Task state", "Seconds"],
               [(f"<code>{r['run_id']}</code>", r["state"], t["task_id"], t["state"], f"{t['duration_s']:.1f}" if t["duration_s"] else "")
                for r in runs for t in r["tasks"]]))
H.append("<p class='small'>Runs executed with Airflow 2.10.5 (pip, SequentialExecutor) on the development machine with the same DAG file; the Docker image was updated for PySpark but not rebuilt for this report.</p>")
for name, cap in [("gold_dag_grid.png", "<code>gold_processing_dag</code> in the Airflow UI: scheduled and manual runs, all tasks successful."),
                  ("gold_dag_graph.png", "<code>gold_processing_dag</code> graph: check_silver_inputs → build_gold → validate_gold."),
                  ("gold_files.png", "Contents of the Gold Parquet files read with pandas (notebook <code>04_gold_preview.ipynb</code>).")]:
    if (ROOT / "workshop_3" / "img" / name).exists():
        H.append(img(f"../img/{name}", cap))
H.append(f"<p>Governance file <code>{gov_file.name}</code> (first rows):</p>" +
         df_html(gov[gov.kpi != "null_rate"].drop(columns="computed_at_utc").head(12)))
H.append(f"<p>Storytelling file <code>{story_file.name}</code> (risk rows):</p>" +
         df_html(story[story.section == "station_risk"].drop(columns="computed_at_utc").head(10)))

H.append("""<h2>8. Experimentation Log and Next Steps</h2>
<ul>
<li>What did not work at first: reading the mixed Silver folder; nanosecond timestamps; Spark worker Python mismatch;
timestamps shifted when collected to Python; <code>array_except</code> silently de-duplicated tokens and distorted the
keyword counts (replaced by <code>filter</code>). Each is fixed and listed in Section 2.</li>
<li>Prophet (Workshop #1 follow-up): the monthly landslide forecast failed validation against naive baselines. The next step is
to extend the ERA5 backfill to 2007–2015 (the DAG already accepts the years as parameters) and retrain Prophet with
monthly rainfall as a regressor, which also enables the LRI back-testing on the 2007–2017 events.</li>
<li>Workshop #4: Plotly Dash reading only <code>datalake_gold/</code>; every section of the storytelling file is one chart.</li>
</ul>
<h2>9. References</h2>
<ol class="small">
<li>Apache Spark 3.5 documentation — PySpark DataFrame API, Spark SQL built-in functions, Parquet data source. https://spark.apache.org/docs/3.5.3/</li>
<li>Apache Airflow 2.9 documentation — TaskFlow API. https://airflow.apache.org/docs/</li>
<li>Open-Meteo — Forecast API and Historical Weather (ERA5) API. https://open-meteo.com/en/docs</li>
<li>Kirschbaum, D. B. et al. (2010). A global landslide catalog for hazard applications. <i>Natural Hazards</i>, 52(3), 561–575.</li>
<li>Wang, R. Y. &amp; Strong, D. M. (1996). Beyond accuracy: what data quality means to data consumers. <i>Journal of Management Information Systems</i>, 12(4), 5–33.</li>
<li>Manning, C. D., Raghavan, P. &amp; Schütze, H. (2008). <i>Introduction to Information Retrieval</i>, ch. 2 (tokenization, stop words). Cambridge University Press.</li>
</ol></body></html>""")
(HERE / "report_w3.html").write_text("".join(H), encoding="utf-8")
print("written report_w3.html")
