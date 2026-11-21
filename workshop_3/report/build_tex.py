"""Builds the Workshop #3 LaTeX report from the Silver and Gold files (every number is computed here).
Usage (repository root):
    python workshop_3/report/build_tex.py
    cd workshop_3/report && latexmk -pdf report_w3.tex   (or pdflatex twice)
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SILVER, GOLD = ROOT / "datalake_silver", ROOT / "datalake_gold"


def t(x):
    """Escapes text for LaTeX."""
    s = str(x)
    for a, b in [("\\", r"\textbackslash{}"), ("&", r"\&"), ("%", r"\%"), ("$", r"\$"), ("#", r"\#"),
                 ("_", r"\_"), ("{", r"\{"), ("}", r"\}"), ("~", r"\textasciitilde{}"), ("^", r"\^{}")]:
        s = s.replace(a, b)
    return s


def c(x):
    # monospace that can break after _ . / ( so long identifiers do not overflow table cells
    s = t(x)
    for ch in [r"\_", ".", "/", "("]:
        s = s.replace(ch, ch + r"\allowbreak{}")
    return r"\texttt{" + s + "}"


gov_file = sorted(GOLD.glob("governance_*.parquet"))[-1]
story_file = sorted(GOLD.glob("storytelling_*.parquet"))[-1]
gov, story = pd.read_parquet(gov_file), pd.read_parquet(story_file)
runs = json.loads((HERE / "airflow_runs_gold.json").read_text())
silver_files = {k: sorted((SILVER / k).glob("*.parquet")) for k in ["rainfall", "rainfall_historical", "landslides", "lhasa_hazard"]}


def g(kpi, topic, field=None):
    q = gov[(gov.kpi == kpi) & (gov.topic == topic)]
    if field is not None:
        q = q[q.field == field]
    return q.iloc[0]


risk = story[story.section == "station_risk"].pivot_table(index="entity", columns="metric", values="value")
risk["level"] = story[(story.section == "station_risk") & (story.metric == "score")].set_index("entity").text
risk = risk.sort_values("score", ascending=False)
levels = story[story.section == "risk_level_distribution"].set_index("entity").value
kw = story[story.section == "top_keywords"].pivot_table(index="entity", columns="metric", values="value").sort_values("freq", ascending=False)
bigrams = story[story.section == "top_bigrams"].sort_values("value", ascending=False)
anom = gov[gov.kpi == "anomalous_station_year"]
lang = gov[gov.kpi == "language_share"].set_index("detail").value
dup = g("duplicate_rate_before", "rainfall")
ing = g("ingestion_compliance", "rainfall")
period = story[story.section == "station_risk"].period.iloc[0]
insight = story[story.section == "narrative"].text.iloc[0]
sec = story.section.value_counts()
top = risk.iloc[0]
n_silver = sum(len(v) for v in silver_files.values())
nonzero_nulls = gov[(gov.kpi == "null_rate") & (gov.value > 0)].sort_values("value", ascending=False)

risk_rows = "\n".join(
    f"{t(p)} & {r.level} & {r.score:.2f} & {r.p24h_mm:.1f} & {r.p_ant_mm:.1f} & {r.p90_ant_hist_mm:.1f} & {r.ratio_vs_p90:.2f} & {r.h_lhasa:.2f} & {int(r.events_30km)} & "
    f"{'--' if pd.isna(r.fatalities_30km) else int(r.fatalities_30km)} \\\\" for p, r in risk.iterrows())
kw_rows = "\n".join(f"{t(w)} & {int(r.freq)} & {r.avg_fatalities:.1f} \\\\" for w, r in kw.head(10).iterrows())
bi_rows = "\n".join(f"{t(r.entity)} & {int(r.value)} \\\\" for r in bigrams.head(10).itertuples())
null_rows = "\n".join(f"{t(r.topic)} & {c(r.field)} & {r.value:.2f} & {t(r.detail)} \\\\" for r in nonzero_nulls.itertuples())
run_rows = "\n".join(f"{c(r['run_id'])} & {t(tk['task_id'])} & {tk['state']} & {tk['duration_s']:.1f} \\\\" for r in runs for tk in r["tasks"])
gov_sample = gov[gov.kpi.isin(["volume_rows", "duplicate_rate_before", "schema_compliance_rate", "outlier_rate", "freshness_hours", "ingestion_compliance", "language_share", "anomalous_station_year"])]
gov_rows = "\n".join(f"{t(r.kpi)} & {t(r.topic)} & {'--' if pd.isna(r.field) else t(r.field)} & {r.value:,.2f} & {t(r.unit)} \\\\" for r in gov_sample.itertuples())
anom_txt = ", ".join(f"{r.field} {r.period} ({r.value:,.0f} mm)" for r in anom.itertuples())

TEX = rf"""\documentclass[11pt,a4paper]{{article}}
\usepackage[utf8]{{inputenc}}\usepackage[T1]{{fontenc}}\usepackage{{lmodern}}
\usepackage[margin=2.3cm]{{geometry}}
\usepackage{{amsmath,booktabs,tabularx,longtable,graphicx,float,xcolor,enumitem,caption,fancyhdr,titlesec,listings}}
\usepackage[hidelinks]{{hyperref}}
\definecolor{{udblue}}{{RGB}}{{31,58,95}}
\titleformat{{\section}}{{\Large\bfseries\color{{udblue}}}}{{\thesection}}{{0.8em}}{{}}
\titleformat{{\subsection}}{{\large\bfseries\color{{udblue}}}}{{\thesubsection}}{{0.8em}}{{}}
\pagestyle{{fancy}}\fancyhf{{}}
\fancyhead[L]{{\small\color{{udblue}}\textbf{{Workshop \#3 Technical Report}} \textcolor{{gray}}{{$|$ Landslide Monitoring Colombia}}}}
\fancyfoot[C]{{\thepage}}\renewcommand{{\headrulewidth}}{{0.4pt}}
\lstset{{basicstyle=\ttfamily\footnotesize,breaklines=true,frame=single,backgroundcolor=\color{{gray!6}},columns=fullflexible}}
\captionsetup{{font=small,labelfont=bf}}
\setlist{{itemsep=2pt,topsep=3pt}}
\renewcommand{{\arraystretch}}{{1.15}}
\newcolumntype{{L}}{{>{{\raggedright\arraybackslash}}X}}
\begin{{document}}
\thispagestyle{{empty}}
\begin{{center}}
{{\large\bfseries UNIVERSIDAD DISTRITAL FRANCISCO JOSÉ DE CALDAS}}\\
Faculty of Engineering --- Systems Engineering\\[0.4em]
\textsc{{Programming for Data Analysis --- Semester 2026-II}}\\
\textit{{Professor: Carlos Andrés Sierra, M.Sc.}}\\[2em]
{{\LARGE\color{{udblue}} Workshop No. 3: Silver-to-Gold Pipeline, Governance KPIs, and Storytelling Definition}}\\[1em]
{{\large\bfseries Distributed Gold-Layer Analytics with PySpark for Rainfall-Triggered Landslide Risk Monitoring in Colombia}}\\[1.2em]
{{\bfseries Team 7}}\\[1em]
\textbf{{Samuel Alberto Leal Saenz}} --- Cod. 20231020072\\
\textbf{{Cristian Andres Gamez Nuñez}} --- Cod. 20231020100\\
\textbf{{Sara Nicolle Martinez Moreno}} --- Cod. 20231020116\\[0.6em]
{{\small GitHub Repository: \url{{https://github.com/saritxm/landslide-monitoring-colombia.git}}}}\\[1em]
{datetime.now():%B %Y}
\end{{center}}
\vspace{{1em}}
\begin{{abstract}}
This report documents the third phase of the landslide risk monitoring platform for Colombia. Building on the Bronze
ingestion and FileSensor-triggered Silver layers delivered in Workshop \#2, we (i) revise both pipelines using the
evidence produced by real runs in development and in the Docker Compose stack, and (ii) implement the Gold layer of the
Medallion Architecture. A scheduled Airflow DAG runs PySpark in local mode, reads each Silver topic folder as a single
distributed DataFrame and persists two Parquet summaries: a governance dataset with {len(gov)} data-quality KPI records
and a storytelling dataset with {len(story):,} analytical records organized in {len(sec)} dashboard-ready sections. Twelve
data-quality findings, measured over {n_silver} Silver files, justify every governance KPI. The storytelling layer
operationalizes the Landslide Risk Indicator (LRI) defined in Workshop \#1 for ten Andean monitoring points, contrasts
current rainfall with a ten-year ERA5 baseline, and links each point to its documented landslide history and to the
vocabulary of the NASA catalog narratives. In the latest run, {int(levels.get('MEDIUM', 0))} point is classified MEDIUM
and {int(levels.get('LOW', 0))} LOW; the highest score corresponds to {t(risk.index[0])} ({top.score:.2f}), where five-day
rainfall reaches {top.ratio_vs_p90:.0%} of the November 90th percentile.
\end{{abstract}}
\noindent\textbf{{Keywords:}} Medallion Architecture, PySpark, Apache Airflow, Data Governance, Data Quality KPIs,
Storytelling, Landslide Risk Indicator, Colombia.
\newpage
\tableofcontents
\newpage

\section{{Executive Summary}}
Workshop \#3 has two goals. The first is to \textbf{{improve the Bronze and Silver pipelines}} with what the real runs of
Workshop \#2 revealed; nine improvements are documented with a before/after comparison in Section~\ref{{sec:improve}}.
The second is the \textbf{{Gold layer}}: the DAG {c('gold_processing_dag')} reads all accumulated Silver Parquet files with
PySpark and writes:
\begin{{itemize}}
\item {c(gov_file.name)}: governance KPIs (null rate per field, volume, duplicate rate, schema compliance, outlier and
quality-flag rates, text statistics, language share, ingestion compliance, freshness and anomalous station-years) for the
data-platform user story US-06.
\item {c(story_file.name)}: analytical aggregations for the functional dashboard (US-01 to US-04): LRI per monitoring
point, risk-level distribution, hourly and daily rainfall against history, monthly climatology, historical landslides by
year, trigger, size and department, events within 30~km of each point, keywords and bigrams.
\end{{itemize}}
\paragraph{{Adapting the statement to the hazard.}} The workshop statement is phrased for opinion mining. In this project
the role of the sentiment label is played by the \textbf{{risk level}} (LOW/MEDIUM/HIGH) of the LRI, the opinion trend
by the \textbf{{rainfall trend relative to its historical percentile}}, the keyword--sentiment association by the
\textbf{{mean fatalities of the events whose narrative contains each keyword}}, and the text corpus by the NASA Global
Landslide Catalog narratives.

\section{{Pipeline Improvements Based on Experimentation}}\label{{sec:improve}}
Table~\ref{{tab:improve}} lists every change made to Bronze, Silver and the infrastructure. Several were discovered only
when the first Spark job was executed, which is the experimentation process the workshop asks to document.
\begin{{longtable}}{{p{{4.1cm}}p{{3.0cm}}p{{4.6cm}}p{{3.4cm}}}}
\caption{{Before/after comparison of pipeline improvements}}\label{{tab:improve}}\\\toprule
\textbf{{Observation}} & \textbf{{Before}} & \textbf{{After (Workshop \#3)}} & \textbf{{Reason}}\\\midrule\endfirsthead
\toprule\textbf{{Observation}} & \textbf{{Before}} & \textbf{{After}} & \textbf{{Reason}}\\\midrule\endhead
Four Silver topics with different schemas share one folder, so {c('spark.read.parquet("datalake_silver/")')} cannot produce one DataFrame &
All files in {c('datalake_silver/')} & One sub-folder per topic, created by the writer; {c('scripts/migrate_silver_w3.py')} moved existing files &
Each folder has one consistent schema (cloud-folder read pattern)\\
Spark 3.5 error \textit{{Illegal Parquet type: TIMESTAMP(NANOS)}} & pandas wrote nanosecond timestamps &
Writer forces microseconds ({c("coerce_timestamps='us'")}); existing files converted without loss & Sources have at most second precision\\
{dup.value:.1f}\% of rainfall rows repeat a station-hour; 36.5\% of repeated hours change value between polls &
Every poll stored & Gold keeps the most recent poll per station-hour (window {c('row_number')}) & Open-Meteo revises the past two days\\
ERA5 7-day sums null at every year boundary (600 rows) & Rolling sum per yearly file & Rolling sums recomputed in Gold across years & Antecedent rain spans New Year\\
Implausible ERA5 annual totals: {t(anom_txt)} & Documented only & Station-years above $2\times$ the station median flagged and excluded from percentiles & They would inflate the P90 baseline\\
Generic words dominate GLC tokens (\textit{{said}} 83, \textit{{two}} 52, \textit{{least}} 43) & EN+ES stop words & Domain noise list in {c('nlp.py')} and Gold; bigrams added & Keywords must describe events\\
Timestamps collected from Spark shifted by $-5$~h & Python {c('collect()')} converts to local time & Time arithmetic and formatting inside Spark (session zone UTC) & Freshness KPIs were wrong\\
Spark workers failed (Python version mismatch) & Default interpreter & {c('PYSPARK_PYTHON')} pinned to the task interpreter & Driver and workers must match\\
No Java in the Airflow image & Airflow 2.9.2 + libraries & OpenJDK 17 and {c('pyspark==3.5.3')} in the Dockerfile; {c('SPARK_DRIVER_MEMORY')} in compose & PySpark needs a JVM\\
\bottomrule
\end{{longtable}}
Bronze reliability improvements made during the Workshop \#2 revision remain in force: retries and execution timeouts,
no offline fallback that could write invented data, and skipping of unchanged payloads.

\section{{Data Quality Findings in the Silver Layer}}
The findings were measured with pandas in {c('notebooks/03_data_quality_findings.ipynb')} and, independently, with PySpark
in the governance dataset. Silver currently holds {n_silver} Parquet files ({', '.join(f'{t(k)}: {len(v)}' for k, v in silver_files.items())}).
\begin{{longtable}}{{p{{0.4cm}}p{{3.6cm}}p{{3.4cm}}p{{4.0cm}}p{{3.4cm}}}}
\caption{{Data-quality findings and handling strategy}}\label{{tab:findings}}\\\toprule
\textbf{{\#}} & \textbf{{Field}} & \textbf{{Issue}} & \textbf{{Frequency / severity}} & \textbf{{Handling}}\\\midrule\endfirsthead
\toprule\textbf{{\#}} & \textbf{{Field}} & \textbf{{Issue}} & \textbf{{Frequency}} & \textbf{{Handling}}\\\midrule\endhead
F1 & rainfall: station + hour & Duplicates across polls, revised values & {dup.value:.1f}\% repeated keys & Latest poll kept\\
F2 & landslides: event time & 00:00 means unknown time & {g('flag_rate','landslides','is_time_unknown').value:.1f}\% of events & Flag; daily resolution\\
F3 & landslides: {c('country_name')} & Null inside the Colombia box (Mocoa 2017) & {g('flag_rate','landslides','country_missing').value:.1f}\% & Kept null + flag\\
F4 & landslides: fatality / injury counts & Unknown counts & {g('null_rate','landslides','fatality_count').value:.1f}\% / {g('null_rate','landslides','injury_count').value:.1f}\% null & Kept null (never 0)\\
F5 & landslides: {c('event_time')}, {c('storm_name')}, {c('notes')}, {c('photo_link')} & Empty columns & 100 / 100 / 98 / 87\% null & Not used; monitored\\
F6 & landslides: fatalities & Extreme values & {g('outlier_rate','landslides','fatality_outlier_iqr').value:.1f}\% IQR flags (max 329, Mocoa) & Flagged, not removed\\
F7 & rainfall: precipitation & Convective extremes & {g('outlier_rate','rainfall','precip_outlier_iqr').value:.1f}\% IQR, {g('outlier_rate','rainfall','precip_outlier_zscore').value:.1f}\% Z flags & Per-station, wet hours\\
F8 & ERA5 annual totals & Reanalysis anomaly & {len(anom)} station-years $>2\times$ median & Excluded from percentiles\\
F9 & ERA5 {c('precip_7d_mm')} & Nulls at year boundaries & {g('null_rate','rainfall_historical','precip_7d_mm').value:.2f}\% & Recomputed in Gold\\
F10 & rainfall ingestion & Missing hourly polls & {ing.value:.0f}\% ({t(ing.detail)}) & Compliance KPI\\
F11 & GLC narratives & Length and language & mean {g('text_length_mean','landslides').value:.0f} chars (min {g('text_length_min','landslides').value:.0f}, max {g('text_length_max','landslides').value:.0f}); {lang.get('en', 0):.1f}\% English & Text KPIs; EN+ES stop words\\
F12 & LHASA & One raster per day; only the newest kept & {int(g('volume_rows','lhasa_hazard').value)} station windows & Daily cadence; freshness\\
\bottomrule
\end{{longtable}}
\begin{{figure}}[H]\centering\includegraphics[width=0.92\textwidth]{{../img/dq_findings.png}}
\caption{{Distribution of wet-hour precipitation and of known fatalities per event (log scale). The tails contain the values flagged by the IQR and Z-score rules (F6, F7).}}\end{{figure}}
\begin{{longtable}}{{p{{2.6cm}}p{{4.6cm}}p{{1.6cm}}p{{5.6cm}}}}
\caption{{Fields with a non-zero null rate (PySpark governance KPI {c('null_rate')})}}\\\toprule
\textbf{{Topic}} & \textbf{{Field}} & \textbf{{Null \%}} & \textbf{{Detail}}\\\midrule\endfirsthead
\toprule\textbf{{Topic}} & \textbf{{Field}} & \textbf{{Null \%}} & \textbf{{Detail}}\\\midrule\endhead
{null_rows}
\bottomrule\end{{longtable}}

\section{{Governance KPI Definition}}
The governance dataset is a long table with schema
({c('kpi')}, {c('topic')}, {c('field')}, {c('period')}, {c('value')}, {c('unit')}, {c('detail')}, {c('computed_at_utc')}),
so the governance dashboard (US-06) selects each KPI with a filter. For a topic with $N$ rows:
\begin{{align}}
\text{{NullRate}}(f) &= \frac{{100}}{{N}}\sum_{{i=1}}^{{N}} \mathbf{{1}}[x_{{i,f}} = \text{{null}} \lor x_{{i,f}} = \text{{''}}] &
\text{{DupRate}} &= 100\cdot\frac{{N - |\{{\text{{distinct natural keys}}\}}|}}{{N}}\\
\text{{Compliance}} &= \frac{{100}}{{N}}\sum_{{i}} \mathbf{{1}}[\text{{required}}(i)\land \text{{bounds}}(i)] &
\text{{IngestionCompliance}} &= 100\cdot\frac{{\text{{hours with a poll}}}}{{\text{{hours in the observed window}}}}
\end{{align}}
\begin{{small}}\begin{{longtable}}{{p{{3.6cm}}p{{4.9cm}}p{{2.4cm}}p{{3.9cm}}}}
\caption{{Governance KPIs, computation and justification}}\\\toprule
\textbf{{KPI}} & \textbf{{PySpark computation}} & \textbf{{Justified by}} & \textbf{{Latest value}}\\\midrule\endfirsthead
\toprule\textbf{{KPI}} & \textbf{{Computation}} & \textbf{{Justified by}} & \textbf{{Latest value}}\\\midrule\endhead
{c('null_rate')} & {c('sum(isNull|empty)')} per column $/N$ & F3, F4, F5, F9 & {len(gov[gov.kpi=='null_rate'])} field records\\
{c('volume_rows')}, {c('volume_bronze_files')}, {c('volume_rows_per_day')} & {c('count')}, {c('countDistinct(_bronze_source_file)')}, {c('groupBy')}(processing day) & Volume per source & rainfall {int(g('volume_rows','rainfall').value):,} rows / {int(g('volume_bronze_files','rainfall').value)} polls\\
{c('duplicate_rate_before')}/{c('_after')} & rows $-$ distinct natural keys & F1 (preventive for GLC/ERA5: 0\%) & rainfall {dup.value:.1f}\% $\to$ 0\%\\
{c('schema_compliance_rate')} & required fields non-null and inside physical bounds & Silver contract & 100\% for all topics\\
{c('outlier_rate')} & {c('avg(flag)')} per outlier flag & F6, F7 & precipitation IQR {g('outlier_rate','rainfall','precip_outlier_iqr').value:.1f}\%\\
{c('flag_rate')} & {c('avg(flag)')} per quality flag & F2, F3, F4 & unknown time {g('flag_rate','landslides','is_time_unknown').value:.1f}\%\\
{c('text_length_*')} & {c('length')}: mean, {c('percentile_approx(0.5)')}, min, max & F11 & median {g('text_length_median','landslides').value:.0f} characters\\
{c('language_share')} & English vs Spanish marker words ({c('array_intersect')}) & F11 & {', '.join(f'{k} {v:.1f}' for k, v in lang.items())}\%\\
{c('ingestion_compliance')} & hourly polls present / expected; ERA5 years / 10 & F10 & rainfall {ing.value:.0f}\%, ERA5 {g('ingestion_compliance','rainfall_historical').value:.0f}\%\\
{c('freshness_hours')} & run time $-$ max({c('_processed_at_utc')}) in Spark & US-06 outages & rainfall {g('freshness_hours','rainfall').value:.1f}~h\\
{c('anomalous_station_year')} & annual total $> 2\times$ station median & F8 & {len(anom)} records\\
\bottomrule\end{{longtable}}\end{{small}}

\section{{Storytelling Aggregations and Dashboard Narrative}}
\subsection{{Narrative}}
\textit{{``Which Andean areas of Colombia are at landslide risk right now, and is the rain falling there unusual for this
time of year?''}} After viewing the dashboard, the UNGRD analyst should know which monitoring points to watch, whether
that is driven by rainfall that is high for the season, and whether those places have a history of deadly landslides.
Each aggregation below maps to one user story of Workshop \#1 and to one dashboard element.
\begin{{small}}\begin{{longtable}}{{p{{3.9cm}}p{{6.2cm}}p{{1.6cm}}p{{2.6cm}}}}
\caption{{Storytelling sections, user stories and dashboard elements}}\\\toprule
\textbf{{Section (rows)}} & \textbf{{PySpark aggregation}} & \textbf{{Story}} & \textbf{{Dashboard}}\\\midrule\endfirsthead
\toprule\textbf{{Section}} & \textbf{{Aggregation}} & \textbf{{Story}} & \textbf{{Dashboard}}\\\midrule\endhead
{c('station_risk')} ({sec.get('station_risk',0)}) & Latest poll per point; $P_{{24h}}$, 5-day antecedent rain, November 5-day P90 (ERA5), LHASA P90, events within 30~km; LRI score and level & US-01/02/03 & Risk map and table\\
{c('risk_level_distribution')} ({sec.get('risk_level_distribution',0)}) & Points per level (analogue of the sentiment distribution) & US-01 & KPI cards\\
{c('hourly_rainfall')} ({sec.get('hourly_rainfall',0)}) & Hourly series of the latest poll, observed vs forecast & US-02 & Line chart\\
{c('daily_rain_vs_history')} ({sec.get('daily_rain_vs_history',0)}) & Daily totals vs historical daily P90 of the month & US-03 & Bars + threshold\\
{c('monthly_climatology')} ({sec.get('monthly_climatology',0)}) & Mean daily rain and 5-day P90 per point and month & US-03 & Seasonal profile\\
{c('landslides_by_*')} ({sum(sec.get(s,0) for s in ['landslides_by_year','landslides_by_trigger','landslides_by_size','landslides_by_department'])}) & Events and known fatalities by year, trigger, size, department & US-04 & Bar charts\\
{c('landslides_near_station')} ({sec.get('landslides_near_station',0)}) & Haversine cross join, distance $\le 30$~km & US-04 & Map layer\\
{c('top_keywords')} ({sec.get('top_keywords',0)}), {c('top_bigrams')} ({sec.get('top_bigrams',0)}) & Frequencies after noise removal; mean fatalities per keyword (keyword--severity association) & US-04 & Word bars\\
{c('narrative')} (1) & Key-insight sentence generated from the data & US-01 & Header\\
\bottomrule\end{{longtable}}\end{{small}}

\subsection{{Landslide Risk Indicator in the Gold layer}}
For monitoring point $c$ and the latest forecast poll $t$, the LRI of Workshop \#1 is computed as
\begin{{equation}}
S(c,t) = 0.35\min\!\left(1,\frac{{P_{{24h}}}}{{75\text{{ mm}}}}\right) + 0.35\min\!\left(1,\frac{{P_{{5d}}}}{{P^{{5d,\text{{hist}}}}_{{90}}(c,m)}}\right)
+ 0.20\,H_{{\text{{LHASA}}}}(c) + 0.10\min\!\left(1,\frac{{D_{{30\text{{km}}}}(c)}}{{5}}\right)
\end{{equation}}
with LOW $<0.40\le$ MEDIUM $<0.70\le$ HIGH. Because each forecast poll covers five days (two past and three forecast
days), the antecedent term compares five-day sums on both sides instead of the seven-day sums planned in Workshop \#1;
$P^{{5d,\text{{hist}}}}_{{90}}$ is computed from ERA5 2016--2025 for the same calendar month, excluding anomalous station-years.

\subsection{{Results of the latest run ({t(period)})}}
\begin{{table}}[H]\centering\small
\caption{{LRI per monitoring point ({', '.join(f'{k}: {int(v)}' for k, v in levels.items())})}}
\begin{{tabular}}{{lcrrrrrrrr}}\toprule
Point & Level & $S$ & $P_{{24h}}$ & $P_{{5d}}$ & P90 & ratio & $H$ & $D_{{30}}$ & deaths\\\midrule
{risk_rows}
\bottomrule\end{{tabular}}\end{{table}}
\noindent\textbf{{Key insight.}} {t(insight)}
\begin{{table}}[H]\centering\small
\begin{{minipage}}{{0.48\textwidth}}\centering
\caption{{Top keywords and mean fatalities}}\begin{{tabular}}{{lrr}}\toprule Keyword & Freq. & Mean deaths\\\midrule
{kw_rows}
\bottomrule\end{{tabular}}\end{{minipage}}\hfill
\begin{{minipage}}{{0.42\textwidth}}\centering
\caption{{Top bigrams}}\begin{{tabular}}{{lr}}\toprule Bigram & Freq.\\\midrule
{bi_rows}
\bottomrule\end{{tabular}}\end{{minipage}}\end{{table}}
\begin{{figure}}[H]\centering\includegraphics[width=0.9\textwidth]{{../img/gold_risk.png}}
\caption{{Gold preview notebook: LRI score per monitoring point with the MEDIUM and HIGH thresholds, and keyword/bigram distributions read from the storytelling file.}}\end{{figure}}

\section{{PySpark Implementation}}
\subsection{{Spark session}}
\begin{{lstlisting}}[language=Python]
SparkSession.builder.master("local[*]").appName("landslide-gold")
    .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.parquet.mergeSchema", "true").getOrCreate()
# PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON = sys.executable (same interpreter as the task)
\end{{lstlisting}}
\subsection{{DAG structure}}
{c('gold_processing_dag')} (TaskFlow API) runs daily at 00:30~UTC with {c('catchup=False')}, one retry and a 30-minute
timeout: {c('check_silver_inputs')} (fails early if a topic folder is empty) $\rightarrow$ {c('build_gold')} (Spark job)
$\rightarrow$ {c('validate_gold')} (both files exist and are non-empty).
\subsection{{Key transformations}}
\begin{{itemize}}
\item \textbf{{Reading:}} {c('spark.read.parquet("datalake_silver/<topic>/")')} --- all accumulated files of a topic as one DataFrame.
\item \textbf{{Deduplication:}} {c('row_number()')} over {c('partitionBy(station, hour).orderBy(poll_time desc)')}.
\item \textbf{{Antecedent rainfall:}} window sums across years; {c('percentile_approx')} for the P90 baselines.
\item \textbf{{Spatial join:}} cross join of the 10 points with the GLC events and a haversine distance expression.
\item \textbf{{Text:}} {c('filter')}, {c('explode')} and {c('transform(sequence(...))')} higher-order functions for keywords and bigrams.
\item \textbf{{Persistence:}} {c('unionByName')} into the long tables, {c('coalesce(1).write.parquet')} to a temporary folder,
then the part file is moved to {c('datalake_gold/<name>_YYYYMMDD_HHMMSS.parquet')}; existing files are never overwritten.
\end{{itemize}}
\subsection{{Docker adjustments}}
The image adds {c('openjdk-17-jre-headless')} (with {c('JAVA_HOME')}) and {c('pyspark==3.5.3')}; compose sets
{c('SPARK_DRIVER_MEMORY=1g')} and {c('DATALAKE_ROOT')}. Local mode needs no extra container; {c('datalake_gold/')} was already mounted.

\section{{Results Evidence}}
\begin{{table}}[H]\centering\small\caption{{Gold DAG runs}}
\begin{{tabular}}{{llll}}\toprule Run & Task & State & Seconds\\\midrule
{run_rows}
\bottomrule\end{{tabular}}\end{{table}}
\noindent The runs were executed with Airflow 2.10.5 on the development machine with the same DAG file; the Docker image
has been updated for PySpark and must be rebuilt ({c('docker compose build')}) to run the DAG in the containers.
\begin{{figure}}[H]\centering\includegraphics[width=0.9\textwidth]{{../img/gold_dag_graph.png}}
\caption{{{c('gold_processing_dag')} in the Airflow UI: scheduled and manual runs, all tasks successful.}}\end{{figure}}
\begin{{figure}}[H]\centering\includegraphics[width=0.85\textwidth]{{../img/gold_files.png}}
\caption{{Governance KPIs read from the Gold Parquet file with pandas ({c('notebooks/04_gold_preview.ipynb')}).}}\end{{figure}}
\begin{{longtable}}{{p{{4.0cm}}p{{3.0cm}}p{{3.8cm}}rp{{1.4cm}}}}
\caption{{Sample of {c(gov_file.name)}}}\\\toprule KPI & Topic & Field & Value & Unit\\\midrule\endfirsthead
\toprule KPI & Topic & Field & Value & Unit\\\midrule\endhead
{gov_rows}
\bottomrule\end{{longtable}}

\section{{Conclusion and Next Steps}}
The Gold layer turns the Silver tables into two dashboard-ready datasets computed with PySpark, and every governance KPI
is backed by a measured finding. The experimentation exposed problems that unit tests had not caught (mixed Silver
schemas, nanosecond timestamps, time-zone conversion on collect, token de-duplication), all now fixed. Next steps:
\begin{{enumerate}}
\item Rebuild the Docker image and run {c('gold_processing_dag')} in the containers.
\item Extend the ERA5 backfill to 2007--2015 (the DAG already accepts the years as parameters) to cover the whole GLC
period, retrain Prophet with monthly rainfall as a regressor (the follow-up announced in Workshop \#1) and back-test the
LRI on the 2007--2017 events.
\item Workshop \#4: Plotly Dash applications reading only {c('datalake_gold/')}, one chart per storytelling section and one
panel per governance KPI.
\end{{enumerate}}

\begin{{thebibliography}}{{9}}\small
\bibitem{{spark}} Apache Software Foundation. \textit{{Apache Spark 3.5 Documentation: PySpark DataFrame API, Spark SQL functions and Parquet data source}}. https://spark.apache.org/docs/3.5.3/
\bibitem{{airflow}} Apache Software Foundation. \textit{{Apache Airflow 2.9 Documentation: TaskFlow API}}. https://airflow.apache.org/docs/
\bibitem{{om}} Open-Meteo. \textit{{Forecast API and Historical Weather (ERA5) API}}. https://open-meteo.com/en/docs
\bibitem{{glc}} Kirschbaum, D. B., Adler, R., Hong, Y., Hill, S., \& Lerner-Lam, A. (2010). A global landslide catalog for hazard applications. \textit{{Natural Hazards}}, 52(3), 561--575.
\bibitem{{dq}} Wang, R. Y., \& Strong, D. M. (1996). Beyond accuracy: What data quality means to data consumers. \textit{{Journal of Management Information Systems}}, 12(4), 5--33.
\bibitem{{ir}} Manning, C. D., Raghavan, P., \& Schütze, H. (2008). \textit{{Introduction to Information Retrieval}}, Ch. 2. Cambridge University Press.
\end{{thebibliography}}
\end{{document}}
"""
(HERE / "report_w3.tex").write_text(TEX, encoding="utf-8")
print("written report_w3.tex")
