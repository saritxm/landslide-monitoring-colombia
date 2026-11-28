# Landslide Monitoring and Risk — Colombia

Data Analysis Programming, Semester 2026-I — Universidad Distrital Francisco José de Caldas  
Team 7 — Professor: Eng. Carlos Andrés Sierra, M.Sc.

## Hazard & Domain Context

Landslides (mass movements) in Colombia are predominantly triggered by intense or prolonged rainfall on steep slopes across the Andean and Pacific regions. This platform implements an end-to-end Medallion Lakehouse that integrates historical landslide inventories, historical and real-time precipitation models, and satellite hazard rasters to compute a regional **Landslide Risk Indicator (LRI)** and deliver interactive decision-support dashboards.

- **Geographic scope:** Colombia (Bounding Box: lat −4.3 to 13.5, lon −79.1 to −66.8; 10 Andean monitoring points)
- **Historical window:** Landslides 2007–2017 (NASA GLC catalog); rainfall 2016–2025 (ERA5 reanalysis)
- **Incremental window:** Open-Meteo hourly forecast (`past_days=2` + 3 forecast days) and NASA LHASA NRT hazard rasters

## Team 7 Members

| Student ID | Name |
| :--- | :--- |
| 20231020072 | Samuel Alberto Leal Saenz |
| 20231020100 | Cristian Andres Gámez Nuñez |
| 20231020116 | Sara Nicolle Martínez Moreno |

---

## Technical Architecture (Medallion Lakehouse)

The system is orchestrated with **Apache Airflow** in Docker Compose, processes Gold-layer aggregations in **PySpark (`local[*]`)**, and serves two independent **Plotly Dash** web applications reading exclusively from the Gold Parquet files:

```
[ Open-Meteo API ]      [ NASA LHASA Scraper ]      [ NASA GLC Catalog ]
        │                         │                          │
        └─────────────────────────┼──────────────────────────┘
                                  ▼
                     [ bronze_ingestion_dag ] (Airflow TaskFlow API)
                                  │
                                  ▼
                   datalake_bronze/source_YYYYMMDD_HHMMSS.json
                                  │
                                  ▼
            [ FileSensor: waits for datalake_bronze/_READY.flag ]
                                  │
                                  ▼
                     [ silver_processing_dag ]
         (Schema Enforcement · Null Flagging · NLP Normalization · IQR/Z-score Outliers)
                                  │
                                  ▼
                 datalake_silver/<topic>/topic_YYYYMMDD_HHMMSS.parquet
                                  │
                                  ▼
                      [ gold_processing_dag ] (PySpark)
                                  │
                ┌─────────────────┴─────────────────┐
                ▼                                   ▼
datalake_gold/governance_*.parquet   datalake_gold/storytelling_*.parquet
                │                                   │
                ▼                                   ▼
   [ Governance Dashboard ]            [ Storytelling Dashboard ]
     http://localhost:8050               http://localhost:8051
```

1. **Bronze Layer (`datalake_bronze/`):** Immutable landing zone storing raw JSON payloads (`source_YYYYMMDD_HHMMSS.json`) and `_READY.flag`.
2. **Silver Layer (`datalake_silver/<topic>/`):** Cleaned, typed, microsecond-timestamp Parquet tables partitioned into topic folders (`rainfall/`, `rainfall_historical/`, `landslides/`, `lhasa_hazard/`) with bilingual NLP normalization and statistical outlier flags.
3. **Gold Layer (`datalake_gold/`):** PySpark analytical summaries (`governance_YYYYMMDD_HHMMSS.parquet` and `storytelling_YYYYMMDD_HHMMSS.parquet`) serving as the single source of truth for the presentation layer.
4. **Presentation Layer (`dashboard/`):** Two independent Plotly Dash applications (`governance_app.py` on port `8050` and `storytelling_app.py` on port `8051`) reading exclusively via `pd.read_parquet()`.

---

## Repository Structure

```
├── airflow/
│   └── dags/
│       ├── bronze_ingestion_dag.py            # Hourly TaskFlow API ingestion DAG
│       ├── bronze_historical_backfill_dag.py  # Manual ERA5 daily-rainfall backfill by year
│       ├── silver_processing_dag.py           # FileSensor-triggered Silver preprocessing DAG
│       └── gold_processing_dag.py             # Scheduled PySpark Silver-to-Gold DAG
├── config/
│   └── sources.yaml                           # Declarative source contracts & schedules
├── dashboard/
│   ├── governance_app.py                      # Governance Dashboard (http://localhost:8050)
│   ├── storytelling_app.py                    # Storytelling Dashboard (http://localhost:8051)
│   ├── utils.py                               # Gold Parquet loader, Okabe-Ito palette & layout helpers
│   └── README.md                              # Dashboard architecture & component reference
├── datalake_bronze/                           # Bronze raw JSON landing zone
├── datalake_silver/                           # Silver topic folders (Parquet)
├── datalake_gold/                             # Gold Parquet outputs (governance & storytelling)
├── notebooks/
│   ├── 01_source_profiling.ipynb              # Workshop #1 source profiling & Prophet baseline
│   ├── 02_silver_layer_exploration.ipynb      # Workshop #2 Silver Parquet exploration
│   ├── 03_data_quality_findings.ipynb         # Workshop #3 data-quality audit
│   └── 04_gold_preview.ipynb                  # Workshop #3 Gold Parquet preview
├── scripts/                                   # Helper & migration scripts
├── src/
│   ├── sources/                               # Open-Meteo client, NASA LHASA scraper, GLC loader
│   ├── preprocessing/                         # Cleaning, NLP normalizer, outliers, Parquet writer
│   ├── validation/                            # Bronze payload schema validator
│   └── gold/
│       └── transformations.py                 # PySpark Gold KPIs & LRI storytelling aggregations
├── tests/                                     # Automated unit & integration tests
├── workshop_1/                                # Workshop #1 deliverables (mapping, samples, report)
├── workshop_2/                                # Workshop #2 deliverables (report PDF & screenshots)
├── workshop_3/                                # Workshop #3 deliverables (LaTeX report & figures)
├── workshop_4/                                # Workshop #4 deliverables (report PDF & screenshots)
├── docker-compose.yml                         # Airflow + PostgreSQL containerized stack
├── Dockerfile                                 # Custom Airflow 2.9.2 + OpenJDK 17 + PySpark 3.5.3 image
└── requirements.txt                           # Python dependencies (pipeline + Dash/Plotly)
```

---

## End-to-End Reproducibility Guide (From a Clean Clone)

### Prerequisites
- **Docker Desktop** (running)
- **Python 3.11+** (to run the Plotly Dash applications locally)

### Step 1: Clone the Repository & Install Python Dependencies
```bash
git clone https://github.com/saritxm/landslide-monitoring-colombia.git
cd landslide-monitoring-colombia

# Create and activate a virtual environment (Windows PowerShell)
python -m venv .venv
.venv\Scripts\activate

# Install all pipeline and dashboard dependencies
pip install -r requirements.txt
```

### Step 2: Build and Start the Docker Airflow Stack
```bash
docker compose build
docker compose up -d
```
Verify that `postgres`, `airflow-webserver`, `airflow-scheduler`, and `airflow-triggerer` are healthy (`docker compose ps`).

### Step 3: Trigger the Medallion Pipeline in Airflow UI
1. Open **`http://localhost:8080`** and sign in with **`admin` / `admin`**.
2. Unpause all DAGs if paused, then trigger them in sequence:
   1. **`bronze_ingestion_dag`** (`▶ Trigger DAG`): Fetches Open-Meteo forecasts, scrapes NASA LHASA rasters, validates payloads, and writes raw JSON files plus `_READY.flag` to `datalake_bronze/`.
   2. **`silver_processing_dag`**: Automatically triggered via `FileSensor` when `_READY.flag` appears; cleans and writes standardized Parquet files into `datalake_silver/<topic>/`. *(Optional: run `bronze_historical_backfill_dag` with `start_year=2016`, `end_year=2025` to refresh the ERA5 baseline).*
   3. **`gold_processing_dag`** (`▶ Trigger DAG`): Runs PySpark in `local[*]` mode across all Silver folders and writes `governance_YYYYMMDD_HHMMSS.parquet` and `storytelling_YYYYMMDD_HHMMSS.parquet` into `datalake_gold/`.

*(Alternatively, outside Airflow with Java 17+ installed, you can generate Gold files directly via `python -m src.gold.transformations`).*

### Step 4: Launch Both Interactive Dashboards (Plotly Dash)
Open **two separate terminals** in the repository root (with `.venv` activated) and start each independent application:

```bash
# Terminal 1 — Data Governance & Quality Observatory (Technical Audience · US-06)
python dashboard/governance_app.py
```
- Accessible at **`http://localhost:8050`**
- Displays KPI summary cards (total records, null rates, duplicate reduction, schema compliance), null rate bar chart by severity, daily volume over time, outlier/quality flag rates, SLA compliance chart, and an interactive threshold table. Includes auto-refresh (60s) and a **Refresh Gold Data** button.

```bash
# Terminal 2 — Andean Landslide Risk Monitor (Functional User: UNGRD Analyst · US-01 to US-04)
python dashboard/storytelling_app.py
```
- Accessible at **`http://localhost:8051`**
- Displays the operational situation summary card, interactive filters (Department, Alert Level, Focus Station), semantic risk distribution donut (`LOW`=green, `MEDIUM`=amber, `HIGH`=red), interactive spatial risk map with historical landslides within 30 km, hourly rainfall trend, daily rainfall vs. 10-year historical P90, multi-source signal comparison (Open-Meteo API vs. NASA LHASA scraping), top narrative keywords colored by mean fatalities, and historical landslide activity/triggers.

---

## Automated Tests

```bash
pytest tests/ -v
```

## Data Attribution

- **NASA Global Landslide Catalog (GLC)** & **NASA LHASA Near-Real-Time Hazard Rasters** (Public Domain / NASA Open Data).
- **Weather Data by Open-Meteo.com** (licensed under CC BY 4.0).
