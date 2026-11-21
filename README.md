# Landslide Monitoring and Risk — Colombia

Data Analysis Programming, Semester 2026-I — Universidad Distrital Francisco José de Caldas  
Team 7 — Professor: Eng. Carlos Andrés Sierra, M.Sc.

## Hazard & Domain Context

Landslides (mass movements) in Colombia are predominantly triggered by intense or prolonged rainfall on steep slopes across the Andean and Pacific regions. This system integrates historical landslide inventories with historical and real-time precipitation models and satellite hazard rasters to calculate a regional **Landslide Risk Indicator**.

- **Geographic scope:** Colombia (Bounding Box: lat −4.3 to 13.5, lon −79.1 to −66.8)
- **Historical window:** Landslides 2007–2017 (NASA GLC catalog); rainfall 2007–present (ERA5 reanalysis)
- **Incremental window:** Open-Meteo real-time forecast and NASA LHASA NRT hazard rasters

## Team 7 Members

| Student ID | Name |
|---|---|
| 20231020072 | Samuel Alberto Leal Saenz |
| 20231020100 | Cristian Andres Gámez Nuñez |
| 20231020116 | Sara Nicolle Martínez Moreno |

---

## Technical Architecture (Medallion Lakehouse)

The project implements a modern Medallion Architecture orchestrated via Apache Airflow running in Docker Compose:

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
         (Schema Enforcement · Null Flagging (no invented values) · NLP Normalization · IQR/Z-score Outliers)
                                  │
                                  ▼
                   datalake_silver/topic_YYYYMMDD_HHMMSS.parquet
                                  │
                                  ▼
                   datalake_gold/ (Planned - Workshop #3 PySpark)
```

1. **Bronze Layer (`datalake_bronze/`):** Immutable landing zone storing raw JSON payloads with datetime nomenclature (`source_YYYYMMDD_HHMMSS.json`).
2. **Silver Layer (`datalake_silver/`):** Cleaned, typed, and standardized Parquet tables (`topic_YYYYMMDD_HHMMSS.parquet`) with Snappy compression, bilingual NLP text normalization on landslide narratives, statistical outlier flags, and end-to-end data lineage.
3. **Gold Layer (`datalake_gold/`):** Planned for Workshop #3 (PySpark spatial-temporal joins on a 0.25° grid to compute the Landslide Risk Indicator).

---

## Repository Structure

```
├── airflow/
│   └── dags/
│       ├── bronze_ingestion_dag.py        # TaskFlow API ingestion DAG (hourly)
│       ├── bronze_historical_backfill_dag.py  # Manual ERA5 daily-rainfall backfill by year
│       └── silver_processing_dag.py        # FileSensor-triggered Silver transformation DAG
├── config/
│   └── sources.yaml                       # Formal declarative source contract
├── datalake_bronze/                       # Bronze raw JSON landing files
├── datalake_silver/                       # Silver standardized Parquet files
├── datalake_gold/                         # Gold analytical layer (Workshop #3)
├── notebooks/
│   ├── 01_source_profiling.ipynb          # Workshop #1 data profiling & Prophet experiment
│   └── 02_silver_layer_exploration.ipynb  # Workshop #2 Parquet exploration & data audits
├── scripts/
│   ├── generate_report_figures.py         # Figures for the Workshop #2 report (from Silver data)
│   └── create_exploration_notebook.py     # Notebook generator
├── src/
│   ├── sources/                           # API client (Open-Meteo), Scraper (BS4), GLC loader
│   ├── preprocessing/                     # Cleaning, NLP normalizer, Outliers, Parquet writer
│   └── validation/                        # Bronze payload schema validator
├── tests/
│   ├── test_workshop1.py                  # Workshop #1 automated tests
│   └── test_workshop2.py                  # Workshop #2 automated tests (ingestion, NLP, Parquet)
├── workshop_1/                            # Workshop #1 deliverables (samples, mapping)
├── workshop_2/                            # Workshop #2 deliverables (report PDF, HTML)
│   ├── Workshop2_Report_Team7.pdf         # Workshop #2 technical report (revised)
│   ├── img/                               # Airflow UI screenshots used in the report
│   └── report/                            # Report source (HTML) and builder script
├── docker-compose.yml                     # Airflow containerized stack definition
├── Dockerfile                             # Custom Airflow image with Python dependencies
└── requirements.txt                       # Project Python requirements
```

---

## Quickstart & Docker Setup

### Prerequisites
- Docker Desktop (active and running)
- Python 3.11+ (for local scripts and Jupyter)

### 1. Launch Airflow in Docker
```bash
docker compose up -d
```
Verify all containers are healthy:
```bash
docker compose ps
```
Services started:
- `postgres`: PostgreSQL 15 metadata database.
- `airflow-init`: Automatic DB migration and admin user creation.
- `airflow-webserver`: Web UI accessible at `http://localhost:8080` (`admin` / `admin`).
- `airflow-scheduler`: Task scheduler and LocalExecutor runner.
- `airflow-triggerer`: Async sensor/trigger handler.

### 2. Run the Airflow DAGs
Open `http://localhost:8080`:
1. Unpause `bronze_ingestion_dag` and trigger a run. It queries Open-Meteo, scrapes NASA LHASA, validates payloads, and writes JSON files to `datalake_bronze/`.
2. `silver_processing_dag` waits (FileSensor) for the `_READY.flag` written by the Bronze DAGs, processes every Bronze file not yet in `datalake_silver/.processed_manifest.json`, and writes one Parquet file per topic to `datalake_silver/`.
3. Optional: trigger `bronze_historical_backfill_dag` (parameters `start_year`, `end_year`) to download ERA5 daily rainfall by year; the Silver DAG then writes `rainfall_historical_*.parquet`.

Data-quality policy: missing values are flagged and kept null, never replaced by invented values (no 0 mm of rain, no 0 deaths, no assumed country). Rows with a missing or physically impossible precipitation value are rejected and counted. If a source cannot be reached, the task fails and Airflow retries it; there is no offline fallback that would write made-up data.

### 3. Local Environment & Automated Testing
```bash
# Activate virtual environment
.venv\Scripts\activate

# Install requirements
pip install -r requirements.txt

# Run all test suites
pytest tests/ -v
```

### 4. Explore Silver Parquet Files
Open `notebooks/02_silver_layer_exploration.ipynb` in Jupyter or VS Code to inspect the schema, NLP token frequencies, and outlier distributions.
