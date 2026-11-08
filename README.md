# Landslide Monitoring and Risk — Colombia

Data Analysis Programming, Semester 2026-II — Universidad Distrital Francisco José de Caldas
Team 7 — Professor: Eng. Carlos Andrés Sierra, M.Sc.

## Hazard

Landslides are downslope movements of soil, rock, and debris. In Colombia, most of them are
triggered by intense or prolonged rainfall on the steep slopes of the Andean region. This
system integrates a historical landslide inventory with historical and current rainfall to
identify areas where present conditions resemble those that preceded past landslides.

The output is a **Landslide Risk Indicator per region**. It is a monitoring and correlation
aid, **not a prediction** of landslides.

- **Geographic scope:** Colombia (approx. lat −4.3 to 13.5, lon −79.1 to −66.8)
- **Historical window:** landslides 2007–2017 (the NASA catalog ends in September 2017); rainfall 2007–present

## Team

| Student ID | Name |
|---|---|
| 20231020072 | Samuel Leal |
| 20231020100 | Cristian Gámez |
| 20231020116 | Sara Martínez |

## Data sources

Validated with real queries on 2026-11-07 (evidence in `workshop_1/samples/`):

| Role | Source | Format | Key needed |
|---|---|---|---|
| Historical 1 | NASA Global Landslide Catalog | CSV (bulk file) | No |
| Historical 2 | Open-Meteo Historical Weather API (ERA5) | JSON REST | No |
| Incremental 1 | Open-Meteo Forecast API | JSON REST | No |
| Incremental 2 | NASA LHASA landslide nowcast (NRT v2) | GeoTIFF raster (daily) | No |

Changes detected in the sources are recorded in `docs/source_status_log.md`.

Licenses and attribution requirements are recorded in `config/sources.yaml`.

## Repository structure

```
src/              ingestion package (sources, normalization, storage)
ingest.py         command-line entry point (stub in Workshop #1)
config/           sources.yaml: one entry per source
data/raw/         raw landing zone (git-ignored except small samples)
sql/              schema definitions
dashboard/        Plotly Dash application
tests/            tests; tests/fixtures/ holds recorded sample responses
notebooks/        profiling and analysis notebooks
docs/             documentation and diagrams
workshop_1/       Workshop #1 deliverables (samples, mapping)
```

## Workshop #1 deliverables

- `notebooks/01_source_profiling.ipynb` — source profiling, executed with outputs
- `workshop_1/samples/` — raw responses kept as received (large files: `python workshop_1/download_samples.py`)
- `workshop_1/mapping.md` — canonical mapping, domain tables, ingestion contracts and ER diagram
- `docs/er_diagram.png`, `docs/source_status_log.md`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

## Usage (contract; implemented from Workshop #2)

```bash
python ingest.py --mode historical --start 2007-01-01 --end 2026-11-06
python ingest.py --mode incremental
python ingest.py --mode status
```
