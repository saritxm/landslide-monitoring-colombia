"""Bronze Ingestion DAG for Landslide Monitoring in Colombia.

Extracts data from both the Open-Meteo REST API and the NASA LHASA portal
(web scraping via BeautifulSoup), validates schemas, and persists raw JSON
files in datalake_bronze/ using datetime nomenclature.
"""

from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# Base datalake path: works both inside Docker container (/opt/airflow/datalake_bronze)
# and when run from the local host repository
BRONZE_DIR = Path("/opt/airflow/datalake_bronze")
if not BRONZE_DIR.exists():
    BRONZE_DIR = Path("datalake_bronze")


@dag(
    dag_id="bronze_ingestion_dag",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    # Sources have no offline fallback: a failed or hung request is retried by Airflow
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2),
                  "execution_timeout": timedelta(minutes=10)},
    tags=["bronze", "ingestion", "landslide_monitoring"],
    doc_md=__doc__,
)
def bronze_ingestion_pipeline():
    """Defines the Bronze ingestion pipeline tasks."""

    @task()
    def extract_weather_api() -> Optional[Dict[str, Any]]:
        """Task 1: Query Open-Meteo Forecast & Recent API for Colombian stations (Hourly cadence)."""
        from src.sources.api_client import extract_openmeteo_data

        logger.info("Starting Open-Meteo API extraction...")
        payload = extract_openmeteo_data()
        logger.info(
            "Extracted %d station forecasts from Open-Meteo.",
            payload["metadata"]["locations_count"],
        )
        return payload

    @task()
    def extract_lhasa_scraper() -> Optional[Dict[str, Any]]:
        """Task 2: Scrape NASA LHASA NRT hazard catalog (Daily cadence)."""
        today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        existing = list(BRONZE_DIR.glob(f"nasa_lhasa_{today_str}_*.json"))
        if existing:
            logger.info("LHASA already ingested for today (%s). Skipping per daily cadence.", today_str)
            return None

        from src.sources.scraper import extract_lhasa_data

        logger.info("Starting NASA LHASA web scraping...")
        payload = extract_lhasa_data()
        logger.info(
            "Detected %d hazard rasters, sampled %d items.",
            payload["metadata"]["total_files_detected"],
            payload["metadata"]["extracted_sample_count"],
        )
        return payload

    @task()
    def extract_landslide_catalog() -> Optional[Dict[str, Any]]:
        """Task 3: Ingest batch of NASA Global Landslide Catalog events (Monthly cadence)."""
        month_str = datetime.now(timezone.utc).strftime("%Y%m")
        existing = list(BRONZE_DIR.glob(f"nasa_glc_{month_str}*.json"))
        if existing:
            logger.info("GLC catalog already ingested for this month (%s). Skipping per monthly cadence.", month_str)
            return None

        from src.sources.glc_client import extract_glc_data

        logger.info("Starting NASA GLC catalog extraction...")
        payload = extract_glc_data()
        logger.info(
            "Extracted %d historical landslide records for Colombia.",
            payload["metadata"]["record_count"],
        )
        return payload

    @task()
    def validate_and_persist(
        weather_payload: Optional[Dict[str, Any]],
        lhasa_payload: Optional[Dict[str, Any]],
        glc_payload: Optional[Dict[str, Any]],
    ) -> List[str]:
        """Task 4: Validate Bronze schemas, persist raw JSON files, and emit _READY.flag."""
        from src.validation.schema import validate_bronze_payload

        now = datetime.now(timezone.utc)
        ts_str = now.strftime("%Y%m%d_%H%M%S")
        target_dir = BRONZE_DIR
        target_dir.mkdir(parents=True, exist_ok=True)

        sources_to_save = [
            ("openmeteo_forecast", weather_payload),
            ("nasa_lhasa", lhasa_payload),
            ("nasa_glc", glc_payload),
        ]

        persisted_files = []
        for source_id, payload in sources_to_save:
            if payload is None:
                logger.info("Skipping %s (up-to-date per ingestion cadence).", source_id)
                continue

            # Skip a payload identical to the last persisted one of the same source
            # (e.g. the Open-Meteo model has not been updated since the previous hour,
            # or the static GLC snapshot is re-read): it would only duplicate Bronze/Silver.
            new_hash = payload.get("metadata", {}).get("payload_sha256")
            previous = sorted(target_dir.glob(f"{source_id}_*.json"))
            if new_hash and previous:
                try:
                    last_hash = json.loads(previous[-1].read_text(encoding="utf-8"))["metadata"].get("payload_sha256")
                except Exception:
                    last_hash = None
                if last_hash == new_hash:
                    logger.info("Skipping %s: content unchanged since %s.", source_id, previous[-1].name)
                    continue

            is_valid, errors = validate_bronze_payload(payload, source_id)
            if not is_valid:
                logger.error("Validation failed for %s: %s. Skipping persistence.", source_id, errors)
                continue

            filename = f"{source_id}_{ts_str}.json"
            filepath = target_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)

            logger.info("Saved Bronze JSON file: %s (%d bytes)", filepath, filepath.stat().st_size)
            persisted_files.append(str(filepath))

        if persisted_files:
            flag_path = target_dir / "_READY.flag"
            flag_path.write_text(
                json.dumps({"timestamp_utc": ts_str, "files": persisted_files}, indent=2),
                encoding="utf-8",
            )
            logger.info("Emitted _READY.flag with %d new Bronze files.", len(persisted_files))

        return persisted_files

    # Orchestrate task execution graph
    weather = extract_weather_api()
    lhasa = extract_lhasa_scraper()
    glc = extract_landslide_catalog()
    validate_and_persist(weather, lhasa, glc)


bronze_dag = bronze_ingestion_pipeline()
