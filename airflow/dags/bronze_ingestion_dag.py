"""Bronze Ingestion DAG for Landslide Monitoring in Colombia.

Extracts data from both the Open-Meteo REST API and the NASA LHASA portal
(web scraping via BeautifulSoup), validates schemas, and persists raw JSON
files in datalake_bronze/ using datetime nomenclature.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

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
    tags=["bronze", "ingestion", "landslide_monitoring"],
    doc_md=__doc__,
)
def bronze_ingestion_pipeline():
    """Defines the Bronze ingestion pipeline tasks."""

    @task()
    def extract_weather_api() -> Dict[str, Any]:
        """Task 1: Query Open-Meteo Forecast & Recent API for Colombian stations."""
        from src.sources.api_client import extract_openmeteo_data

        logger.info("Starting Open-Meteo API extraction...")
        payload = extract_openmeteo_data()
        logger.info(
            "Extracted %d station forecasts from Open-Meteo.",
            payload["metadata"]["locations_count"],
        )
        return payload

    @task()
    def extract_lhasa_scraper() -> Dict[str, Any]:
        """Task 2: Scrape NASA LHASA NRT hazard catalog using BeautifulSoup."""
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
    def extract_landslide_catalog() -> Dict[str, Any]:
        """Task 3: Ingest batch of NASA Global Landslide Catalog events."""
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
        weather_payload: Dict[str, Any],
        lhasa_payload: Dict[str, Any],
        glc_payload: Dict[str, Any],
    ) -> List[str]:
        """Task 4: Validate Bronze schemas and persist raw JSON files."""
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
            is_valid, errors = validate_bronze_payload(payload, source_id)
            if not is_valid:
                logger.error("Validation failed for %s: %s", source_id, errors)

            filename = f"{source_id}_{ts_str}.json"
            filepath = target_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)

            logger.info("Saved Bronze JSON file: %s (%d bytes)", filepath, filepath.stat().st_size)
            persisted_files.append(str(filepath))

        return persisted_files

    # Orchestrate task execution graph
    weather = extract_weather_api()
    lhasa = extract_lhasa_scraper()
    glc = extract_landslide_catalog()
    validate_and_persist(weather, lhasa, glc)


bronze_dag = bronze_ingestion_pipeline()
