"""Historical Backfill DAG for Open-Meteo ERA5 Daily Precipitation.

Manually triggered (schedule=None) to ingest multi-year daily precipitation
partitioned by calendar year into datalake_bronze/, then emits _READY.flag
so silver_processing_dag processes the files automatically.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import time
from typing import List

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

BRONZE_DIR = Path("/opt/airflow/datalake_bronze")
if not BRONZE_DIR.exists():
    BRONZE_DIR = Path("datalake_bronze")


@dag(
    dag_id="bronze_historical_backfill_dag",
    schedule=None,
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    params={
        "start_year": Param(2016, type="integer", minimum=1980, maximum=2026),
        "end_year": Param(2025, type="integer", minimum=1980, maximum=2026),
        "force_overwrite": Param(False, type="boolean"),
    },
    tags=["bronze", "historical", "era5", "backfill"],
    doc_md=__doc__,
)
def bronze_historical_backfill_pipeline():
    """Backfills ERA5 daily precipitation partitioned by year."""

    @task()
    def ingest_era5_by_year(**context) -> List[str]:
        from src.sources.era5_client import extract_era5_historical

        params = context["params"]
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])
        force = bool(params["force_overwrite"])

        if start_year > end_year:
            raise ValueError(f"start_year ({start_year}) cannot exceed end_year ({end_year})")

        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        persisted_files: List[str] = []

        for year in range(start_year, end_year + 1):
            existing = list(BRONZE_DIR.glob(f"openmeteo_era5_{year}_*.json"))
            if existing and not force:
                logger.info("Year %d already ingested (%s). Skipping.", year, existing[0].name)
                continue

            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            payload = extract_era5_historical(start_date=start_date, end_date=end_date)

            filename = f"openmeteo_era5_{year}_{ts_str}.json"
            filepath = BRONZE_DIR / filename
            filepath.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info("Saved ERA5 Bronze partition: %s (%d bytes)", filepath.name, filepath.stat().st_size)
            persisted_files.append(str(filepath))

            # Small polite pause between batch requests
            time.sleep(1.5)

        return persisted_files

    @task()
    def emit_ready_flag(persisted_files: List[str]) -> str:
        if not persisted_files:
            logger.info("No new ERA5 partitions persisted; skipping _READY.flag.")
            return "no_new_files"

        flag_path = BRONZE_DIR / "_READY.flag"
        flag_payload = {
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            "source": "openmeteo_era5_backfill",
            "files": persisted_files,
        }
        flag_path.write_text(json.dumps(flag_payload, indent=2), encoding="utf-8")
        logger.info("Emitted _READY.flag for %d ERA5 partitions.", len(persisted_files))
        return str(flag_path)

    files = ingest_era5_by_year()
    emit_ready_flag(files)


era5_backfill_dag = bronze_historical_backfill_pipeline()
