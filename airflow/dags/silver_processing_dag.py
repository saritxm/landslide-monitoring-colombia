"""Silver Processing DAG for Landslide Monitoring in Colombia.

Triggered via FileSensor upon arrival of new JSON files in datalake_bronze/.
Executes schema validation, null handling, NLP text processing, outlier detection,
deduplication, and persists standardized Parquet files in datalake_silver/.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

logger = logging.getLogger(__name__)

# Directory paths: compatible with Docker environment and host repo
BRONZE_DIR = Path("/opt/airflow/datalake_bronze")
if not BRONZE_DIR.exists():
    BRONZE_DIR = Path("datalake_bronze")

SILVER_DIR = Path("/opt/airflow/datalake_silver")
if not SILVER_DIR.exists():
    SILVER_DIR = Path("datalake_silver")


@dag(
    dag_id="silver_processing_dag",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["silver", "transformation", "parquet", "landslide_monitoring"],
    doc_md=__doc__,
)
def silver_processing_pipeline():
    """Defines the Silver processing pipeline triggered by FileSensor."""

    # FileSensor watching the Bronze directory
    sensor_detect_bronze_file = FileSensor(
        task_id="sensor_detect_bronze_file",
        filepath=".",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=300,
        mode="poke",
        soft_fail=False,
    )

    @task()
    def process_bronze_to_silver() -> List[Dict[str, Any]]:
        """Processes any new/unprocessed Bronze JSON files into Silver Parquet."""
        from src.preprocessing.cleaning import (
            transform_glc_to_silver,
            transform_lhasa_to_silver,
            transform_weather_to_silver,
        )
        from src.preprocessing.parquet_writer import write_dataframe_to_silver_parquet

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = SILVER_DIR / ".processed_manifest.json"

        # Load processed files manifest for idempotency
        processed_files = set()
        if manifest_path.exists():
            try:
                processed_files = set(json.loads(manifest_path.read_text(encoding="utf-8")))
            except Exception:
                processed_files = set()

        json_files = sorted(BRONZE_DIR.glob("*.json"))
        logger.info("Found %d JSON files in %s", len(json_files), BRONZE_DIR)

        processed_outputs = []

        for json_path in json_files:
            file_name = json_path.name
            if file_name in processed_files:
                logger.info("File %s already processed. Skipping.", file_name)
                continue

            logger.info("Processing Bronze file: %s", file_name)
            with open(json_path, "r", encoding="utf-8") as f:
                payload = json.load(f)

            meta = payload.get("metadata", {})
            source_id = meta.get("source_id", "")

            # Dispatch to appropriate transformation pipeline
            if "openmeteo" in source_id or "openmeteo" in file_name:
                df, stats = transform_weather_to_silver(payload, file_name)
                topic = "rainfall"
            elif "lhasa" in source_id or "lhasa" in file_name:
                df, stats = transform_lhasa_to_silver(payload, file_name)
                topic = "lhasa_hazard"
            elif "glc" in source_id or "glc" in file_name:
                df, stats = transform_glc_to_silver(payload, file_name)
                topic = "landslides"
            else:
                logger.warning("Unknown source for %s, skipping.", file_name)
                continue

            if df.empty:
                logger.warning("Transformation resulted in empty DataFrame for %s", file_name)
                continue

            # Persist to Silver Parquet
            parquet_path = write_dataframe_to_silver_parquet(
                df=df,
                topic=topic,
                output_dir=SILVER_DIR,
                custom_metadata={
                    "bronze_source_file": file_name,
                    "source_id": source_id,
                    "rows_count": str(len(df)),
                },
            )

            processed_files.add(file_name)
            processed_outputs.append({
                "bronze_file": file_name,
                "parquet_file": str(parquet_path),
                "topic": topic,
                "row_count": len(df),
                "stats": stats,
            })

        # Save updated manifest
        manifest_path.write_text(json.dumps(sorted(list(processed_files)), indent=2), encoding="utf-8")
        logger.info("Finished processing. Generated %d Silver files.", len(processed_outputs))
        return processed_outputs

    # Sensor triggers processing
    sensor_detect_bronze_file >> process_bronze_to_silver()


silver_dag = silver_processing_pipeline()
