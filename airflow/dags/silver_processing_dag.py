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

    # FileSensor watching the Bronze directory for ready batch flag
    sensor_detect_bronze_file = FileSensor(
        task_id="sensor_detect_bronze_file",
        filepath="_READY.flag",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="reschedule",
        soft_fail=True,
    )

    @task()
    def process_bronze_to_silver() -> List[Dict[str, Any]]:
        """Processes any new/unprocessed Bronze JSON files into Silver Parquet."""
        from src.preprocessing.cleaning import (
            transform_era5_to_silver,
            transform_glc_to_silver,
            transform_lhasa_to_silver,
            transform_weather_to_silver,
        )
        from src.preprocessing.parquet_writer import write_dataframe_to_silver_parquet
        import pandas as pd

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        manifest_path = SILVER_DIR / ".processed_manifest.json"

        # Load processed files manifest for idempotency
        processed_files = set()
        if manifest_path.exists():
            try:
                processed_files = set(json.loads(manifest_path.read_text(encoding="utf-8")))
            except Exception:
                processed_files = set()

        # Consume the trigger flag BEFORE listing the files: a Bronze run that finishes
        # while this task is running writes a new flag, which triggers the next Silver run
        # instead of being deleted at the end of this one.
        (BRONZE_DIR / "_READY.flag").unlink(missing_ok=True)

        json_files = sorted(BRONZE_DIR.glob("*.json"))
        logger.info("Found %d JSON files in %s", len(json_files), BRONZE_DIR)

        processed_outputs = []
        # One Parquet file per topic and run. (Writing one file per Bronze file, named to
        # the second, let two files of the same topic processed within the same second
        # overwrite each other.) Every row keeps its Bronze file in _bronze_source_file.
        batches: Dict[str, Dict[str, Any]] = {}

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
            if "era5" in source_id or "era5" in file_name:
                df, stats = transform_era5_to_silver(payload, file_name)
                topic = "rainfall_historical"
            elif "openmeteo" in source_id or "openmeteo" in file_name:
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

            processed_files.add(file_name)
            if df.empty:
                logger.warning("Transformation resulted in empty DataFrame for %s", file_name)
                continue

            batch = batches.setdefault(topic, {"frames": [], "files": [], "sources": set(), "stats": {}})
            batch["frames"].append(df)
            batch["files"].append(file_name)
            batch["sources"].add(source_id)
            batch["stats"][file_name] = stats

        for topic, batch in batches.items():
            df = pd.concat(batch["frames"], ignore_index=True)
            for col in ("station_name", "department"):
                if col in df.columns:
                    df[col] = df[col].astype("category")
            parquet_path = write_dataframe_to_silver_parquet(
                df=df,
                topic=topic,
                output_dir=SILVER_DIR,
                custom_metadata={
                    "bronze_source_files": json.dumps(batch["files"]),
                    "source_id": ",".join(sorted(batch["sources"])),
                    "rows_count": str(len(df)),
                },
            )
            processed_outputs.append({
                "bronze_files": batch["files"],
                "parquet_file": str(parquet_path),
                "topic": topic,
                "row_count": len(df),
                "stats": batch["stats"],
            })

        # Save updated manifest and clear trigger flag
        manifest_path.write_text(json.dumps(sorted(list(processed_files)), indent=2), encoding="utf-8")
        logger.info("Finished processing. Generated %d Silver files.", len(processed_outputs))
        return processed_outputs

    # Sensor triggers processing
    sensor_detect_bronze_file >> process_bronze_to_silver()


silver_dag = silver_processing_pipeline()
