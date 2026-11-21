"""Gold Processing DAG (Workshop #3): Silver Parquet -> Gold summaries with PySpark.

Reads every Silver topic folder with spark.read.parquet("datalake_silver/<topic>/") in
local mode and writes two Parquet files to datalake_gold/:
  governance_YYYYMMDD_HHMMSS.parquet   (data-quality KPIs, user story US-06)
  storytelling_YYYYMMDD_HHMMSS.parquet (dashboard aggregations, US-01..US-04)
"""

from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

SILVER_DIR = Path("/opt/airflow/datalake_silver")
if not SILVER_DIR.exists():
    SILVER_DIR = Path("datalake_silver")
TOPICS = ["rainfall", "rainfall_historical", "landslides", "lhasa_hazard"]


@dag(
    dag_id="gold_processing_dag",
    schedule="30 0 * * *",          # daily at 00:30 UTC (19:30 Colombia), after the hourly Silver runs
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=30)},
    tags=["gold", "pyspark", "landslide_monitoring"],
    doc_md=__doc__,
)
def gold_processing_pipeline():

    @task()
    def check_silver_inputs() -> dict:
        """Fails early if a Silver topic folder is missing or empty."""
        counts = {t: len(list((SILVER_DIR / t).glob("*.parquet"))) for t in TOPICS}
        missing = [t for t, n in counts.items() if n == 0]
        if missing:
            raise FileNotFoundError(f"Silver topics without Parquet files: {missing}")
        logger.info("Silver files per topic: %s", counts)
        return counts

    @task()
    def build_gold(counts: dict, data_interval_end=None, logical_date=None, dag_run=None) -> dict:
        """Runs the PySpark job (local[*]) and returns the two Gold file paths."""
        from src.gold.transformations import run

        manual = dag_run is not None and dag_run.run_type == "manual"
        stamp = (logical_date if manual else data_interval_end) or datetime.now(timezone.utc)
        return run(stamp.astimezone(timezone.utc).replace(microsecond=0))

    @task()
    def validate_gold(paths: dict) -> dict:
        """Checks that both files exist, are readable and are not empty."""
        import pyarrow.parquet as pq

        summary = {}
        for name, path in paths.items():
            meta = pq.read_metadata(path)
            if meta.num_rows == 0:
                raise ValueError(f"{name} Gold file is empty: {path}")
            summary[name] = {"path": path, "rows": meta.num_rows, "columns": meta.num_columns}
        logger.info("Gold outputs: %s", summary)
        return summary

    validate_gold(build_gold(check_silver_inputs()))


gold_dag = gold_processing_pipeline()
