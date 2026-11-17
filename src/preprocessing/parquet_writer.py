"""Parquet Writer with PyArrow Schema Enforcement and Lineage Metadata."""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

SILVER_DIR = Path("datalake_silver")


def write_dataframe_to_silver_parquet(
    df: pd.DataFrame,
    topic: str,
    output_dir: Optional[Path] = None,
    custom_metadata: Optional[Dict[str, Any]] = None,
    timestamp: Optional[datetime] = None,
) -> Path:
    """Writes a Pandas DataFrame to a Parquet file with timestamped naming and metadata.

    Filename format: {topic}_{YYYYMMDD_HHMMSS}.parquet

    Args:
        df: The DataFrame to persist.
        topic: Topic or entity name (e.g. 'rainfall', 'landslides', 'lhasa_hazard').
        output_dir: Target directory (defaults to datalake_silver).
        custom_metadata: Dict with metadata to embed in Parquet schema.
        timestamp: Timestamp for filename (defaults to current UTC).

    Returns:
        Path of written Parquet file.
    """
    dest_dir = output_dir or SILVER_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    ts = timestamp or datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    filename = f"{topic}_{ts_str}.parquet"
    target_path = dest_dir / filename

    # Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Embed custom metadata
    meta = custom_metadata or {}
    meta["topic"] = topic
    meta["created_at_utc"] = ts.isoformat()
    meta["row_count"] = str(len(df))
    meta["column_count"] = str(len(df.columns))

    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, **{k.encode("utf-8"): str(v).encode("utf-8") for k, v in meta.items()}}
    table = table.replace_schema_metadata(new_meta)

    # Write Parquet with Snappy compression
    pq.write_table(
        table,
        target_path,
        compression="snappy",
        version="2.6",
    )
    logger.info("Persisted Silver Parquet file: %s (%d rows)", target_path, len(df))
    return target_path
