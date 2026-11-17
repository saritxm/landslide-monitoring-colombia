"""NASA Global Landslide Catalog (GLC) connector for Bronze Ingestion."""

from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)

SAMPLE_CSV_PATH = Path("workshop_1/samples/nasa_glc_20261107_045852.csv")


def extract_glc_data(csv_path: Optional[Path] = None, colombia_only: bool = True) -> Dict[str, Any]:
    """Loads GLC landslide records and formats them as a Bronze JSON envelope.

    Args:
        csv_path: Path to the CSV export. Defaults to the committed sample CSV.
        colombia_only: If True, filters records by Colombian bounding box.

    Returns:
        Bronze payload dictionary.
    """
    path = csv_path or SAMPLE_CSV_PATH
    if not path.exists():
        logger.warning("GLC sample file not found at %s", path)
        data_records = []
    else:
        df = pd.read_csv(path, low_memory=False)
        if colombia_only:
            # Colombia bbox: lat [-4.3, 13.5], lon [-79.1, -66.8]
            mask = (
                (df["latitude"] >= -4.3)
                & (df["latitude"] <= 13.5)
                & (df["longitude"] >= -79.1)
                & (df["longitude"] <= -66.8)
            )
            df = df[mask]
        data_records = df.to_dict(orient="records")

    query_time = datetime.now(timezone.utc)
    payload_bytes = json.dumps(data_records, default=str, sort_keys=True).encode("utf-8")
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()

    return {
        "metadata": {
            "source_id": "nasa_glc",
            "source_name": "NASA Global Landslide Catalog (GLC)",
            "extraction_timestamp_utc": query_time.isoformat(),
            "record_count": len(data_records),
            "payload_sha256": payload_hash,
        },
        "data": data_records,
    }
