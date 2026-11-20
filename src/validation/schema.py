"""Validation utilities for Bronze layer raw payloads."""

from datetime import datetime
import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

COLOMBIA_BBOX = {"min_lat": -4.3, "max_lat": 13.5, "min_lon": -79.1, "max_lon": -66.8}


def validate_bronze_payload(payload: Dict[str, Any], expected_source: str) -> Tuple[bool, List[str]]:
    """Validates raw bronze payload structure before persisting to datalake_bronze/.

    Args:
        payload: The JSON-serializable dictionary to validate.
        expected_source: Expected source_id (e.g. 'openmeteo_forecast', 'nasa_lhasa').

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    if not isinstance(payload, dict):
        return False, ["Payload must be a dictionary"]

    # Check top-level envelope
    if "metadata" not in payload:
        errors.append("Missing 'metadata' envelope")
    if "data" not in payload:
        errors.append("Missing 'data' field")

    if errors:
        return False, errors

    meta = payload["metadata"]
    source_id = meta.get("source_id")
    if source_id != expected_source:
        errors.append(f"Unexpected source_id '{source_id}', expected '{expected_source}'")

    data = payload["data"]
    if not isinstance(data, list):
        errors.append("Field 'data' must be a list of records")
    elif len(data) == 0:
        errors.append("Payload 'data' list is empty (0 records)")

    # Source-specific field validations
    if expected_source == "openmeteo_forecast" and isinstance(data, list):
        for idx, item in enumerate(data[:5]):  # Sample check first 5
            if "returned_latitude" not in item or "returned_longitude" not in item:
                errors.append(f"Item #{idx} missing returned latitude/longitude")
            lat = item.get("returned_latitude")
            lon = item.get("returned_longitude")
            if lat is not None and not (COLOMBIA_BBOX["min_lat"] - 1.0 <= lat <= COLOMBIA_BBOX["max_lat"] + 1.0):
                errors.append(f"Latitude {lat} out of Colombian bounds")
            if lon is not None and not (COLOMBIA_BBOX["min_lon"] - 1.0 <= lon <= COLOMBIA_BBOX["max_lon"] + 1.0):
                errors.append(f"Longitude {lon} out of Colombian bounds")

    elif expected_source == "nasa_lhasa" and isinstance(data, list):
        for idx, item in enumerate(data[:5]):
            if "file_name" not in item or "url" not in item:
                errors.append(f"Item #{idx} missing file_name or url")

    elif expected_source == "openmeteo_era5" and isinstance(data, list):
        for idx, item in enumerate(data[:5]):
            if "returned_latitude" not in item or "returned_longitude" not in item:
                errors.append(f"Item #{idx} missing returned latitude/longitude")
            if "daily" not in item:
                errors.append(f"Item #{idx} missing daily section")

    is_valid = len(errors) == 0
    if not is_valid:
        logger.warning("Bronze payload validation errors: %s", errors)
    return is_valid, errors
