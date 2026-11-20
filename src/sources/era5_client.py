"""Open-Meteo ERA5 Historical Archive Client (Daily Precipitation).

Extracts daily accumulated precipitation (precipitation_sum) for Colombian
monitoring stations to compute historical percentiles in the Gold layer.
"""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.sources.api_client import MONITORING_LOCATIONS

logger = logging.getLogger(__name__)

ERA5_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
COLOMBIA_STATIONS: List[Dict[str, Any]] = MONITORING_LOCATIONS


def _build_retry_session(retries: int = 3, backoff_factor: float = 2.0) -> requests.Session:
    """Creates an HTTP session with exponential backoff for 429/5xx responses."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_era5_historical(
    start_date: str,
    end_date: str,
    stations: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 45,
) -> Dict[str, Any]:
    """Queries Open-Meteo Archive API in multi-station mode for daily precipitation_sum."""
    target_stations = stations or COLOMBIA_STATIONS
    params = {
        "latitude": ",".join(str(s["latitude"]) for s in target_stations),
        "longitude": ",".join(str(s["longitude"]) for s in target_stations),
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": "UTC",
    }

    session = _build_retry_session()
    logger.info(
        "Requesting ERA5 Archive (%s to %s) for %d stations...",
        start_date,
        end_date,
        len(target_stations),
    )
    resp = session.get(ERA5_ARCHIVE_URL, params=params, timeout=timeout)
    resp.raise_for_status()

    raw_json = resp.json()
    # Open-Meteo returns a single dict for 1 location, or a list of dicts for multiple locations
    responses = raw_json if isinstance(raw_json, list) else [raw_json]

    if len(responses) != len(target_stations):
        raise ValueError(
            f"Station count mismatch: requested {len(target_stations)}, got {len(responses)}"
        )

    records = []
    for st, loc_resp in zip(target_stations, responses):
        records.append({
            "location_name": st["name"],
            "department": st["department"],
            "requested_latitude": st["latitude"],
            "requested_longitude": st["longitude"],
            "returned_latitude": loc_resp.get("latitude"),
            "returned_longitude": loc_resp.get("longitude"),
            "elevation_m": loc_resp.get("elevation"),
            "daily": loc_resp.get("daily", {}),
        })

    return {
        "metadata": {
            "source_id": "openmeteo_era5",
            "api_url": ERA5_ARCHIVE_URL,
            "start_date": start_date,
            "end_date": end_date,
            "locations_count": len(records),
            "rate_limit_cost": resp.headers.get("X-RateLimit-Cost", "unknown"),
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        },
        "data": records,
    }
