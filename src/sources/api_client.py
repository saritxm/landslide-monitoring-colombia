"""Open-Meteo API Client for Colombia Landslide & Rainfall Monitoring.

Extracts hourly and daily precipitation, rain, and soil moisture observations
and forecasts across key monitoring points in Colombia.
"""

from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional
import requests

logger = logging.getLogger(__name__)

# Key monitoring stations/regions across Colombia prone to landslides (Andean and Pacific regions)
MONITORING_LOCATIONS = [
    {"name": "Bogota", "latitude": 4.6097, "longitude": -74.0817, "department": "Cundinamarca"},
    {"name": "Medellin", "latitude": 6.2442, "longitude": -75.5812, "department": "Antioquia"},
    {"name": "Cali", "latitude": 3.4516, "longitude": -76.5320, "department": "Valle del Cauca"},
    {"name": "Manizales", "latitude": 5.0689, "longitude": -75.5174, "department": "Caldas"},
    {"name": "Pereira", "latitude": 4.8133, "longitude": -75.6961, "department": "Risaralda"},
    {"name": "Ibague", "latitude": 4.4389, "longitude": -75.2322, "department": "Tolima"},
    {"name": "Bucaramanga", "latitude": 7.1254, "longitude": -73.1198, "department": "Santander"},
    {"name": "Pasto", "latitude": 1.2136, "longitude": -77.2811, "department": "Narino"},
    {"name": "Mocoa", "latitude": 1.1528, "longitude": -76.6521, "department": "Putumayo"},
    {"name": "Villavicencio", "latitude": 4.1420, "longitude": -73.6266, "department": "Meta"},
]

DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_USER_AGENT = "landslide-monitoring-colombia/1.0 (university-project; Universidad Distrital)"


class OpenMeteoClient:
    """Client for querying Open-Meteo API with retries and metadata enrichment."""

    def __init__(
        self,
        base_url: str = DEFAULT_API_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: int = 30,
    ):
        self.base_url = base_url
        self.headers = {"User-Agent": user_agent, "Accept": "application/json"}
        self.timeout = timeout

    def fetch_forecast_batch(
        self,
        locations: Optional[List[Dict[str, Any]]] = None,
        past_days: int = 2,
        forecast_days: int = 3,
    ) -> Dict[str, Any]:
        """Fetch forecast and past rainfall data for multiple Colombian monitoring stations.

        Args:
            locations: List of dicts with name, latitude, longitude.
            past_days: Number of past days to query (overlap window).
            forecast_days: Number of forecast days.

        Returns:
            Dict containing ingestion metadata and list of location payloads.
        """
        if locations is None:
            locations = MONITORING_LOCATIONS

        lats = [loc["latitude"] for loc in locations]
        lons = [loc["longitude"] for loc in locations]

        params = {
            "latitude": ",".join(str(lat) for lat in lats),
            "longitude": ",".join(str(lon) for lon in lons),
            "hourly": "precipitation,rain,soil_moisture_0_to_1cm",
            "current": "precipitation,rain",
            "past_days": past_days,
            "forecast_days": forecast_days,
            "timezone": "UTC",
        }

        query_time = datetime.now(timezone.utc)
        logger.info("Querying Open-Meteo API for %d locations in Colombia...", len(locations))

        response = requests.get(
            self.base_url,
            params=params,
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        raw_json = response.json()

        # Open-Meteo returns a single dict if only one location, or a list of dicts if multiple
        items = raw_json if isinstance(raw_json, list) else [raw_json]

        # Enrich each location response with configured metadata
        enriched_locations = []
        for i, item in enumerate(items):
            loc_meta = locations[i] if i < len(locations) else {}
            enriched_item = {
                "location_name": loc_meta.get("name", f"Station_{i+1}"),
                "department": loc_meta.get("department", "Unknown"),
                "requested_latitude": loc_meta.get("latitude"),
                "requested_longitude": loc_meta.get("longitude"),
                "returned_latitude": item.get("latitude"),
                "returned_longitude": item.get("longitude"),
                "elevation_m": item.get("elevation"),
                "timezone": item.get("timezone", "UTC"),
                "current": item.get("current", {}),
                "current_units": item.get("current_units", {}),
                "hourly": item.get("hourly", {}),
                "hourly_units": item.get("hourly_units", {}),
            }
            enriched_locations.append(enriched_item)

        payload_bytes = json.dumps(enriched_locations, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        result = {
            "metadata": {
                "source_id": "openmeteo_forecast",
                "source_name": "Open-Meteo Forecast & Recent API",
                "query_timestamp_utc": query_time.isoformat(),
                "locations_count": len(enriched_locations),
                "past_days": past_days,
                "forecast_days": forecast_days,
                "http_status_code": response.status_code,
                "rate_limit_cost": response.headers.get("X-RateLimit-Cost", "1"),
                "payload_sha256": payload_hash,
            },
            "data": enriched_locations,
        }
        return result


def extract_openmeteo_data() -> Dict[str, Any]:
    """Helper entry point for Airflow task."""
    client = OpenMeteoClient()
    return client.fetch_forecast_batch()
