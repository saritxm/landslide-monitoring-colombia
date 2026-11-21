"""Web scraper for NASA LHASA NRT Landslide Hazard Raster Portal.

Uses BeautifulSoup to parse the directory listing at:
https://portal.nccs.nasa.gov/datashare/landslides/nrt/hazard/tif/
Extracts available daily GeoTIFF rasters, issue timestamps, file sizes, and HTTP metadata.
"""

from datetime import datetime, timezone
import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)

LHASA_DIRECTORY_URL = "https://portal.nccs.nasa.gov/datashare/landslides/nrt/hazard/tif/"
DEFAULT_USER_AGENT = "landslide-monitoring-colombia/1.0 (web-scraper; Universidad Distrital)"


class LhasaScraper:
    """Scraper for NASA LHASA directory index using BeautifulSoup."""

    def __init__(
        self,
        directory_url: str = LHASA_DIRECTORY_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: int = 30,
    ):
        self.directory_url = directory_url
        self.headers = {"User-Agent": user_agent}
        self.timeout = timeout

    def scrape_directory_listing(self, max_items: int = 50) -> Dict[str, Any]:
        """Scrapes the directory listing HTML page for available raster files.

        Args:
            max_items: Maximum number of recent files to return.

        Returns:
            Dict containing extraction metadata and list of raster file records.
        """
        query_time = datetime.now(timezone.utc)
        logger.info("Scraping LHASA directory at %s...", self.directory_url)

        # No offline fallback: if the portal cannot be reached the task fails and Airflow
        # retries it. Persisting a made-up listing would put invented data into Bronze.
        response = requests.get(
            self.directory_url,
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        html_text = response.text
        status_code = response.status_code

        soup = BeautifulSoup(html_text, "html.parser")
        extracted_files = []

        # Find all <a> links pointing to .tif files
        tif_regex = re.compile(r"(\d{8}T\d{4})\.tif$", re.IGNORECASE)

        # NASA NCCS directory is an Apache or Nginx directory listing with <a> links and file stats
        links = soup.find_all("a", href=True)
        for a in links:
            href = a["href"]
            match = tif_regex.search(href)
            if match:
                nominal_str = match.group(1)  # YYYYMMDDTHHMM
                file_name = href.split("/")[-1]
                file_url = (
                    href if href.startswith("http")
                    else f"{self.directory_url.rstrip('/')}/{file_name}"
                )

                # Parse nominal timestamp
                try:
                    nominal_dt = datetime.strptime(nominal_str, "%Y%m%dT%H%M").replace(
                        tzinfo=timezone.utc
                    )
                    issue_time_iso = nominal_dt.isoformat()
                except ValueError:
                    issue_time_iso = None

                # Extract line text or sibling text (often contains file date and size in Apache index)
                parent_line = a.find_parent("tr") or a.parent
                line_text = parent_line.get_text(" ", strip=True) if parent_line else ""

                extracted_files.append({
                    "file_name": file_name,
                    "url": file_url,
                    "nominal_issue_time": issue_time_iso,
                    "issue_code": nominal_str,
                    "context_text": line_text[:100],
                    "discovered_at_utc": query_time.isoformat(),
                })

        # Sort by nominal issue time descending (most recent first)
        extracted_files.sort(
            key=lambda x: x.get("nominal_issue_time") or "",
            reverse=True,
        )

        selected_files = extracted_files[:max_items]

        # Query HTTP headers for the top 3 most recent files
        for item in selected_files[:3]:
            try:
                head_resp = requests.head(
                    item["url"],
                    headers=self.headers,
                    timeout=10,
                )
                if head_resp.status_code == 200:
                    item["etag"] = head_resp.headers.get("ETag")
                    item["last_modified"] = head_resp.headers.get("Last-Modified")
                    item["content_length_bytes"] = int(
                        head_resp.headers.get("Content-Length", 0)
                    )
            except Exception as e:
                logger.debug("HEAD request failed for %s: %s", item["file_name"], e)

        # Extract station-level raster hazard statistics for the latest available raster
        station_hazards = []
        if selected_files:
            latest = selected_files[0]
            latest_url = latest.get("url")
            nominal_time = latest.get("nominal_issue_time")
            try:
                from src.sources.lhasa_raster import extract_station_hazard_stats
                from src.sources.api_client import MONITORING_LOCATIONS
                logger.info("Attempting /vsicurl/ hazard stats extraction for %s...", latest.get("file_name"))
                station_hazards = extract_station_hazard_stats(
                    latest_url,
                    stations=MONITORING_LOCATIONS,
                    nominal_issue_time=nominal_time,
                )
            except Exception as err:
                # No fallback to an older sample raster: its values would be labelled with
                # today's issue time. The listing is still persisted without station hazards.
                logger.warning("Live /vsicurl/ hazard extraction unavailable (%s); "
                               "persisting the listing without station hazards.", err)

        payload_bytes = json.dumps(selected_files, sort_keys=True).encode("utf-8")
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()

        return {
            "metadata": {
                "source_id": "nasa_lhasa",
                "source_name": "NASA LHASA NRT Hazard Portal Directory",
                "scrape_timestamp_utc": query_time.isoformat(),
                "portal_url": self.directory_url,
                "http_status_code": status_code,
                "total_files_detected": len(extracted_files),
                "extracted_sample_count": len(selected_files),
                "has_station_hazards": len(station_hazards) > 0,
                "payload_sha256": payload_hash,
            },
            "data": selected_files,
            "station_hazards": station_hazards,
        }


def extract_lhasa_data() -> Dict[str, Any]:
    """Helper entry point for Airflow task."""
    scraper = LhasaScraper()
    return scraper.scrape_directory_listing()
