"""NASA LHASA GeoTIFF Spatial Extraction Module.

Supports HTTP Range Requests via GDAL /vsicurl/ to crop Colombia's bounding box
and extract station-level hazard statistics (0.0 to 1.0) for Silver Parquet tables.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import rasterio
from rasterio.transform import from_bounds as transform_from_bounds
from rasterio.windows import from_bounds as window_from_bounds

logger = logging.getLogger(__name__)

# Official Colombia bounding box for project
COLOMBIA_BBOX = {"west": -79.1, "south": -4.3, "east": -66.8, "north": 13.5}


def _to_vsi_path(tif_url_or_path: str) -> str:
    if tif_url_or_path.startswith(("http://", "https://")):
        return f"/vsicurl/{tif_url_or_path}"
    return str(tif_url_or_path)


def crop_colombia_raster(tif_url_or_path: str, output_path: Path) -> Path:
    """Downloads or extracts Colombia's window via /vsicurl/ and saves a north-up GeoTIFF."""
    vsi_path = _to_vsi_path(tif_url_or_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(vsi_path) as src:
        is_south_up = src.transform.e > 0
        window = window_from_bounds(
            left=COLOMBIA_BBOX["west"],
            bottom=COLOMBIA_BBOX["north"] if is_south_up else COLOMBIA_BBOX["south"],
            right=COLOMBIA_BBOX["east"],
            top=COLOMBIA_BBOX["south"] if is_south_up else COLOMBIA_BBOX["north"],
            transform=src.transform,
        )
        data = src.read(1, window=window).astype("float32")

        if is_south_up:
            data = np.flipud(data)

        # Normalize non-physical values to NaN
        data = np.where(np.isfinite(data) & (data >= 0.0) & (data <= 1.0), data, np.nan)

        height, width = data.shape
        out_transform = transform_from_bounds(
            COLOMBIA_BBOX["west"],
            COLOMBIA_BBOX["south"],
            COLOMBIA_BBOX["east"],
            COLOMBIA_BBOX["north"],
            width,
            height,
        )

        profile = src.profile.copy()
        profile.update({
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": 1,
            "dtype": "float32",
            "transform": out_transform,
            "nodata": np.nan,
            "compress": "deflate",
        })

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(data, 1)

    logger.info("Cropped Colombia LHASA raster saved to %s (%d bytes)", output_path, output_path.stat().st_size)
    return output_path


def extract_station_hazard_stats(
    tif_url_or_path: str,
    stations: List[Dict[str, Any]],
    nominal_issue_time: Optional[str] = None,
    half_box_deg: float = 0.125,
) -> List[Dict[str, Any]]:
    """Extracts LHASA hazard stats within a +/-0.125 deg (~28 km) box around each station."""
    vsi_path = _to_vsi_path(tif_url_or_path)
    results: List[Dict[str, Any]] = []

    with rasterio.open(vsi_path) as src:
        is_south_up = src.transform.e > 0

        for st in stations:
            lat, lon = float(st["latitude"]), float(st["longitude"])
            window = window_from_bounds(
                left=lon - half_box_deg,
                bottom=lat + half_box_deg if is_south_up else lat - half_box_deg,
                right=lon + half_box_deg,
                top=lat - half_box_deg if is_south_up else lat + half_box_deg,
                transform=src.transform,
            )
            data = src.read(1, window=window).astype("float32")
            valid = data[np.isfinite(data) & (data >= 0.0) & (data <= 1.0)]

            lhasa_max = float(np.max(valid)) if valid.size > 0 else None
            results.append({
                "station_name": st["name"],
                "department": st["department"],
                "latitude": lat,
                "longitude": lon,
                "nominal_issue_time": nominal_issue_time,
                "valid_pixel_count": int(valid.size),
                "lhasa_max": round(lhasa_max, 4) if lhasa_max is not None else None,
                "lhasa_mean": round(float(np.mean(valid)), 4) if valid.size > 0 else None,
                "lhasa_p90": round(float(np.percentile(valid, 90)), 4) if valid.size > 0 else None,
                "has_high_hazard": bool(lhasa_max is not None and lhasa_max >= 0.5),
            })

    return results
