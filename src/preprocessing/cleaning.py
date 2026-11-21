"""Silver Layer Transformation and Cleaning Pipelines.

Converts raw Bronze JSON payloads into cleansed, standardized Silver DataFrames
with explicit schema enforcement, conservative null handling (no invented values),
NLP normalization, outlier flags,
and end-to-end lineage tracking.
"""

from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, Optional, Tuple
import numpy as np
import pandas as pd

from src.preprocessing.nlp import clean_text_for_nlp, tokenize_and_remove_stopwords
from src.preprocessing.outliers import detect_outliers_iqr, detect_outliers_zscore

logger = logging.getLogger(__name__)

# Physical validity bound for hourly precipitation (mm/h). Values above it are
# far beyond any recorded hourly rainfall and are treated as errors, not extremes.
MAX_HOURLY_PRECIP_MM = 300.0
# Longest soil-moisture gap (hours) that may be interpolated.
SOIL_MOISTURE_MAX_GAP_H = 2


def compute_row_hash(row_dict: Dict[str, Any]) -> str:
    """Computes a deterministic SHA-256 hash of a dictionary."""
    serialized = json.dumps(row_dict, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def transform_weather_to_silver(
    raw_payload: Dict[str, Any],
    source_filename: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Transforms raw Open-Meteo weather JSON into the Silver rainfall schema."""
    processed_time = datetime.now(timezone.utc)
    raw_data = raw_payload.get("data", [])

    rows = []
    for loc in raw_data:
        loc_name = loc.get("location_name", "Unknown")
        department = loc.get("department", "Unknown")
        lat = loc.get("returned_latitude")
        lon = loc.get("returned_longitude")
        elevation = loc.get("elevation_m")

        hourly = loc.get("hourly", {})
        times = hourly.get("time", [])
        precips = hourly.get("precipitation", [])
        rains = hourly.get("rain", [])
        soil_moistures = hourly.get("soil_moisture_0_to_1cm", [])

        n = len(times)
        for i in range(n):
            p_val = precips[i] if i < len(precips) else None
            r_val = rains[i] if i < len(rains) else None
            sm_val = soil_moistures[i] if i < len(soil_moistures) else None

            rows.append({
                "station_name": loc_name,
                "department": department,
                "latitude": lat,
                "longitude": lon,
                "elevation_m": elevation,
                "timestamp_raw": times[i],
                "precipitation_mm": p_val,
                "rain_mm": r_val,
                "soil_moisture_m3m3": sm_val,
            })

    if not rows:
        return pd.DataFrame(), {"rows_in": 0, "rows_out": 0}

    df = pd.DataFrame(rows)
    rows_initial = len(df)

    # 1. Schema enforcement and type casting
    df["timestamp"] = pd.to_datetime(df["timestamp_raw"], utc=True)
    df = df.drop(columns=["timestamp_raw"])
    df["latitude"] = df["latitude"].astype("float64")
    df["longitude"] = df["longitude"].astype("float64")
    df["elevation_m"] = df["elevation_m"].astype("float64")
    df["precipitation_mm"] = pd.to_numeric(df["precipitation_mm"], errors="coerce")
    df["rain_mm"] = pd.to_numeric(df["rain_mm"], errors="coerce")
    df["soil_moisture_m3m3"] = pd.to_numeric(df["soil_moisture_m3m3"], errors="coerce")
    df["station_name"] = df["station_name"].astype("category")
    df["department"] = df["department"].astype("category")

    # 2. Null handling
    # Drop rows without timestamp or coordinates (cannot be placed in time/space)
    df = df.dropna(subset=["timestamp", "latitude", "longitude"])

    # Precipitation is the variable the risk indicator is built on: a missing or
    # physically impossible value is NOT replaced by 0 (that would invent "no rain").
    # Such rows are rejected and counted; valid rows keep their raw value.
    precip_missing = df["precipitation_mm"].isna()
    precip_invalid = (df["precipitation_mm"] < 0) | (df["precipitation_mm"] > MAX_HOURLY_PRECIP_MM)
    rows_rejected_missing = int(precip_missing.sum())
    rows_rejected_invalid = int(precip_invalid.sum())
    df = df[~precip_missing & ~precip_invalid].copy()
    # rain_mm is informative only: keep nulls as nulls
    df.loc[df["rain_mm"] < 0, "rain_mm"] = np.nan

    # Soil moisture changes slowly: interpolate short gaps (<= 2 h) within each station
    # and flag them; longer gaps stay null instead of being filled with a median or 0.
    df = df.sort_values(["station_name", "timestamp"])
    df["soil_moisture_imputed"] = df["soil_moisture_m3m3"].isna()
    df["soil_moisture_m3m3"] = df.groupby("station_name", observed=True)["soil_moisture_m3m3"].transform(
        lambda s: s.interpolate(limit=SOIL_MOISTURE_MAX_GAP_H, limit_area="inside")
    )
    df["soil_moisture_imputed"] = df["soil_moisture_imputed"] & df["soil_moisture_m3m3"].notna()

    # 3. Deduplication by natural key (before computing statistics/outliers)
    df = df.drop_duplicates(subset=["station_name", "timestamp"], keep="last").copy()

    # 4. Outlier detection per station on wet hours (precipitation_mm > 0)
    df["precip_outlier_iqr"] = False
    df["precip_outlier_zscore"] = False
    df["precip_zscore"] = 0.0

    for _, group in df.groupby("station_name", observed=True):
        idx = group.index
        is_iqr, _ = detect_outliers_iqr(group["precipitation_mm"], multiplier=1.5, exclude_zeros=True)
        is_z, z_scores, _ = detect_outliers_zscore(group["precipitation_mm"], threshold=3.0, exclude_zeros=True)
        df.loc[idx, "precip_outlier_iqr"] = is_iqr
        df.loc[idx, "precip_outlier_zscore"] = is_z
        df.loc[idx, "precip_zscore"] = z_scores.round(3)

    # 5. Lineage & Traceability
    df["_bronze_source_file"] = source_filename
    df["_processed_at_utc"] = processed_time
    df["_record_hash"] = df.apply(
        lambda r: compute_row_hash({
            "st": str(r["station_name"]),
            "ts": str(r["timestamp"]),
            "p": float(r["precipitation_mm"]),
        }),
        axis=1,
    )

    stats = {
        "rows_initial": rows_initial,
        "rows_clean": len(df),
        "iqr_outliers": int(df["precip_outlier_iqr"].sum()),
        "zscore_outliers": int(df["precip_outlier_zscore"].sum()),
        "imputed_soil_moisture": int(df["soil_moisture_imputed"].sum()),
        "rows_rejected_missing_precip": rows_rejected_missing,
        "rows_rejected_invalid_precip": rows_rejected_invalid,
    }
    return df, stats


def transform_era5_to_silver(
    raw_payload: Dict[str, Any],
    source_filename: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Transforms raw Open-Meteo ERA5 daily JSON into Silver historical rainfall schema."""
    processed_time = datetime.now(timezone.utc)
    raw_data = raw_payload.get("data", [])

    rows = []
    for loc in raw_data:
        loc_name = loc.get("location_name", "Unknown")
        department = loc.get("department", "Unknown")
        lat = loc.get("returned_latitude")
        lon = loc.get("returned_longitude")
        elevation = loc.get("elevation_m")

        daily = loc.get("daily", {})
        dates = daily.get("time", [])
        precips = daily.get("precipitation_sum", [])

        for i, date_str in enumerate(dates):
            p_val = precips[i] if i < len(precips) else None
            rows.append({
                "station_name": loc_name,
                "department": department,
                "latitude": lat,
                "longitude": lon,
                "elevation_m": elevation,
                "date_raw": date_str,
                "precip_24h_mm": p_val,
            })

    if not rows:
        return pd.DataFrame(), {"rows_initial": 0, "rows_clean": 0}

    df = pd.DataFrame(rows)
    rows_initial = len(df)

    # 1. Schema enforcement and UTC date parsing
    df["date_utc"] = pd.to_datetime(df["date_raw"], utc=True, errors="coerce")
    df = df.drop(columns=["date_raw"])
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").astype("float64")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").astype("float64")
    df["elevation_m"] = pd.to_numeric(df["elevation_m"], errors="coerce").astype("float64")
    df["precip_24h_mm"] = pd.to_numeric(df["precip_24h_mm"], errors="coerce")
    # Missing or negative daily totals stay null (never 0) and are flagged
    df.loc[df["precip_24h_mm"] < 0, "precip_24h_mm"] = np.nan
    df["precip_24h_missing"] = df["precip_24h_mm"].isna()
    df["station_name"] = df["station_name"].astype("category")
    df["department"] = df["department"].astype("category")

    # 2. Critical null handling and deduplication before rolling windows
    df = df.dropna(subset=["date_utc", "latitude", "longitude"])
    df = df.drop_duplicates(subset=["station_name", "date_utc"], keep="last")
    df = df.sort_values(["station_name", "date_utc"]).reset_index(drop=True)

    # 3. 7-day rolling precipitation sum and seasonal attributes for PySpark percentiles
    df["precip_7d_mm"] = (
        df.groupby("station_name", observed=True)["precip_24h_mm"]
        .transform(lambda s: s.rolling(window=7, min_periods=7).sum())
        .round(3)
    )
    df["year"] = df["date_utc"].dt.year.astype("int16")
    df["month"] = df["date_utc"].dt.month.astype("int8")

    # 4. Lineage and traceability
    df["_bronze_source_file"] = source_filename
    df["_processed_at_utc"] = processed_time
    df["_record_hash"] = df.apply(
        lambda r: compute_row_hash({
            "st": str(r["station_name"]),
            "dt": str(r["date_utc"].date()),
            "p24": None if pd.isna(r["precip_24h_mm"]) else float(r["precip_24h_mm"]),
        }),
        axis=1,
    )

    stats = {
        "rows_initial": rows_initial,
        "rows_clean": len(df),
        "missing_precip_24h": int(df["precip_24h_missing"].sum()),
        "max_precip_24h": float(df["precip_24h_mm"].max()) if not df.empty else None,
        "max_precip_7d": float(df["precip_7d_mm"].max()) if not df.empty else None,
    }
    return df, stats


def transform_lhasa_to_silver(
    raw_payload: Dict[str, Any],
    source_filename: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Transforms raw scraped LHASA JSON into Silver hazard schema (station-level or metadata)."""
    processed_time = datetime.now(timezone.utc)

    # Priority: If station-level raster hazard stats were extracted, produce tabular hazard table
    if "station_hazards" in raw_payload and raw_payload["station_hazards"]:
        hazards = raw_payload["station_hazards"]
        df = pd.DataFrame(hazards)
        rows_initial = len(df)
        df["nominal_issue_time"] = pd.to_datetime(df["nominal_issue_time"], utc=True)
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").astype("float64")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").astype("float64")
        df["station_name"] = df["station_name"].astype("category")
        df["department"] = df["department"].astype("category")
        df["valid_pixel_count"] = pd.to_numeric(df.get("valid_pixel_count", 0), errors="coerce").fillna(0).astype("int64")
        df["lhasa_max"] = pd.to_numeric(df.get("lhasa_max"), errors="coerce")
        df["lhasa_mean"] = pd.to_numeric(df.get("lhasa_mean"), errors="coerce")
        df["lhasa_p90"] = pd.to_numeric(df.get("lhasa_p90"), errors="coerce")
        df["has_high_hazard"] = df.get("has_high_hazard", False).astype(bool)

        df = df.dropna(subset=["station_name", "nominal_issue_time"])
        df = df.drop_duplicates(subset=["station_name", "nominal_issue_time"], keep="last")

        df["_bronze_source_file"] = source_filename
        df["_processed_at_utc"] = processed_time
        df["_record_hash"] = df.apply(
            lambda r: compute_row_hash({
                "st": str(r["station_name"]),
                "time": str(r["nominal_issue_time"]),
                "max": str(r["lhasa_max"]),
            }),
            axis=1,
        )
        stats = {
            "rows_initial": rows_initial,
            "rows_clean": len(df),
            "high_hazard_stations": int(df["has_high_hazard"].sum()),
        }
        return df, stats

    raw_data = raw_payload.get("data", [])

    if not raw_data:
        return pd.DataFrame(), {"rows_in": 0, "rows_out": 0}

    df = pd.DataFrame(raw_data)
    rows_initial = len(df)

    # 1. Type normalization
    df["nominal_issue_time"] = pd.to_datetime(df["nominal_issue_time"], utc=True)
    df["file_name"] = df["file_name"].astype(str)
    df["url"] = df["url"].astype(str)
    # Only the newest files get an HTTP HEAD request: the others have an unknown size (null, not 0)
    df["content_length_bytes"] = pd.to_numeric(df.get("content_length_bytes", pd.Series(dtype="float64")), errors="coerce").astype("Int64")
    df["etag"] = df.get("etag", pd.Series(dtype="str")).fillna("unknown").astype(str)
    df["last_modified"] = df.get("last_modified", pd.Series(dtype="str")).fillna("unknown").astype(str)

    # 2. Null handling: drop rows without nominal issue time
    df = df.dropna(subset=["nominal_issue_time", "file_name"])

    # 3. Deduplication by issue time and filename
    df = df.drop_duplicates(subset=["nominal_issue_time", "file_name"], keep="first")

    # 4. Lineage & Traceability
    df["_bronze_source_file"] = source_filename
    df["_processed_at_utc"] = processed_time
    df["_record_hash"] = df.apply(
        lambda r: compute_row_hash({
            "file": r["file_name"],
            "time": str(r["nominal_issue_time"]),
            "etag": r["etag"],
        }),
        axis=1,
    )

    stats = {
        "rows_initial": rows_initial,
        "rows_clean": len(df),
    }
    return df, stats


def transform_glc_to_silver(
    raw_payload: Dict[str, Any],
    source_filename: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Transforms raw NASA GLC landslide catalog into Silver schema with NLP preprocessing."""
    processed_time = datetime.now(timezone.utc)
    raw_data = raw_payload.get("data", [])

    if not raw_data:
        return pd.DataFrame(), {"rows_in": 0, "rows_out": 0}

    df = pd.DataFrame(raw_data)
    rows_initial = len(df)

    # 1. Type normalization & timestamp parsing
    # event_date format: "%m/%d/%Y %I:%M:%S %p" or similar
    df["event_date_parsed"] = pd.to_datetime(df["event_date"], format="mixed", errors="coerce")
    df["is_time_unknown"] = (
        (df["event_date_parsed"].dt.hour == 0)
        & (df["event_date_parsed"].dt.minute == 0)
        & (df["event_date_parsed"].dt.second == 0)
    ).fillna(False)
    # Add 5 hours to convert Colombia local time to UTC
    df["timestamp_utc"] = (df["event_date_parsed"] + pd.Timedelta(hours=5)).dt.tz_localize("UTC")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    # Casualty counts: a missing value is unknown, not zero. Keep it null (nullable Int64)
    # and flag it, so no deaths or injuries are invented.
    for col in ("fatality_count", "injury_count"):
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df["fatality_missing"] = df["fatality_count"].isna()

    # 2. Null handling & Geographic filter
    # Drop rows without coordinates or timestamps
    df = df.dropna(subset=["latitude", "longitude", "timestamp_utc"])

    # Scope filter: Colombia Bounding Box AND (country_name is Colombia OR country_name is empty/null)
    # Exclude border events explicitly tagged with neighboring countries (e.g. Ecuador, Venezuela)
    mask_bbox = (
        (df["latitude"] >= -4.3)
        & (df["latitude"] <= 13.5)
        & (df["longitude"] >= -79.1)
        & (df["longitude"] <= -66.8)
    )
    mask_country = (
        (df["country_name"] == "Colombia")
        | (df["country_name"].isna())
        | (df["country_name"] == "")
    )
    df = df[mask_bbox & mask_country].copy()

    # Missing country_name is NOT imputed: the bounding box also covers parts of Ecuador,
    # Venezuela, Panama, Peru and Brazil, so a null-country event inside it is not
    # necessarily Colombian (Mocoa 2017, event_id 10790, is one of them). The rows are
    # kept and flagged; the country is assigned spatially in the Gold layer.
    df.loc[df["country_name"] == "", "country_name"] = pd.NA
    df["country_missing"] = df["country_name"].isna()
    for col, default_val in [
        ("admin_division_name", "Unknown"),
        ("landslide_trigger", "unknown"),
        ("landslide_size", "unknown"),
    ]:
        if col not in df.columns:
            df[col] = default_val
        else:
            df[col] = df[col].fillna(default_val)

    # 3. Deduplication by event_id (before computing narrative features and outliers)
    if "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"], keep="first").copy()

    # 4. Text Preprocessing for NLP on narrative columns
    text_fields = ["event_title", "event_description", "location_description"]
    for col in text_fields:
        if col not in df.columns:
            df[col] = ""

    df["raw_text_narrative"] = (
        df["event_title"].fillna("") + " " +
        df["event_description"].fillna("") + " " +
        df["location_description"].fillna("")
    ).str.strip()

    df["cleaned_text_nlp"] = df["raw_text_narrative"].apply(clean_text_for_nlp)
    df["tokens_nlp"] = df["raw_text_narrative"].apply(tokenize_and_remove_stopwords)
    df["token_count"] = df["tokens_nlp"].apply(len)

    # 5. Outlier detection on fatality_count (ignoring 0-fatality events to prevent zero collapse)
    fatalities = df["fatality_count"].astype("float64")  # nulls stay NaN and are never flagged
    is_iqr, _ = detect_outliers_iqr(fatalities, multiplier=1.5, exclude_zeros=True)
    is_z, z_scores, _ = detect_outliers_zscore(fatalities, threshold=3.0, exclude_zeros=True)
    df["fatality_outlier_iqr"] = is_iqr
    df["fatality_outlier_zscore"] = is_z
    df["fatality_zscore"] = z_scores.round(2)

    # 6. Lineage & Traceability
    df["_bronze_source_file"] = source_filename
    df["_processed_at_utc"] = processed_time
    df["_record_hash"] = df.apply(
        lambda r: compute_row_hash({
            "eid": str(r.get("event_id", "")),
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "ts": str(r["timestamp_utc"]),
        }),
        axis=1,
    )

    stats = {
        "rows_initial": rows_initial,
        "rows_clean": len(df),
        "total_tokens_extracted": int(df["token_count"].sum()),
        "fatality_iqr_outliers": int(is_iqr.sum()),
        "country_missing": int(df["country_missing"].sum()),
        "fatality_missing": int(df["fatality_missing"].sum()),
    }
    return df, stats
