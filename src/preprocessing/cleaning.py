"""Silver Layer Transformation and Cleaning Pipelines.

Converts raw Bronze JSON payloads into cleansed, standardized Silver DataFrames
with explicit schema enforcement, null imputation, NLP normalization, outlier flags,
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

    # 2. Null handling & imputation
    # Drop rows without timestamp or coordinates
    df = df.dropna(subset=["timestamp", "latitude", "longitude"])

    # Precipitation: non-negative check and zero-fill missing values
    df["precipitation_mm"] = df["precipitation_mm"].fillna(0.0).clip(lower=0.0)
    df["rain_mm"] = df["rain_mm"].fillna(0.0).clip(lower=0.0)

    # Soil moisture: track whether imputed, then fill missing with median of the station
    df["soil_moisture_imputed"] = df["soil_moisture_m3m3"].isna()
    station_medians = df.groupby("station_name", observed=True)["soil_moisture_m3m3"].transform("median")
    overall_median = df["soil_moisture_m3m3"].median()
    df["soil_moisture_m3m3"] = df["soil_moisture_m3m3"].fillna(station_medians).fillna(overall_median).fillna(0.0)

    # 3. Outlier detection
    is_iqr, iqr_stats = detect_outliers_iqr(df["precipitation_mm"], multiplier=1.5)
    is_z, z_scores, z_stats = detect_outliers_zscore(df["precipitation_mm"], threshold=3.0)
    df["precip_outlier_iqr"] = is_iqr
    df["precip_outlier_zscore"] = is_z
    df["precip_zscore"] = z_scores.round(3)

    # 4. Deduplication by natural key
    df = df.drop_duplicates(subset=["station_name", "timestamp"], keep="last")

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
        "iqr_outliers": int(is_iqr.sum()),
        "zscore_outliers": int(is_z.sum()),
        "imputed_soil_moisture": int(df["soil_moisture_imputed"].sum()),
    }
    return df, stats


def transform_lhasa_to_silver(
    raw_payload: Dict[str, Any],
    source_filename: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Transforms raw scraped LHASA directory JSON into the Silver hazard metadata schema."""
    processed_time = datetime.now(timezone.utc)
    raw_data = raw_payload.get("data", [])

    if not raw_data:
        return pd.DataFrame(), {"rows_in": 0, "rows_out": 0}

    df = pd.DataFrame(raw_data)
    rows_initial = len(df)

    # 1. Type normalization
    df["nominal_issue_time"] = pd.to_datetime(df["nominal_issue_time"], utc=True)
    df["file_name"] = df["file_name"].astype(str)
    df["url"] = df["url"].astype(str)
    df["content_length_bytes"] = pd.to_numeric(df.get("content_length_bytes", pd.Series(dtype="float64")), errors="coerce").fillna(0).astype("int64")
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
    # Add 5 hours to convert Colombia local time to UTC
    df["timestamp_utc"] = df["event_date_parsed"].apply(
        lambda d: (d + pd.Timedelta(hours=5)).tz_localize("UTC") if pd.notnull(d) else pd.NaT
    )

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["fatality_count"] = pd.to_numeric(df.get("fatality_count", 0), errors="coerce").fillna(0).astype("int64")
    df["injury_count"] = pd.to_numeric(df.get("injury_count", 0), errors="coerce").fillna(0).astype("int64")

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

    # Impute missing country_name (as learned from Mocoa tragedy event_id 10790)
    df["country_name"] = df["country_name"].fillna("Colombia")
    df.loc[df["country_name"] == "", "country_name"] = "Colombia"
    df["admin_division_name"] = df["admin_division_name"].fillna("Unknown")
    df["landslide_trigger"] = df["landslide_trigger"].fillna("unknown")
    df["landslide_size"] = df["landslide_size"].fillna("unknown")

    # 3. Text Preprocessing for NLP on narrative columns
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

    # 4. Outlier detection on fatality_count
    is_iqr, _ = detect_outliers_iqr(df["fatality_count"], multiplier=1.5)
    is_z, z_scores, _ = detect_outliers_zscore(df["fatality_count"], threshold=3.0)
    df["fatality_outlier_iqr"] = is_iqr
    df["fatality_outlier_zscore"] = is_z
    df["fatality_zscore"] = z_scores.round(2)

    # 5. Deduplication by event_id
    if "event_id" in df.columns:
        df = df.drop_duplicates(subset=["event_id"], keep="first")

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
    }
    return df, stats
