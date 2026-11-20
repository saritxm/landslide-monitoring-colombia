"""Unit and integration tests for Workshop #2: Bronze & Silver Pipelines."""

import json
from pathlib import Path
import pandas as pd
import pytest

from src.preprocessing.cleaning import (
    transform_era5_to_silver,
    transform_glc_to_silver,
    transform_lhasa_to_silver,
    transform_weather_to_silver,
)
from src.preprocessing.nlp import clean_text_for_nlp, tokenize_and_remove_stopwords
from src.preprocessing.outliers import detect_outliers_iqr, detect_outliers_zscore
from src.preprocessing.parquet_writer import write_dataframe_to_silver_parquet
from src.sources.api_client import MONITORING_LOCATIONS, extract_openmeteo_data
from src.sources.era5_client import extract_era5_historical
from src.sources.glc_client import extract_glc_data
from src.sources.lhasa_raster import extract_station_hazard_stats
from src.sources.scraper import extract_lhasa_data
from src.validation.schema import validate_bronze_payload

ROOT = Path(__file__).resolve().parents[1]


def test_openmeteo_api_client_structure():
    """Verify that Open-Meteo client returns valid schema with Colombia locations."""
    payload = extract_openmeteo_data()
    assert "metadata" in payload
    assert "data" in payload
    assert payload["metadata"]["source_id"] == "openmeteo_forecast"
    assert len(payload["data"]) >= 5

    sample_station = payload["data"][0]
    assert "returned_latitude" in sample_station
    assert "returned_longitude" in sample_station
    assert "hourly" in sample_station
    assert "precipitation" in sample_station["hourly"]


def test_lhasa_scraper_structure():
    """Verify that NASA LHASA scraper detects raster files and returns metadata."""
    payload = extract_lhasa_data()
    assert "metadata" in payload
    assert "data" in payload
    assert payload["metadata"]["source_id"] == "nasa_lhasa"
    assert len(payload["data"]) > 0

    first_file = payload["data"][0]
    assert first_file["file_name"].endswith(".tif")
    assert "nominal_issue_time" in first_file
    assert first_file["url"].startswith("http")


def test_bronze_payload_validation():
    """Verify schema validator checks required fields and bounds."""
    valid_payload = {
        "metadata": {"source_id": "openmeteo_forecast"},
        "data": [{"returned_latitude": 4.6, "returned_longitude": -74.0}],
    }
    is_valid, errors = validate_bronze_payload(valid_payload, "openmeteo_forecast")
    assert is_valid
    assert len(errors) == 0

    invalid_payload = {
        "metadata": {"source_id": "wrong_source"},
        "data": [],
    }
    is_valid, errors = validate_bronze_payload(invalid_payload, "openmeteo_forecast")
    assert not is_valid


def test_nlp_text_preprocessing():
    """Verify text cleaning, stopword removal, and tokenization."""
    raw_text = "Deslizamiento provocado por FUERTES LLUVIAS en la vía Manizales... ¡Emergencia total!"
    cleaned = clean_text_for_nlp(raw_text)
    assert "fuertes lluvias" in cleaned
    assert "!" not in cleaned

    tokens = tokenize_and_remove_stopwords(raw_text)
    assert "fuertes" in tokens or "lluvias" in tokens or "manizales" in tokens
    # Spanish stopwords like 'en', 'la', 'por' must be filtered out
    assert "la" not in tokens
    assert "por" not in tokens
    assert "en" not in tokens


def test_outlier_detection_iqr_and_zscore():
    """Verify statistical outlier detection on synthetic distributions."""
    data = pd.Series([1.0, 1.2, 1.1, 0.9, 1.3, 1.0, 1.1, 0.8, 100.0])  # 100 is outlier
    is_iqr, iqr_stats = detect_outliers_iqr(data)
    assert is_iqr.iloc[-1] is True or is_iqr.iloc[-1] == True
    assert iqr_stats["outlier_count"] >= 1

    is_z, z_scores, z_stats = detect_outliers_zscore(data, threshold=2.0)
    assert is_z.iloc[-1] is True or is_z.iloc[-1] == True
    assert z_scores.iloc[-1] > 2.0


def test_weather_transformation_and_lineage(tmp_path):
    """Verify weather transformation produces clean DataFrame and Parquet with lineage."""
    raw_payload = extract_openmeteo_data()
    df, stats = transform_weather_to_silver(raw_payload, "openmeteo_test.json")

    assert not df.empty
    assert "timestamp" in df.columns
    assert "precipitation_mm" in df.columns
    assert "precip_outlier_iqr" in df.columns
    assert "_bronze_source_file" in df.columns
    assert "_record_hash" in df.columns
    assert df["precipitation_mm"].min() >= 0.0

    # Write Parquet and verify readability
    pq_path = write_dataframe_to_silver_parquet(df, "rainfall_test", output_dir=tmp_path)
    assert pq_path.exists()

    read_df = pd.read_parquet(pq_path)
    assert len(read_df) == len(df)


def test_glc_landslide_transformation_and_nlp(tmp_path):
    """Verify GLC transformation performs NLP and geographic filtering."""
    raw_payload = extract_glc_data()
    df, stats = transform_glc_to_silver(raw_payload, "glc_test.json")

    assert not df.empty
    assert "cleaned_text_nlp" in df.columns
    assert "tokens_nlp" in df.columns
    assert "fatality_outlier_iqr" in df.columns
    assert (df["country_name"] == "Colombia").all()

    pq_path = write_dataframe_to_silver_parquet(df, "landslides_test", output_dir=tmp_path)
    assert pq_path.exists()


def test_outlier_detection_with_zero_exclusion():
    """Verify that exclude_zeros=True prevents zero-inflation collapse in precipitation/fatalities."""
    # 20 dry hours (0.0) and 5 wet hours, with one extreme storm (150.0)
    data = pd.Series([0.0] * 20 + [1.0, 2.0, 1.5, 3.0, 150.0])

    # Without exclude_zeros, Q1=0, Q3=0, IQR=0 -> even normal drizzle (1.0 mm) is wrongly an outlier
    is_iqr_naive, _ = detect_outliers_iqr(data, exclude_zeros=False)
    assert is_iqr_naive.iloc[20] is True or is_iqr_naive.iloc[20] == True  # 1.0 was flagged

    # With exclude_zeros=True, normal rain is NOT flagged, only extreme spike (150.0) is flagged
    is_iqr_clean, stats = detect_outliers_iqr(data, exclude_zeros=True)
    assert not is_iqr_clean.iloc[0]   # 0.0 is never an outlier
    assert not is_iqr_clean.iloc[20]  # 1.0 is normal rain
    assert is_iqr_clean.iloc[-1] is True or is_iqr_clean.iloc[-1] == True  # 150.0 is outlier
    assert stats["outlier_count"] == 1


def test_glc_time_unknown_flag():
    """Verify that unknown timestamps (midnight 00:00:00) are flagged as is_time_unknown."""
    synthetic_payload = {
        "metadata": {"source_id": "nasa_glc"},
        "data": [
            {
                "event_id": 99901,
                "event_date": "04/01/2017 12:00:00 AM",  # Midnight unknown time
                "latitude": 1.15,
                "longitude": -76.65,
                "country_name": "Colombia",
                "fatality_count": 10,
            },
            {
                "event_id": 99902,
                "event_date": "04/01/2017 03:45:00 PM",  # Explicit afternoon time
                "latitude": 4.60,
                "longitude": -74.08,
                "country_name": "Colombia",
                "fatality_count": 0,
            },
        ],
    }
    df, stats = transform_glc_to_silver(synthetic_payload, "synthetic_glc.json")
    assert "is_time_unknown" in df.columns
    row_unknown = df[df["event_id"] == 99901].iloc[0]
    row_explicit = df[df["event_id"] == 99902].iloc[0]
    assert bool(row_unknown["is_time_unknown"]) is True
    assert bool(row_explicit["is_time_unknown"]) is False


def test_era5_extraction_and_silver_transformation(tmp_path):
    """Verify ERA5 archive extraction, schema validation, and Silver Parquet transformation."""
    # Test a short 5-day window for 2 stations to stay lightweight
    test_stations = MONITORING_LOCATIONS[:2]
    payload = extract_era5_historical(
        start_date="2023-05-01",
        end_date="2023-05-05",
        stations=test_stations,
    )
    assert payload["metadata"]["source_id"] == "openmeteo_era5"
    assert len(payload["data"]) == 2

    is_valid, errors = validate_bronze_payload(payload, "openmeteo_era5")
    assert is_valid
    assert len(errors) == 0

    df, stats = transform_era5_to_silver(payload, "era5_test.json")
    assert not df.empty
    assert "date_utc" in df.columns
    assert "precip_24h_mm" in df.columns
    assert "precip_7d_mm" in df.columns
    assert "year" in df.columns
    assert "month" in df.columns
    assert "_record_hash" in df.columns
    assert df["precip_24h_mm"].min() >= 0.0

    pq_path = write_dataframe_to_silver_parquet(df, "rainfall_historical", output_dir=tmp_path)
    assert pq_path.exists()
    read_df = pd.read_parquet(pq_path)
    assert len(read_df) == len(df)


def test_lhasa_raster_hazard_extraction_and_silver(tmp_path):
    """Verify spatial raster extraction with rasterio and Silver hazard table generation."""
    sample_tif = ROOT / "workshop_1" / "samples" / "lhasa_colombia_20261106T0000.tif"
    assert sample_tif.exists(), "Sample GeoTIFF required for test"

    test_stations = MONITORING_LOCATIONS[:3]
    hazard_stats = extract_station_hazard_stats(
        tif_url_or_path=str(sample_tif),
        stations=test_stations,
        nominal_issue_time="2026-11-06T00:00:00Z",
    )
    assert len(hazard_stats) == 3
    first = hazard_stats[0]
    assert "station_name" in first
    assert "lhasa_max" in first
    assert "valid_pixel_count" in first
    assert first["valid_pixel_count"] > 0

    # Build Bronze payload with station_hazards and transform to Silver Parquet
    payload = {
        "metadata": {"source_id": "nasa_lhasa"},
        "data": [{"file_name": sample_tif.name, "url": "local_test"}],
        "station_hazards": hazard_stats,
    }
    df, stats = transform_lhasa_to_silver(payload, "lhasa_test.json")
    assert not df.empty
    assert "station_name" in df.columns
    assert "lhasa_max" in df.columns
    assert "has_high_hazard" in df.columns
    assert "_bronze_source_file" in df.columns

    pq_path = write_dataframe_to_silver_parquet(df, "lhasa_hazard", output_dir=tmp_path)
    assert pq_path.exists()
    read_df = pd.read_parquet(pq_path)
    assert len(read_df) == len(df)

