"""Unit and integration tests for Workshop #2: Bronze & Silver Pipelines."""

import json
from pathlib import Path
import pandas as pd
import pytest

from src.preprocessing.cleaning import (
    transform_glc_to_silver,
    transform_lhasa_to_silver,
    transform_weather_to_silver,
)
from src.preprocessing.nlp import clean_text_for_nlp, tokenize_and_remove_stopwords
from src.preprocessing.outliers import detect_outliers_iqr, detect_outliers_zscore
from src.preprocessing.parquet_writer import write_dataframe_to_silver_parquet
from src.sources.api_client import extract_openmeteo_data
from src.sources.glc_client import extract_glc_data
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
