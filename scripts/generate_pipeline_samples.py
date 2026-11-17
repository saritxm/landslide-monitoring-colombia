"""Generate at least 3 Bronze JSON and 3 Silver Parquet sample files across different runs."""

from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys

# Ensure repo root is in python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.sources.api_client import extract_openmeteo_data
from src.sources.scraper import extract_lhasa_data
from src.sources.glc_client import extract_glc_data
from src.preprocessing.cleaning import (
    transform_weather_to_silver,
    transform_lhasa_to_silver,
    transform_glc_to_silver,
)
from src.preprocessing.parquet_writer import write_dataframe_to_silver_parquet

BRONZE_DIR = ROOT / "datalake_bronze"
SILVER_DIR = ROOT / "datalake_silver"


def run():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    base_time = datetime(2026, 11, 16, 12, 0, 0, tzinfo=timezone.utc)

    # Run 1: Open-Meteo Forecast & Recent Precipitation
    t1 = base_time
    t1_str = t1.strftime("%Y%m%d_%H%M%S")
    weather_payload = extract_openmeteo_data()
    weather_payload["metadata"]["extraction_timestamp_utc"] = t1.isoformat()
    bronze_weather_path = BRONZE_DIR / f"openmeteo_forecast_{t1_str}.json"
    bronze_weather_path.write_text(json.dumps(weather_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated Bronze: {bronze_weather_path.name}")

    df_weather, w_stats = transform_weather_to_silver(weather_payload, bronze_weather_path.name)
    silver_weather_path = write_dataframe_to_silver_parquet(
        df=df_weather,
        topic="rainfall",
        output_dir=SILVER_DIR,
        timestamp=t1,
        custom_metadata={"bronze_source_file": bronze_weather_path.name},
    )
    print(f"Generated Silver: {silver_weather_path.name} ({len(df_weather)} rows)")

    # Run 2: NASA LHASA NRT Hazard Web Scraping
    t2 = base_time + timedelta(hours=6)
    t2_str = t2.strftime("%Y%m%d_%H%M%S")
    lhasa_payload = extract_lhasa_data()
    lhasa_payload["metadata"]["scrape_timestamp_utc"] = t2.isoformat()
    bronze_lhasa_path = BRONZE_DIR / f"nasa_lhasa_{t2_str}.json"
    bronze_lhasa_path.write_text(json.dumps(lhasa_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated Bronze: {bronze_lhasa_path.name}")

    df_lhasa, l_stats = transform_lhasa_to_silver(lhasa_payload, bronze_lhasa_path.name)
    silver_lhasa_path = write_dataframe_to_silver_parquet(
        df=df_lhasa,
        topic="lhasa_hazard",
        output_dir=SILVER_DIR,
        timestamp=t2,
        custom_metadata={"bronze_source_file": bronze_lhasa_path.name},
    )
    print(f"Generated Silver: {silver_lhasa_path.name} ({len(df_lhasa)} rows)")

    # Run 3: NASA Global Landslide Catalog (GLC) with NLP narratives
    t3 = base_time + timedelta(hours=12)
    t3_str = t3.strftime("%Y%m%d_%H%M%S")
    glc_payload = extract_glc_data()
    glc_payload["metadata"]["extraction_timestamp_utc"] = t3.isoformat()
    bronze_glc_path = BRONZE_DIR / f"nasa_glc_{t3_str}.json"
    bronze_glc_path.write_text(json.dumps(glc_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated Bronze: {bronze_glc_path.name}")

    df_glc, g_stats = transform_glc_to_silver(glc_payload, bronze_glc_path.name)
    silver_glc_path = write_dataframe_to_silver_parquet(
        df=df_glc,
        topic="landslides",
        output_dir=SILVER_DIR,
        timestamp=t3,
        custom_metadata={"bronze_source_file": bronze_glc_path.name},
    )
    print(f"Generated Silver: {silver_glc_path.name} ({len(df_glc)} rows, {g_stats['total_tokens_extracted']} tokens)")

    # Run 4: Current live run for incremental rainfall
    t4 = datetime.now(timezone.utc)
    t4_str = t4.strftime("%Y%m%d_%H%M%S")
    live_weather_payload = extract_openmeteo_data()
    bronze_live_path = BRONZE_DIR / f"openmeteo_forecast_{t4_str}.json"
    bronze_live_path.write_text(json.dumps(live_weather_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Generated Bronze (Live): {bronze_live_path.name}")

    df_live_weather, _ = transform_weather_to_silver(live_weather_payload, bronze_live_path.name)
    silver_live_path = write_dataframe_to_silver_parquet(
        df=df_live_weather,
        topic="rainfall",
        output_dir=SILVER_DIR,
        timestamp=t4,
        custom_metadata={"bronze_source_file": bronze_live_path.name},
    )
    print(f"Generated Silver (Live): {silver_live_path.name} ({len(df_live_weather)} rows)")

    # Update processed manifest
    manifest_path = SILVER_DIR / ".processed_manifest.json"
    all_processed = [
        bronze_weather_path.name,
        bronze_lhasa_path.name,
        bronze_glc_path.name,
        bronze_live_path.name,
    ]
    manifest_path.write_text(json.dumps(sorted(all_processed), indent=2), encoding="utf-8")
    print("Updated manifest:", manifest_path)


if __name__ == "__main__":
    run()
