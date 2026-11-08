"""Basic checks for Workshop #1: the CLI stub, the source configuration and the recorded fixtures."""

import json
import pathlib

import yaml

import ingest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def test_cli_accepts_the_three_modes():
    for mode in ["historical", "incremental", "status"]:
        assert ingest.main(["--mode", mode]) == 0


def test_sources_yaml_has_four_sources_with_required_fields():
    cfg = yaml.safe_load((ROOT / "config" / "sources.yaml").read_text(encoding="utf-8"))
    sources = cfg["sources"]
    assert len(sources) == 4
    assert sorted(s["role"] for s in sources) == ["historical", "historical", "incremental", "incremental"]
    for s in sources:
        for field in ["id", "url", "format", "schedule", "watermark_field", "duplicate_key", "license", "attribution"]:
            assert field in s, f"{s['id']} is missing {field}"


def test_forecast_fixtures_share_keys_and_have_no_duplicates():
    polls = [json.loads((FIXTURES / name).read_text(encoding="utf-8"))
             for name in ["openmeteo_forecast_poll1.json", "openmeteo_forecast_poll2.json"]]
    for poll in polls:
        assert len(poll) == 10                      # ten locations per request
        for loc in poll:
            times = loc["hourly"]["time"]
            assert len(times) == len(set(times))    # (cell, hour) is a valid duplicate key
            assert loc["hourly_units"]["precipitation"] == "mm"
    assert [loc["hourly"]["time"] for loc in polls[0]] == [loc["hourly"]["time"] for loc in polls[1]]
