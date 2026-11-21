"""Generates the two Workshop #3 notebooks (data-quality findings and Gold preview)."""

from pathlib import Path
import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
KERNEL = {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}}
SETUP = """import json, pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
ROOT = pathlib.Path.cwd()
if ROOT.name == "notebooks":
    ROOT = ROOT.parent
SILVER = ROOT / "datalake_silver"
GOLD = ROOT / "datalake_gold"
pd.set_option("display.width", 160); pd.set_option("display.max_columns", 30)"""


def nb(cells, out):
    book = nbf.v4.new_notebook(metadata=KERNEL)
    book["cells"] = [nbf.v4.new_markdown_cell(c[3:]) if c.startswith("md:") else nbf.v4.new_code_cell(c) for c in cells]
    nbf.write(book, ROOT / "notebooks" / out)
    print("written", out)


nb([
    """md:# Workshop #3 — Silver data-quality findings
Team 7 — Landslide Monitoring and Risk (Colombia). Reads every Silver Parquet file with **pandas** (one folder per
topic) and measures the issues that justify the governance KPIs of the Gold layer.""",
    SETUP,
    """def load(topic):
    files = sorted((SILVER / topic).glob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True), len(files)

data = {t: load(t) for t in ["rainfall", "rainfall_historical", "landslides", "lhasa_hazard"]}
pd.DataFrame([{"topic": t, "files": n, "rows": len(df), "columns": df.shape[1]} for t, (df, n) in data.items()])""",
    """md:## 1. Null rates per field (non-zero only)""",
    """rows = []
for t, (df, _) in data.items():
    for c in df.columns:
        s = df[c]
        if s.dtype == object:
            nulls = s.isna() | s.astype(str).str.strip().eq("")
        else:
            nulls = s.isna()
        if nulls.any():
            rows.append({"topic": t, "field": c, "null_rate_%": round(100 * nulls.mean(), 2), "nulls": int(nulls.sum())})
nulls = pd.DataFrame(rows).sort_values("null_rate_%", ascending=False)
nulls""",
    """md:## 2. Duplicates: the same station-hour appears in several forecast polls
Each Open-Meteo poll repeats the previous 2 days, so the natural key (station, hour) is repeated across files, and the
values of the repeated hours are revised between polls.""",
    """rain = data["rainfall"][0]
dup = rain.duplicated(["station_name", "timestamp"], keep=False)
print(f"rows: {len(rain):,} | distinct station-hours: {rain[['station_name','timestamp']].drop_duplicates().shape[0]:,} | "
      f"rows whose key is repeated: {dup.mean():.1%}")
rev = rain[dup].groupby(["station_name", "timestamp"]).precipitation_mm.agg(["min", "max", "count"])
rev["revised"] = rev["max"] != rev["min"]
print(f"repeated station-hours whose precipitation changed between polls: {rev.revised.mean():.1%} "
      f"(largest change {(rev['max'] - rev['min']).max():.1f} mm)")""",
    """md:## 3. Outlier flags and quality flags""",
    """flags = []
for t, cols in {"rainfall": ["precip_outlier_iqr", "precip_outlier_zscore", "soil_moisture_imputed"],
                "landslides": ["fatality_outlier_iqr", "fatality_outlier_zscore", "country_missing", "fatality_missing", "is_time_unknown"],
                "rainfall_historical": ["precip_24h_missing"], "lhasa_hazard": ["has_high_hazard"]}.items():
    df = data[t][0]
    for c in cols:
        flags.append({"topic": t, "flag": c, "rate_%": round(100 * df[c].astype(float).mean(), 2)})
pd.DataFrame(flags)""",
    """fig, ax = plt.subplots(1, 2, figsize=(12, 3.5))
wet = rain[rain.precipitation_mm > 0]
ax[0].hist(wet.precipitation_mm, bins=40, color="#2b5c8f"); ax[0].set_title("Wet-hour precipitation (mm/h)"); ax[0].set_yscale("log")
gl = data["landslides"][0]
ax[1].hist(gl.fatality_count.dropna(), bins=40, color="#a04000"); ax[1].set_title("Fatalities per event (known counts)"); ax[1].set_yscale("log")
plt.tight_layout(); plt.show()""",
    """md:## 4. ERA5 annual totals: an anomalous station-year""",
    """hist = data["rainfall_historical"][0]
annual = hist.groupby(["station_name", "year"], observed=True).precip_24h_mm.sum().unstack().round(0)
ratio = annual.div(annual.median(axis=1), axis=0)
print("station-years above 2x their station median:")
print(ratio.stack().loc[lambda s: s > 2].round(2).to_string())
annual""",
    """md:## 5. Text: narrative length, tokens and language""",
    """gl["len_chars"] = gl.raw_text_narrative.str.len()
print(gl[["len_chars", "token_count"]].describe().round(1))
tokens = pd.Series([t for lst in gl.tokens_nlp for t in lst])
print("\\nTop tokens in Silver (before the Workshop #3 noise-word refinement):")
print(tokens.value_counts().head(12).to_string())
gl.len_chars.plot.hist(bins=30, figsize=(6, 3), title="Narrative length (characters)"); plt.show()""",
    """md:## 6. Time and coverage gaps""",
    """print(f"GLC events with unknown time (00:00): {gl.is_time_unknown.mean():.1%}")
polls = sorted(rain._bronze_source_file.unique())
print(f"forecast polls in Silver: {len(polls)}:", polls)
lh = data["lhasa_hazard"][0]
print("LHASA rasters in Silver:", lh.nominal_issue_time.dt.strftime('%Y-%m-%d').unique())""",
], "03_data_quality_findings.ipynb")

nb([
    """md:# Workshop #3 — Reading the Gold layer
The Gold DAG (`gold_processing_dag`, PySpark) writes two long-format Parquet files. This notebook reads the latest of
each with pandas and previews what the Workshop #4 dashboard will show.""",
    SETUP,
    """gov_file = sorted(GOLD.glob("governance_*.parquet"))[-1]
story_file = sorted(GOLD.glob("storytelling_*.parquet"))[-1]
gov, story = pd.read_parquet(gov_file), pd.read_parquet(story_file)
print(gov_file.name, gov.shape, "|", story_file.name, story.shape)
gov.dtypes""",
    """md:## Governance KPIs""",
    """gov[gov.kpi != "null_rate"].drop(columns="computed_at_utc")""",
    """gov[(gov.kpi == "null_rate") & (gov.value > 0)].sort_values("value", ascending=False)[["topic", "field", "value", "detail"]]""",
    """md:## Storytelling: risk per monitoring point (US-01, US-02, US-03)""",
    """print(story[story.section == "narrative"].text.iloc[0])
risk = story[story.section == "station_risk"].pivot_table(index="entity", columns="metric", values="value")
risk["level"] = story[(story.section == "station_risk") & (story.metric == "score")].set_index("entity").text
risk.sort_values("score", ascending=False).round(2)""",
    """ax = risk.sort_values("score").score.plot.barh(figsize=(7, 4), color="#1f3a5f")
ax.axvline(0.40, ls="--", color="orange", label="MEDIUM"); ax.axvline(0.70, ls="--", color="red", label="HIGH")
ax.set_title("Landslide Risk Indicator score per monitoring point"); ax.legend(); plt.tight_layout(); plt.show()""",
    """hourly = story[story.section == "hourly_rainfall"].pivot_table(index="period", columns="entity", values="value")
hourly.index = pd.to_datetime(hourly.index)
hourly[risk.sort_values("score").index[-4:]].plot(figsize=(11, 3.5), title="Hourly precipitation, latest forecast poll (mm/h)")
plt.tight_layout(); plt.show()""",
    """md:## Storytelling: historical landslides and text (US-04)""",
    """for sec in ["landslides_by_year", "landslides_by_trigger", "landslides_by_size"]:
    print(story[(story.section == sec)].pivot_table(index="entity", columns="metric", values="value").fillna(0).astype(int).T.to_string(), "\\n")""",
    """kw = story[story.section == "top_keywords"].pivot_table(index="entity", columns="metric", values="value").sort_values("freq", ascending=False)
fig, ax = plt.subplots(1, 2, figsize=(12, 4))
kw.freq.head(15).sort_values().plot.barh(ax=ax[0], color="#1e6e32", title="Top keywords (occurrences)")
story[story.section == "top_bigrams"].set_index("entity").value.head(15).sort_values().plot.barh(ax=ax[1], color="#285a96", title="Top bigrams")
plt.tight_layout(); plt.show()
kw.head(15).round(1)""",
], "04_gold_preview.ipynb")
