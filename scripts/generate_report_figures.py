"""Generate high-resolution academic figures for the Workshop #2 LaTeX report."""

from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
FIG_DIR = ROOT / "workshop_2" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR = ROOT / "datalake_silver"

# Set academic plot style
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.labelsize": 11,
    "axes.titlesize": 12,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "figure.titlesize": 13,
})

# 1. Figure: Precipitation Distribution and Station Means
rain_files = sorted(list(SILVER_DIR.glob("rainfall_2*.parquet")))  # excludes rainfall_historical_*
if rain_files:
    df_rain = pd.read_parquet(rain_files[-1])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), dpi=300)

    # Histogram of precipitation
    axes[0].hist(df_rain["precipitation_mm"], bins=30, color="#14325f", edgecolor="black", alpha=0.85)
    axes[0].set_title("(a) Hourly Precipitation Distribution (mm)")
    axes[0].set_xlabel("Precipitation (mm/h)")
    axes[0].set_ylabel("Frequency (hours)")
    axes[0].grid(True, linestyle="--", alpha=0.5)

    # Mean precipitation by station
    station_means = df_rain.groupby("station_name", observed=True)["precipitation_mm"].mean().sort_values()
    axes[1].barh(station_means.index.astype(str), station_means.values, color="#285a96", edgecolor="black", alpha=0.85)
    axes[1].set_title("(b) Mean Hourly Precipitation by Station (mm)")
    axes[1].set_xlabel("Mean Precipitation (mm/h)")
    axes[1].grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    fig_path = FIG_DIR / "rainfall_analysis.png"
    plt.savefig(fig_path, bbox_inches="tight")
    plt.close()
    print(f"Generated {fig_path.name}")

# 2. Figure: Top NLP Keywords in Landslide Narratives
landslide_files = sorted(list(SILVER_DIR.glob("landslides_*.parquet")))
if landslide_files:
    df_landslides = pd.read_parquet(landslide_files[-1])
    all_tokens = []
    for token_list in df_landslides["tokens_nlp"]:
        if isinstance(token_list, (list, tuple, np.ndarray)):
            all_tokens.extend(token_list)

    # Filter out generic news narrative reporting words if any
    reporting_stopwords = {"said", "two", "least", "one", "three", "first", "also", "new"}
    domain_tokens = [t for t in all_tokens if t not in reporting_stopwords]

    token_counts = Counter(domain_tokens).most_common(15)
    words = [w[0] for w in token_counts][::-1]
    counts = [w[1] for w in token_counts][::-1]

    plt.figure(figsize=(8, 4.5), dpi=300)
    bars = plt.barh(words, counts, color="#1e6e32", edgecolor="black", alpha=0.85)
    plt.title("Top 15 Descriptive Keywords in Colombian Landslide Reports (NLP Cleaned)")
    plt.xlabel("Frequency Across Historical Catalog (2007–2017)")
    plt.ylabel("Token")
    plt.grid(True, linestyle="--", alpha=0.5)

    # Add numeric labels at the end of each bar
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1.5, bar.get_y() + bar.get_height() / 2, f"{int(width)}",
                 va="center", ha="left", fontsize=8.5, color="#222")

    plt.xlim(0, max(counts) * 1.12)
    plt.tight_layout()
    fig_path = FIG_DIR / "nlp_keywords.png"
    plt.savefig(fig_path, bbox_inches="tight")
    plt.close()
    print(f"Generated {fig_path.name} with {len(words)} bars.")

# 3. Figure: End-to-End Pipeline Architecture Diagram
fig, ax = plt.subplots(figsize=(12, 5.2), dpi=300)
ax.axis("off")

# Draw boxes for layers
box_props_src = dict(boxstyle="round,pad=0.5", facecolor="#e6f0fa", edgecolor="#14325f", linewidth=1.5)
box_props_dag1 = dict(boxstyle="round,pad=0.5", facecolor="#fff3e6", edgecolor="#b85d19", linewidth=1.5)
box_props_bronze = dict(boxstyle="round,pad=0.5", facecolor="#f5e6d3", edgecolor="#8c531b", linewidth=1.5)
box_props_sensor = dict(boxstyle="round,pad=0.4", facecolor="#f0f4f8", edgecolor="#50667a", linewidth=1.2, linestyle="--")
box_props_dag2 = dict(boxstyle="round,pad=0.5", facecolor="#e8f4ec", edgecolor="#1e6e32", linewidth=1.5)
box_props_silver = dict(boxstyle="round,pad=0.5", facecolor="#eef2f5", edgecolor="#4a5568", linewidth=1.5)
box_props_gold = dict(boxstyle="round,pad=0.5", facecolor="#fef9e7", edgecolor="#b7950b", linewidth=1.5, linestyle=":")
box_props_dash = dict(boxstyle="round,pad=0.5", facecolor="#fbeee6", edgecolor="#a04000", linewidth=1.5, linestyle=":")

# Upper Row: Ingestion into Bronze
ax.text(0.08, 0.78, "External Sources\n\n• Open-Meteo Forecast\n• Open-Meteo ERA5 (Archive)\n• NASA LHASA Portal\n• NASA GLC Catalog",
        ha="center", va="center", bbox=box_props_src, fontsize=8)

ax.text(0.30, 0.78, "Airflow Ingestion DAGs\n\n• bronze_ingestion_dag (@hourly)\n• bronze_historical_backfill_dag\n• Validates schemas & bbox\n• Skips unchanged payloads\n• Emits _READY.flag",
        ha="center", va="center", bbox=box_props_dag1, fontsize=8)

ax.text(0.53, 0.78, "datalake_bronze/ (Raw)\n\n• openmeteo_forecast_*.json\n• openmeteo_era5_*.json\n• nasa_lhasa_*.json (+ station hazards)\n• nasa_glc_*.json\n• _READY.flag (witness)",
        ha="center", va="center", bbox=box_props_bronze, fontsize=8)

# Intermediate Sensor
ax.text(0.53, 0.40, "Airflow FileSensor (Event-Driven)\nDetects _READY.flag\nmode='reschedule', poke=30s\nConsumes the flag, then processes\nfiles missing from the manifest",
        ha="center", va="center", bbox=box_props_sensor, fontsize=7.5)

# Lower Row: Silver, Gold, Dashboard
ax.text(0.30, 0.18, "Silver Processing DAG\n(Pandas / PyArrow)\n\n• Type casting & UTC dates\n• Short-gap soil moisture interpolation\n• Missing values flagged (no 0 / no country)\n• NLP normalization on narratives\n• Per-station wet-hour IQR/Z outliers\n• One Parquet per topic and run",
        ha="center", va="center", bbox=box_props_dag2, fontsize=7.5)

ax.text(0.53, 0.18, "datalake_silver/ (Parquet)\n\n• rainfall_*.parquet (Hourly)\n• rainfall_historical_*.parquet\n• lhasa_hazard_*.parquet\n• landslides_*.parquet\n• Lineage: Bronze file + row hash",
        ha="center", va="center", bbox=box_props_silver, fontsize=7.5)

ax.text(0.76, 0.18, "Gold Layer (Planned - W#3)\n(Apache PySpark)\n\n• 0.25° grid spatial joins\n• 24h & 7d antecedent rain\n• Historical ERA5 percentiles\n• Landslide Risk Indicator (LRI)",
        ha="center", va="center", bbox=box_props_gold, fontsize=7.5)

ax.text(0.94, 0.18, "Dashboard (Planned - W#4)\n(PostGIS + Streamlit)\n\n• Real-time alert maps\n• Multi-hazard spatial index\n• Risk visualization for UNGRD",
        ha="center", va="center", bbox=box_props_dash, fontsize=7.5)

# Connectors
arrow = dict(arrowstyle="->", lw=1.4, color="#14325f")
dash_arrow = dict(arrowstyle="->", lw=1.3, color="#50667a", linestyle="--")
planned_arrow = dict(arrowstyle="->", lw=1.3, color="#b7950b", linestyle=":")

ax.annotate("", xy=(0.19, 0.78), xytext=(0.16, 0.78), arrowprops=arrow)
ax.annotate("", xy=(0.42, 0.78), xytext=(0.40, 0.78), arrowprops=arrow)
ax.annotate("", xy=(0.53, 0.48), xytext=(0.53, 0.62), arrowprops=dash_arrow)
ax.annotate("", xy=(0.39, 0.18), xytext=(0.53, 0.32), arrowprops=dash_arrow)
ax.annotate("", xy=(0.44, 0.18), xytext=(0.41, 0.18), arrowprops=arrow)
ax.annotate("", xy=(0.67, 0.18), xytext=(0.62, 0.18), arrowprops=planned_arrow)
ax.annotate("", xy=(0.87, 0.18), xytext=(0.85, 0.18), arrowprops=planned_arrow)

plt.tight_layout()
fig_arch_path = FIG_DIR / "pipeline_architecture.png"
plt.savefig(fig_arch_path, bbox_inches="tight")
plt.close()
print(f"Generated {fig_arch_path.name}")
