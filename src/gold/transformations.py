"""Silver-to-Gold transformations with PySpark (Workshop #3).

Reads every Silver topic folder as one distributed DataFrame
(spark.read.parquet("datalake_silver/<topic>/"), the cloud-folder read pattern) and builds:

* governance_YYYYMMDD_HHMMSS.parquet   - data-quality KPIs (user story US-06)
* storytelling_YYYYMMDD_HHMMSS.parquet - analytical aggregations for the dashboard (US-01..US-04)

Both are "long" tables with a fixed schema, so one file holds every KPI / aggregation and the
Workshop #4 dashboard can filter them by `kpi` or `section`.
"""

from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import shutil
import sys
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

ROOT = Path(os.environ.get("DATALAKE_ROOT", "/opt/airflow"))
if not (ROOT / "datalake_silver").exists():
    ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "datalake_silver"
GOLD = ROOT / "datalake_gold"

TOPICS = ["rainfall", "rainfall_historical", "landslides", "lhasa_hazard"]

# Required fields and validity rules per topic (the Silver contract)
REQUIRED = {
    "rainfall": ["station_name", "timestamp", "latitude", "longitude", "precipitation_mm"],
    "rainfall_historical": ["station_name", "date_utc", "latitude", "longitude"],
    "landslides": ["event_id", "timestamp_utc", "latitude", "longitude"],
    "lhasa_hazard": ["station_name", "nominal_issue_time", "lhasa_max"],
}
KEYS = {
    "rainfall": ["station_name", "timestamp"],
    "rainfall_historical": ["station_name", "date_utc"],
    "landslides": ["event_id"],
    "lhasa_hazard": ["station_name", "nominal_issue_time"],
}
OUTLIER_FLAGS = {
    "rainfall": ["precip_outlier_iqr", "precip_outlier_zscore"],
    "landslides": ["fatality_outlier_iqr", "fatality_outlier_zscore"],
}
QUALITY_FLAGS = {
    "rainfall": ["soil_moisture_imputed"],
    "rainfall_historical": ["precip_24h_missing"],
    "landslides": ["country_missing", "fatality_missing", "is_time_unknown"],
    "lhasa_hazard": ["has_high_hazard"],
}

# Workshop #3 NLP refinement: words that dominate the GLC narratives without describing the
# event (news-reporting verbs, numbers, distance units, generic place words).
DOMAIN_NOISE = ["said", "near", "km", "one", "two", "three", "four", "five", "least", "also", "according",
                "reported", "local", "area", "province", "department", "colombia", "san", "town", "city",
                "kilometers", "miles", "located", "caused", "occurred", "around", "early", "late"]
EN_MARKERS = ["the", "of", "and", "was", "were", "to", "on", "by", "with", "after"]
ES_MARKERS = ["de", "la", "el", "los", "las", "del", "y", "que", "por", "con"]

# Landslide Risk Indicator (Workshop #1, section 8.3)
W_P24, W_PANT, W_LHASA, W_HIST = 0.35, 0.35, 0.20, 0.10
P24_REF_MM = 75.0
HIST_REF_EVENTS = 5.0
RADIUS_KM = 30.0
ANTECEDENT_DAYS = 5          # a forecast poll covers 2 past + 3 forecast days
ANOMALY_FACTOR = 2.0         # station-year total > 2x the station median -> excluded from percentiles

GOV_SCHEMA = T.StructType([
    T.StructField("kpi", T.StringType()), T.StructField("topic", T.StringType()),
    T.StructField("field", T.StringType()), T.StructField("period", T.StringType()),
    T.StructField("value", T.DoubleType()), T.StructField("unit", T.StringType()),
    T.StructField("detail", T.StringType()),
])
STORY_SCHEMA = T.StructType([
    T.StructField("section", T.StringType()), T.StructField("user_story", T.StringType()),
    T.StructField("entity", T.StringType()), T.StructField("department", T.StringType()),
    T.StructField("latitude", T.DoubleType()), T.StructField("longitude", T.DoubleType()),
    T.StructField("period", T.StringType()), T.StructField("metric", T.StringType()),
    T.StructField("value", T.DoubleType()), T.StructField("text", T.StringType()),
])


# ----------------------------------------------------------------------------------- session
def get_spark(app: str = "landslide-gold") -> SparkSession:
    """Local-mode Spark session sized for the Airflow container."""
    # Spark's Python workers must use the same interpreter as the driver (the Airflow task)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (SparkSession.builder.master("local[*]").appName(app)
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.parquet.mergeSchema", "true")
            .getOrCreate())


def read_silver(spark: SparkSession) -> Dict[str, DataFrame]:
    """Reads each topic folder as a single DataFrame (all accumulated Parquet files)."""
    frames = {}
    for topic in TOPICS:
        folder = SILVER / topic
        if folder.exists() and any(folder.glob("*.parquet")):
            frames[topic] = spark.read.parquet(str(folder))
            logger.info("Read %s: %d files", topic, len(list(folder.glob("*.parquet"))))
    return frames


def _rows(spark, rows: List[tuple], schema) -> DataFrame:
    return spark.createDataFrame(rows, schema)


def _gov(kpi, topic, value, unit, field=None, period=None, detail=None):
    return (kpi, topic, field, period, None if value is None else float(value), unit, detail)


def _haversine_km(lat1, lon1, lat2, lon2):
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def _poll_time(col):
    """Bronze file name ..._YYYYMMDD_HHMMSS.json -> timestamp (the poll/extraction time)."""
    return F.to_timestamp(F.regexp_extract(col, r"(\d{8}_\d{6})\.json$", 1), "yyyyMMdd_HHmmss")


# ------------------------------------------------------------------------- shared derivations
def latest_rainfall(rain: DataFrame) -> DataFrame:
    """Keeps, for every station-hour, the value of the most recent forecast poll."""
    r = rain.withColumn("poll_time", _poll_time("_bronze_source_file"))
    w = Window.partitionBy("station_name", "timestamp").orderBy(F.col("poll_time").desc())
    return r.withColumn("_rk", F.row_number().over(w)).filter("_rk = 1").drop("_rk")


def era5_with_flags(hist: DataFrame) -> DataFrame:
    """Adds 5-day sums computed across years and flags anomalous station-years."""
    w = Window.partitionBy("station_name").orderBy("date_utc").rowsBetween(-(ANTECEDENT_DAYS - 1), 0)
    h = (hist.withColumn("p_ant_mm", F.when(F.count("precip_24h_mm").over(w) == ANTECEDENT_DAYS,
                                            F.sum("precip_24h_mm").over(w)))
         .withColumn("p7d_full_mm", F.sum("precip_24h_mm").over(
             Window.partitionBy("station_name").orderBy("date_utc").rowsBetween(-6, 0))))
    annual = h.groupBy("station_name", "year").agg(F.sum("precip_24h_mm").alias("annual_mm"))
    med = annual.groupBy("station_name").agg(F.percentile_approx("annual_mm", 0.5).alias("median_annual_mm"))
    flags = (annual.join(med, "station_name")
             .withColumn("anomalous_year", F.col("annual_mm") > ANOMALY_FACTOR * F.col("median_annual_mm")))
    return h.join(flags.select("station_name", "year", "annual_mm", "anomalous_year"), ["station_name", "year"])


# --------------------------------------------------------------------------------- governance
def build_governance(spark: SparkSession, frames: Dict[str, DataFrame], run_time: datetime) -> DataFrame:
    rows: List[tuple] = []
    for topic, df in frames.items():
        n = df.count()
        files = df.select("_bronze_source_file").distinct().count()
        rows.append(_gov("volume_rows", topic, n, "rows"))
        rows.append(_gov("volume_bronze_files", topic, files, "files"))
        # Volume per processing day
        for r in df.groupBy(F.date_format("_processed_at_utc", "yyyy-MM-dd").alias("d")).count().collect():
            rows.append(_gov("volume_rows_per_day", topic, r["count"], "rows", period=r["d"]))
        # Null rate per field (empty strings count as null)
        simple = [f.name for f in df.schema.fields if not isinstance(f.dataType, T.ArrayType)]
        exprs = []
        for c in simple:
            dt = df.schema[c].dataType
            cond = F.col(c).isNull() | (F.trim(F.col(c)) == "") if isinstance(dt, T.StringType) else F.col(c).isNull()
            exprs.append(F.sum(cond.cast("int")).alias(c))
        nulls = df.agg(*exprs).collect()[0].asDict()
        for c, k in nulls.items():
            rows.append(_gov("null_rate", topic, 100.0 * k / n if n else 0.0, "%", field=c, detail=f"{k} of {n}"))
        # Duplicate rate on the natural key, before Gold deduplication (after = 0 by construction)
        distinct_keys = df.select(*KEYS[topic]).distinct().count()
        rows.append(_gov("duplicate_rate_before", topic, 100.0 * (n - distinct_keys) / n if n else 0.0, "%",
                         field="+".join(KEYS[topic]), detail=f"{n - distinct_keys} repeated keys of {n}"))
        rows.append(_gov("duplicate_rate_after", topic, 0.0, "%", field="+".join(KEYS[topic]),
                         detail="Gold keeps one row per key" + (" (most recent forecast poll)" if topic == "rainfall" else "")))
        # Schema compliance: required fields present and non-null, values inside physical bounds
        missing_cols = [c for c in REQUIRED[topic] if c not in df.columns]
        ok = F.lit(True)
        for c in REQUIRED[topic]:
            if c in df.columns:
                ok = ok & F.col(c).isNotNull()
        if "latitude" in df.columns:
            ok = ok & F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
        if topic == "rainfall":
            ok = ok & F.col("precipitation_mm").between(0, 300)
        if topic == "lhasa_hazard":
            ok = ok & F.col("lhasa_max").between(0, 1)
        compliant = df.filter(ok).count() if not missing_cols else 0
        rows.append(_gov("schema_compliance_rate", topic, 100.0 * compliant / n if n else 0.0, "%",
                         detail=f"required: {', '.join(REQUIRED[topic])}" + (f"; missing columns {missing_cols}" if missing_cols else "")))
        # Outlier and quality-flag rates
        for flag in OUTLIER_FLAGS.get(topic, []):
            v = df.agg(F.avg(F.col(flag).cast("double"))).collect()[0][0] or 0.0
            rows.append(_gov("outlier_rate", topic, 100.0 * v, "%", field=flag))
        for flag in QUALITY_FLAGS.get(topic, []):
            v = df.agg(F.avg(F.col(flag).cast("double"))).collect()[0][0] or 0.0
            rows.append(_gov("flag_rate", topic, 100.0 * v, "%", field=flag))
        # Freshness: hours since the newest Silver row was processed
        # (computed inside Spark in UTC: collect() would convert timestamps to the local zone)
        run_lit = F.lit(run_time.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
        fr = df.agg(((F.unix_timestamp(run_lit) - F.unix_timestamp(F.max("_processed_at_utc"))) / 3600.0).alias("h"),
                    F.date_format(F.max("_processed_at_utc"), "yyyy-MM-dd HH:mm").alias("last")).collect()[0]
        rows.append(_gov("freshness_hours", topic, fr["h"], "hours", detail=f"last processed {fr['last']} UTC"))

    # Text statistics and language distribution of the landslide narratives
    if "landslides" in frames:
        g = frames["landslides"].withColumn("len_chars", F.length("raw_text_narrative"))
        s = g.agg(F.avg("len_chars").alias("mean"), F.percentile_approx("len_chars", 0.5).alias("median"),
                  F.min("len_chars").alias("min"), F.max("len_chars").alias("max"),
                  F.avg("token_count").alias("tokens")).collect()[0]
        for stat in ("mean", "median", "min", "max"):
            rows.append(_gov(f"text_length_{stat}", "landslides", s[stat], "characters", field="raw_text_narrative"))
        rows.append(_gov("tokens_per_record_mean", "landslides", s["tokens"], "tokens", field="tokens_nlp"))
        words = F.split(F.lower(F.col("raw_text_narrative")), r"[^a-záéíóúñ]+")
        lang = (frames["landslides"]
                .withColumn("en", F.size(F.array_intersect(words, F.array(*[F.lit(w) for w in EN_MARKERS]))))
                .withColumn("es", F.size(F.array_intersect(words, F.array(*[F.lit(w) for w in ES_MARKERS]))))
                .withColumn("lang", F.when(F.col("es") > F.col("en"), "es").when(F.col("en") > 0, "en").otherwise("unknown")))
        total = lang.count()
        for r in lang.groupBy("lang").count().collect():
            rows.append(_gov("language_share", "landslides", 100.0 * r["count"] / total, "%", field="raw_text_narrative",
                             period=None, detail=r["lang"]))

    # Ingestion frequency compliance (expected vs actual polls)
    if "rainfall" in frames:
        polls = frames["rainfall"].select(_poll_time("_bronze_source_file").alias("p")).distinct()
        st = polls.agg(F.date_format(F.min("p"), "yyyy-MM-dd HH:mm").alias("first"),
                       F.date_format(F.max("p"), "yyyy-MM-dd HH:mm").alias("last"),
                       F.countDistinct(F.date_trunc("hour", "p")).alias("actual"),
                       ((F.unix_timestamp(F.date_trunc("hour", F.max("p"))) -
                         F.unix_timestamp(F.date_trunc("hour", F.min("p")))) / 3600 + 1).cast("int").alias("expected")).collect()[0]
        rows.append(_gov("ingestion_compliance", "rainfall", 100.0 * st["actual"] / st["expected"], "%",
                         detail=f"{st['actual']} of {st['expected']} expected hourly polls between {st['first']} and {st['last']} UTC"))
    if "rainfall_historical" in frames:
        years = frames["rainfall_historical"].select("year").distinct().count()
        rows.append(_gov("ingestion_compliance", "rainfall_historical", 100.0 * years / 10, "%", detail=f"{years} of 10 backfill years (2016-2025)"))
        flagged = era5_with_flags(frames["rainfall_historical"]).select("station_name", "year", "annual_mm", "anomalous_year").distinct()
        for r in flagged.filter("anomalous_year").collect():
            rows.append(_gov("anomalous_station_year", "rainfall_historical", r["annual_mm"], "mm/year",
                             field=r["station_name"], period=str(r["year"]), detail="excluded from the percentile baseline"))
        nulls_7d = era5_with_flags(frames["rainfall_historical"]).filter(F.col("p7d_full_mm").isNull()).count()
        rows.append(_gov("null_rate", "rainfall_historical", 0.0, "%", field="p7d (recomputed across years in Gold)",
                         detail=f"{nulls_7d} nulls (Silver computed it per yearly file and left 600)"))

    gov = _rows(spark, rows, GOV_SCHEMA)
    return gov.withColumn("computed_at_utc", F.lit(run_time.replace(tzinfo=None)).cast("timestamp"))


# ------------------------------------------------------------------------------- storytelling
def build_storytelling(spark: SparkSession, frames: Dict[str, DataFrame], run_time: datetime) -> DataFrame:
    parts: List[DataFrame] = []

    def add(df: DataFrame):
        parts.append(df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) if f.name in df.columns
                                 else F.lit(None).cast(f.dataType).alias(f.name) for f in STORY_SCHEMA.fields]))

    rain = latest_rainfall(frames["rainfall"])
    latest_poll = rain.agg(F.max("poll_time")).collect()[0][0]
    current = rain.filter(F.col("poll_time") == F.lit(latest_poll))
    stations = current.groupBy("station_name").agg(F.first("department").alias("department"),
                                                   F.first("latitude").alias("latitude"),
                                                   F.first("longitude").alias("longitude"))
    # P24h: next 24 h of the latest forecast; antecedent: the 5-day window of the poll
    p24 = current.filter((F.col("timestamp") > F.col("poll_time")) &
                         (F.col("timestamp") <= F.col("poll_time") + F.expr("INTERVAL 24 HOURS"))) \
        .groupBy("station_name").agg(F.sum("precipitation_mm").alias("p24h_mm"))
    pant = current.groupBy("station_name").agg(F.sum("precipitation_mm").alias("p_ant_mm"),
                                               F.max("precipitation_mm").alias("max_hourly_mm"))
    # Historical 90th percentile of 5-day rainfall for the same calendar month (ERA5, anomalous years excluded)
    month = latest_poll.month
    hist = era5_with_flags(frames["rainfall_historical"]).filter(~F.col("anomalous_year"))
    clim = hist.filter(F.month("date_utc") == month).groupBy("station_name").agg(
        F.percentile_approx("p_ant_mm", 0.9).alias("p90_ant_hist_mm"),
        F.percentile_approx("p_ant_mm", 0.5).alias("p50_ant_hist_mm"))
    lhasa = frames["lhasa_hazard"]
    lw = Window.partitionBy("station_name").orderBy(F.col("nominal_issue_time").desc())
    lh = lhasa.withColumn("_rk", F.row_number().over(lw)).filter("_rk = 1") \
        .select("station_name", F.col("lhasa_p90").alias("h_lhasa"), "nominal_issue_time")
    ev = frames["landslides"].select("event_id", F.col("latitude").alias("elat"), F.col("longitude").alias("elon"),
                                     "fatality_count", "timestamp_utc")
    near = stations.crossJoin(ev).withColumn("d_km", _haversine_km(F.col("latitude"), F.col("longitude"), F.col("elat"), F.col("elon"))) \
        .filter(F.col("d_km") <= RADIUS_KM)
    near_agg = near.groupBy("station_name").agg(F.count("*").alias("events_30km"),
                                                F.sum("fatality_count").alias("fatalities_30km"),
                                                F.max("timestamp_utc").alias("last_event"))
    risk = (stations.join(p24, "station_name", "left").join(pant, "station_name", "left")
            .join(clim, "station_name", "left").join(lh, "station_name", "left").join(near_agg, "station_name", "left")
            .fillna({"events_30km": 0})
            .withColumn("score", F.lit(W_P24) * F.least(F.lit(1.0), F.col("p24h_mm") / P24_REF_MM)
                        + F.lit(W_PANT) * F.least(F.lit(1.0), F.col("p_ant_mm") / F.col("p90_ant_hist_mm"))
                        + F.lit(W_LHASA) * F.coalesce(F.col("h_lhasa"), F.lit(0.0))
                        + F.lit(W_HIST) * F.least(F.lit(1.0), F.col("events_30km") / HIST_REF_EVENTS))
            .withColumn("level", F.when(F.col("score") >= 0.70, "HIGH").when(F.col("score") >= 0.40, "MEDIUM").otherwise("LOW"))
            .withColumn("ratio_vs_p90", F.col("p_ant_mm") / F.col("p90_ant_hist_mm"))
            .cache())
    period = latest_poll.strftime("%Y-%m-%d %H:%M UTC")
    for metric, us in [("score", "US-01"), ("p24h_mm", "US-02"), ("p_ant_mm", "US-02"), ("max_hourly_mm", "US-02"),
                       ("p90_ant_hist_mm", "US-03"), ("p50_ant_hist_mm", "US-03"), ("ratio_vs_p90", "US-03"),
                       ("h_lhasa", "US-01"), ("events_30km", "US-04"), ("fatalities_30km", "US-04")]:
        add(risk.select(F.lit("station_risk").alias("section"), F.lit(us).alias("user_story"),
                        F.col("station_name").alias("entity"), "department", "latitude", "longitude",
                        F.lit(period).alias("period"), F.lit(metric).alias("metric"),
                        F.col(metric).cast("double").alias("value"), F.col("level").alias("text")))
    # Risk level distribution (the analogue of a sentiment distribution)
    n_st = risk.count()
    for lvl in ["LOW", "MEDIUM", "HIGH"]:
        k = risk.filter(F.col("level") == lvl).count()
        add(_rows(spark, [("risk_level_distribution", "US-01", lvl, None, None, None, period, "stations", float(k),
                           f"{100.0 * k / n_st:.0f}% of {n_st} monitoring points")], STORY_SCHEMA))
    # Hourly rainfall of the latest poll (time series per station)
    add(current.select(F.lit("hourly_rainfall").alias("section"), F.lit("US-02").alias("user_story"),
                       F.col("station_name").alias("entity"), "department", "latitude", "longitude",
                       F.date_format("timestamp", "yyyy-MM-dd'T'HH:mm").alias("period"),
                       F.lit("precipitation_mm").alias("metric"), F.col("precipitation_mm").alias("value"),
                       F.when(F.col("timestamp") > F.col("poll_time"), "forecast").otherwise("observed").alias("text")))
    # Daily totals of the latest poll vs the historical daily P90 for the month
    dclim = hist.filter(F.month("date_utc") == month).groupBy("station_name").agg(
        F.percentile_approx("precip_24h_mm", 0.9).alias("p90_daily"))
    daily = current.groupBy("station_name", F.to_date("timestamp").alias("day")).agg(F.sum("precipitation_mm").alias("mm")) \
        .join(dclim, "station_name")
    add(daily.select(F.lit("daily_rain_vs_history").alias("section"), F.lit("US-03").alias("user_story"),
                     F.col("station_name").alias("entity"), F.date_format("day", "yyyy-MM-dd").alias("period"),
                     F.lit("daily_mm").alias("metric"), F.col("mm").alias("value"),
                     F.format_string("historical daily P90 for the month: %.1f mm", F.col("p90_daily")).alias("text")))
    # Monthly climatology per station (ERA5 2016-2025)
    mclim = hist.groupBy("station_name", F.month("date_utc").alias("m")).agg(
        F.avg("precip_24h_mm").alias("mean_daily_mm"), F.percentile_approx("p_ant_mm", 0.9).alias("p90_5d_mm"))
    for metric in ["mean_daily_mm", "p90_5d_mm"]:
        add(mclim.select(F.lit("monthly_climatology").alias("section"), F.lit("US-03").alias("user_story"),
                         F.col("station_name").alias("entity"), F.format_string("%02d", "m").alias("period"),
                         F.lit(metric).alias("metric"), F.col(metric).alias("value")))
    # Historical landslides: per year, trigger, size and department (with known fatalities)
    gl = frames["landslides"].withColumn("year", F.year("timestamp_utc"))
    for section, col in [("landslides_by_year", "year"), ("landslides_by_trigger", "landslide_trigger"),
                         ("landslides_by_size", "landslide_size"), ("landslides_by_department", "admin_division_name")]:
        agg = gl.groupBy(F.col(col).cast("string").alias("entity")).agg(
            F.count("*").alias("events"), F.sum("fatality_count").alias("fatalities"),
            F.sum(F.col("fatality_missing").cast("int")).alias("unknown"))
        for metric in ["events", "fatalities"]:
            add(agg.select(F.lit(section).alias("section"), F.lit("US-04").alias("user_story"), "entity",
                           F.lit(metric).alias("metric"), F.col(metric).cast("double").alias("value"),
                           F.format_string("%d events without a fatality count", "unknown").alias("text")))
    # Landslides within 30 km of each monitoring point
    add(near.select(F.lit("landslides_near_station").alias("section"), F.lit("US-04").alias("user_story"),
                    F.col("station_name").alias("entity"), F.col("elat").alias("latitude"), F.col("elon").alias("longitude"),
                    F.date_format("timestamp_utc", "yyyy-MM-dd").alias("period"), F.lit("distance_km").alias("metric"),
                    F.col("d_km").alias("value"),
                    F.concat(F.lit("event "), F.col("event_id").cast("string"), F.lit(", fatalities: "),
                             F.coalesce(F.col("fatality_count").cast("string"), F.lit("unknown"))).alias("text")))
    # Keywords and bigrams (refined stop-word list) with their association to fatalities
    noise = F.array(*[F.lit(w) for w in DOMAIN_NOISE])
    # filter() keeps repeated words and word order (array_except would de-duplicate them)
    tok = frames["landslides"].select("event_id", "fatality_count",
                                      F.filter(F.col("tokens_nlp"), lambda x: ~F.array_contains(noise, x)).alias("t"))
    words = tok.select("event_id", "fatality_count", F.explode("t").alias("w")).filter(F.length("w") > 2)
    kw = words.groupBy("w").agg(F.count("*").alias("freq"), F.countDistinct("event_id").alias("events"),
                                F.avg("fatality_count").alias("avg_fatalities")) \
        .orderBy(F.col("freq").desc(), "w").limit(25)
    for metric in ["freq", "avg_fatalities"]:
        add(kw.select(F.lit("top_keywords").alias("section"), F.lit("US-04").alias("user_story"),
                      F.col("w").alias("entity"), F.lit(metric).alias("metric"), F.col(metric).cast("double").alias("value"),
                      F.format_string("in %d events", "events").alias("text")))
    bi = tok.filter(F.size("t") > 1).select(
        F.explode(F.expr("transform(sequence(0, size(t) - 2), i -> concat(t[i], ' ', t[i + 1]))")).alias("b"))
    big = bi.groupBy("b").count().orderBy(F.col("count").desc(), "b").limit(20)
    add(big.select(F.lit("top_bigrams").alias("section"), F.lit("US-04").alias("user_story"), F.col("b").alias("entity"),
                   F.lit("freq").alias("metric"), F.col("count").cast("double").alias("value")))
    # Narrative: the key insight of the dashboard, computed from the data
    top = risk.orderBy(F.col("score").desc()).first()
    wettest = risk.orderBy(F.col("ratio_vs_p90").desc_nulls_last()).first()
    insight = (f"Latest forecast ({period}): {n_st} monitoring points; highest risk at {top['station_name']} "
               f"(score {top['score']:.2f}, {top['level']}). Rain relative to history is highest at {wettest['station_name']}: "
               f"{wettest['p_ant_mm']:.0f} mm in 5 days = {wettest['ratio_vs_p90']:.0%} of the November 5-day P90 "
               f"({wettest['p90_ant_hist_mm']:.0f} mm).")
    add(_rows(spark, [("narrative", "US-01", "key_insight", None, None, None, period, "text", None, insight)], STORY_SCHEMA))

    story = parts[0]
    for p in parts[1:]:
        story = story.unionByName(p)
    return story.withColumn("computed_at_utc", F.lit(run_time.replace(tzinfo=None)).cast("timestamp"))


# ----------------------------------------------------------------------------------- persist
def write_gold(df: DataFrame, name: str, run_time: datetime, out_dir: Optional[Path] = None) -> Path:
    """Writes a single Parquet file datalake_gold/<name>_YYYYMMDD_HHMMSS.parquet with Spark."""
    out_dir = out_dir or GOLD
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"{name}_{run_time:%Y%m%d_%H%M%S}.parquet"
    tmp = out_dir / f"_tmp_{name}_{run_time:%Y%m%d_%H%M%S}"
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    part = next(tmp.glob("part-*.parquet"))
    if target.exists():
        raise FileExistsError(target)
    shutil.move(str(part), str(target))
    shutil.rmtree(tmp)
    logger.info("Gold file written: %s", target)
    return target


def run(run_time: Optional[datetime] = None) -> Dict[str, str]:
    run_time = run_time or datetime.now(timezone.utc)
    spark = get_spark()
    try:
        frames = read_silver(spark)
        gov = build_governance(spark, frames, run_time)
        story = build_storytelling(spark, frames, run_time)
        return {"governance": str(write_gold(gov, "governance", run_time)),
                "storytelling": str(write_gold(story, "storytelling", run_time))}
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())
