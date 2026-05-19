"""
processing/data_quality.py
==========================

Per-topic data-quality framework (Section E of the spec).

For each of the 6 ingestion topics we compute, per 15-minute window:
  - temporal_coverage_pct  : received / expected × 100
  - completeness_pct       : % of messages with all vital fields non-null
  - noise_rate_pct         : % of values physically impossible / out-of-bounds
  - anomaly_rate_pct       : % of business-interesting events
  - alert                  : CRIT / WARN string when thresholds breached

All metrics are appended to MongoDB collection `data_quality_metrics`
(one document per (topic, window_start)). The dashboard `/quality` page
groups by topic and shows the latest window for each.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, when, lit, current_timestamp, isnull
)
from processing.spark_session import get_mongo_uri


# ── Expected message count per 15-minute window ───────────────────────────────
# Calibrated to OBSERVED averages from a running pseudo-mode pipeline
# (see audit 2026-05-19). Producer cadences in ingestion/config/kafka_config.py
# define the theoretical max — these constants reflect what Spark actually sees
# end-to-end (including Kafka delivery + watermark + window closure). Adjust
# if you change producer intervals or NUM_METERS.
EXPECTED_PER_15MIN = {
    "smart-meters":     960,  # 20 meters × ~2 batches/min × 15 min × delivery overhead
    "weather":          26,   # 4 zones every 30 s — observed ≈ 26 per closed window
    "incident-reports": 20,   # 1/min × 15 min + occasional bursts
    "rss-feeds":        10,   # 1 / 2 min × 15 min — slightly above 8 due to source bursts
    "market-prices":    1,    # 1/hour — partial coverage expected in 15 min
    "user-feedback":    26,   # 1 / 45 s × 15 min, observed
}


# ── Per-topic quality computation ─────────────────────────────────────────────
# Each function takes a parsed streaming DataFrame, returns a DataFrame with
# columns: topic, window_start, temporal_coverage_pct, completeness_pct,
# noise_rate_pct, anomaly_rate_pct.

def _agg_base(df_parsed):
    """Common watermark + 15-min window groupBy that every topic uses."""
    return (df_parsed
        .withWatermark("timestamp", "2 minutes")
        .groupBy(window(col("timestamp"), "15 minutes")))


def _finalize(agg_df, topic):
    """Project window struct → window_start column, add topic literal."""
    return (agg_df
        .withColumn("topic", lit(topic))
        .withColumn("window_start", col("window.start"))
        .drop("window"))


def _meters_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(isnull("consumption_kwh") | isnull("zone"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when((col("voltage_v") < 210) | (col("voltage_v") > 250), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("is_anomaly") == True, 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "smart-meters")


def _weather_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(isnull("temperature_c") | isnull("station_id"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when(
            (col("temperature_c") < -40) | (col("temperature_c") > 55)
            | (col("humidity_pct") < 0) | (col("humidity_pct") > 100), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("weather_severity").isin("STORM", "EXTREME"), 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "weather")


def _incidents_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(
            isnull("incident_id") | isnull("zone") | isnull("severity"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when(
            (col("estimated_duration_min") < 0) | (col("estimated_duration_min") > 1440), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("severity").isin("HIGH", "CRITICAL"), 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "incident-reports")


def _rss_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(isnull("feed_id") | isnull("title"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when((col("impact_score") < 0) | (col("impact_score") > 1), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("impact_score") > 0.7, 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "rss-feeds")


def _market_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(isnull("price_mad_mwh") | isnull("market"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when((col("price_mad_mwh") < 0) | (col("price_mad_mwh") > 5000), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("trend") == "RISING", 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "market-prices")


def _feedback_quality(df, expected):
    return _finalize(_agg_base(df).agg(
        ((count("*") / lit(expected)) * 100).alias("temporal_coverage_pct"),
        (((count("*") - count(when(
            isnull("feedback_id") | isnull("text") | isnull("sentiment"), 1)))
            / count("*")) * 100).alias("completeness_pct"),
        ((count(when(~col("sentiment").isin("POSITIVE", "NEGATIVE", "NEUTRAL"), 1))
            / count("*")) * 100).alias("noise_rate_pct"),
        ((count(when(col("sentiment") == "NEGATIVE", 1))
            / count("*")) * 100).alias("anomaly_rate_pct"),
    ), "user-feedback")


_COMPUTERS = {
    "smart-meters":     _meters_quality,
    "weather":          _weather_quality,
    "incident-reports": _incidents_quality,
    "rss-feeds":        _rss_quality,
    "market-prices":    _market_quality,
    "user-feedback":    _feedback_quality,
}


def _add_alert(df, topic):
    """Section E business rules. Same thresholds across topics for simplicity."""
    return df.withColumn(
        "alert",
        when(col("temporal_coverage_pct") < 80,
             lit(f"CRIT: {topic} data loss - sensor offline?"))
        .when(col("noise_rate_pct") > 5,
              lit(f"WARN: {topic} high noise rate - calibration needed"))
        .otherwise(lit(None).cast("string"))
    )


def _write_stream(quality_df, topic, db_name, checkpoint_dir):
    safe = topic.replace("-", "_")
    return (quality_df
        .withColumn("computed_at", current_timestamp())
        .writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/data_quality_{safe}")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "data_quality_metrics")
        .outputMode("append")
        .start())


def start_data_quality_stream(spark: SparkSession, df_per_topic):
    """
    Start one streaming query per topic. All 6 streams write to the same
    `data_quality_metrics` collection (distinguished by the `topic` field).

    Parameters
    ----------
    spark : SparkSession
    df_per_topic : dict[str, DataFrame]
        Map from topic name (e.g. "smart-meters") to its parsed input
        streaming DataFrame. Topics absent from this dict are skipped, so
        callers can opt out of any source.

    Returns
    -------
    list[StreamingQuery]
        One handle per topic actually started — useful for testing.
    """
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")

    queries = []
    for topic, df in df_per_topic.items():
        compute = _COMPUTERS.get(topic)
        if compute is None:
            continue
        expected = EXPECTED_PER_15MIN.get(topic, 1)
        q_df = _add_alert(compute(df, expected), topic)
        queries.append(_write_stream(q_df, topic, db_name, checkpoint_dir))
    return queries
