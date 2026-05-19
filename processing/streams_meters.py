import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, avg, max as _max,
    min as _min, sum as _sum, count, when, stddev, approx_count_distinct,
    first, lit
)
from processing.schemas import METER_SCHEMA
from processing.spark_session import get_mongo_uri


def read_kafka_stream(spark, topic_var, default_topic, schema):
    """Lit un topic Kafka et parse le JSON en DataFrame structuré."""
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = os.getenv(topic_var, default_topic)

    raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    return (raw.selectExpr("CAST(value AS STRING) as json_value")
               .select(from_json(col("json_value"), schema).alias("data"))
               .select("data.*"))


def start_meters_stream(spark: SparkSession):
    """
    Lance deux requêtes streaming sur le topic smart-meters :
      A) meters_raw : append brut, alimente le ML batch (P5) et les KPI temps réel.
      B) meters_aggregated_15min : agrégat par (fenêtre, zone) avec stats + anomalies.

    Architecture : on n'effectue PAS de stream-stream join avec weather/incidents
    ici. Raison : les joins entre streams agrégés en mode append sont fragiles
    sous Spark 3.5 (les fenêtres se ferment, les watermarks avancent, mais les
    lignes ne sont jamais émises en sortie quand on chaîne deux joins). Le
    croisement météo + incidents se fait côté MongoDB via $lookup à la lecture
    (dashboard backend), ce qui découple la complexité stateful du streaming
    de l'analytique. Spark gère la vélocité, Mongo gère la variété de la
    jointure — séparation des responsabilités classique pour les pipelines
    temps réel en production.
    """
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")

    # Window + watermark paramétrables : production = "15 minutes" / "2 minutes"
    # (intervalle de règlement du marché européen). Démo = "2 minutes" /
    # "1 minute" pour voir les fenêtres se fermer pendant la présentation.
    agg_window = os.getenv("AGGREGATION_WINDOW", "15 minutes")
    agg_watermark = os.getenv("AGGREGATION_WATERMARK", "2 minutes")

    # ─── Lecture du topic smart-meters ───────────────────────────────────────
    parsed_meters = read_kafka_stream(spark, "KAFKA_TOPIC_METERS", "smart-meters", METER_SCHEMA)

    # ─── A. meters_raw : append brut ─────────────────────────────────────────
    query_raw = (parsed_meters
        .withColumn("ingested_at", current_timestamp())
        .writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/meters_raw")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "meters_raw")
        .outputMode("append")
        .start())

    # ─── B. meters_aggregated_15min : fenêtre × zone ─────────────────────────
    meters_watermarked = parsed_meters.withWatermark("timestamp", agg_watermark)

    aggregated = (meters_watermarked
        .groupBy(window(col("timestamp"), agg_window), col("zone"))
        .agg(
            avg("consumption_kwh").alias("avg_consumption"),
            _max("consumption_kwh").alias("max_consumption"),
            _min("consumption_kwh").alias("min_consumption"),
            _sum("consumption_kwh").alias("total_consumption_kwh"),
            _sum(when(col("is_anomaly") == True, 1).otherwise(0)).alias("anomaly_count"),
            count("*").alias("reading_count"),
            _min("voltage_v").alias("voltage_min"),
            _max("voltage_v").alias("voltage_max"),
            avg("voltage_v").alias("voltage_avg"),
            stddev("frequency_hz").alias("frequency_stddev"),
            approx_count_distinct("meter_id").alias("meter_count"),
            first("zone_lat").alias("zone_lat"),
            first("zone_lon").alias("zone_lon"),
        )
        .withColumn("anomaly_rate_pct", (col("anomaly_count") / col("reading_count")) * 100)
        .withColumn("active_incidents", lit(0))  # placeholder ; rempli au query-time via Mongo $lookup
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("computed_at", current_timestamp()))

    query_agg = (aggregated
        .writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/meters_agg")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "meters_aggregated_15min")
        .outputMode("append")
        .start())

    return query_raw, query_agg
