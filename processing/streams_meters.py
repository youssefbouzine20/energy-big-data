import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, window, avg, max as _max,
    min as _min, sum as _sum, count, when, stddev, approx_count_distinct,
    first, last, lit
)
from processing.schemas import METER_SCHEMA, WEATHER_SCHEMA, INCIDENT_SCHEMA
from processing.spark_session import get_mongo_uri

def read_kafka_stream(spark, topic_var, default_topic, schema):
    """Fonction utilitaire locale pour lire un topic Kafka et parser le JSON"""
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
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    # 1. LECTURE DES FLUX NÉCESSAIRES
    parsed_meters = read_kafka_stream(spark, "KAFKA_TOPIC_METERS", "smart-meters", METER_SCHEMA)
    parsed_weather = read_kafka_stream(spark, "KAFKA_TOPIC_WEATHER", "weather", WEATHER_SCHEMA)
    parsed_incidents = read_kafka_stream(spark, "KAFKA_TOPIC_INCIDENTS", "incident-reports", INCIDENT_SCHEMA)

    # 2. FLUX A : SAUVEGARDE DES DONNÉES BRUTES (Pour le ML)
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

    # 3. FLUX B : PREPARATION DES AGREGATIONS (On garde l'objet 'window' intact)
    meters_watermarked = parsed_meters.withWatermark("timestamp", "2 minutes")
    
    windowed_meters = (meters_watermarked
        .groupBy(window(col("timestamp"), "15 minutes"), col("zone"))
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
            first("zone_lon").alias("zone_lon")
        )
        .withColumn("anomaly_rate_pct", (col("anomaly_count") / col("reading_count")) * 100))

    # 4. JOINTURE AVEC LA METEO (stream-stream join sur fenêtre 15 min alignée)
    # broadcast() est interdit sur un DataFrame streamé — join stream-stream standard.
    latest_weather = (parsed_weather
        .withWatermark("timestamp", "5 minutes")
        .groupBy(window(col("timestamp"), "15 minutes")) # Aligné sur 15 min pour la jointure
        .agg(
            last("temperature_c").alias("weather_temperature_c"),
            last("weather_severity").alias("weather_severity")
        ))

    # Jointure directe sur l'objet structure 'window' de Spark
    aggregated = windowed_meters.join(
        latest_weather,
        windowed_meters.window == latest_weather.window,
        "left"
    ).drop(latest_weather.window) # On évite la collision de colonnes

    # 5. JOINTURE AVEC LES INCIDENTS ACTIFS (Sur la zone ET la fenêtre)
    incident_counts = (parsed_incidents
        .withWatermark("timestamp", "5 minutes")
        .groupBy(window(col("timestamp"), "15 minutes"), col("zone"))
        .agg(count("*").alias("active_incidents")))

    aggregated_final = aggregated.join(
        incident_counts,
        (aggregated.window == incident_counts.window) & (aggregated.zone == incident_counts.zone), 
        "left"
    ).drop(incident_counts.window, incident_counts.zone)

    # 6. ENRICHISSEMENT FINAL ET FORMATAGE DES COLONNES POUR MONGODB
    aggregated_final = (aggregated_final
        .withColumn("active_incidents", when(col("active_incidents").isNull(), 0).otherwise(col("active_incidents")))
        # Extraction finale demandée par le schéma 2.2 du README
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("computed_at", current_timestamp()))

    # 7. ÉCRITURE DANS MONGODB (Le mode Append est requis suite aux jointures de flux)
    query_agg = (aggregated_final
        .writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/meters_agg")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "meters_aggregated_15min")
        .outputMode("append")
        .start())

    return query_raw, query_agg