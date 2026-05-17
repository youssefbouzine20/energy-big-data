import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, when, lit, current_timestamp, 
    isnull, min as _min, max as _max
)
from processing.spark_session import get_mongo_uri

# Nombres de messages attendus par fenêtre de 15 minutes selon les fréquences de P1.
# smart-meters: NUM_METERS msgs/batch × 1 batch/30s = 2 batches/min × 15 min
# Défaut NUM_METERS=10 → 10 × 2 × 15 = 300. Lit NUM_METERS depuis l'env pour s'adapter.
_NUM_METERS = int(os.getenv("NUM_METERS", 10))
EXPECTED_PER_15MIN = {
    "smart-meters": _NUM_METERS * 30,  # dynamique selon NUM_METERS
    "weather": 30,                     # 2 msgs/min * 15 min
    "incident-reports": 15,            # 1 msg/min * 15 min
    "user-feedback": 20                # 1 msg/45s ≈ 1.33/min * 15 min
}

def compute_meters_quality(df_meters_parsed):
    """
    Calcule les métriques de qualité (DQ) sur des fenêtres de 15 minutes
    spécifiquement pour le topic des compteurs intelligents.
    """
    return (df_meters_parsed
        .withWatermark("timestamp", "2 minutes")
        .groupBy(window(col("timestamp"), "15 minutes"))
        .agg(
            # 1. Couverture temporelle : (Reçus / Attendus) * 100
            ((count("*") / lit(EXPECTED_PER_15MIN["smart-meters"])) * 100).alias("temporal_coverage_pct"),
            
            # 2. Complétude : % de messages où les champs vitaux sont présents
            (((count("*") - count(when(isnull("consumption_kwh") | isnull("zone"), 1))) / count("*")) * 100).alias("completeness_pct"),
            
            # 3. Taux de Bruit : % de valeurs physiquement impossibles/hors normes (ex: tension hors [210, 250])
            ((count(when((col("voltage_v") < 210) | (col("voltage_v") > 250), 1)) / count("*")) * 100).alias("noise_rate_pct"),
            
            # 4. Taux d'anomalies : % de messages flaggés comme anomalie
            ((count(when(col("is_anomaly") == True, 1)) / count("*")) * 100).alias("anomaly_rate_pct")
        )
        .withColumn("topic", lit("smart-meters"))
        .withColumn("window_start", col("window.start"))
        .drop("window"))

def start_data_quality_stream(spark: SparkSession, df_meters_parsed):
    """
    Lance l'évaluation de la qualité en continu et écrit dans MongoDB.
    """
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    quality_df = compute_meters_quality(df_meters_parsed)
    
    # 5. Règle d'Alerte Métier : Si on perd plus de 20% des paquets ou trop de bruit
    quality_df = quality_df.withColumn(
        "alert",
        when(col("temporal_coverage_pct") < 80, lit("CRIT: Data loss detected - Sensor offline?"))
        .when(col("noise_rate_pct") > 5, lit("WARN: High noise rate - Calibration needed"))
        .otherwise(lit(None))
    )
    
    return (quality_df
        .withColumn("computed_at", current_timestamp())
        .writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/data_quality")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "data_quality_metrics")
        .outputMode("append") # Update car on modifie la fenêtre en cours
        .start())