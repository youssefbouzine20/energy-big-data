import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from processing.schemas import WEATHER_SCHEMA
from processing.spark_session import get_mongo_uri

def start_weather_stream(spark: SparkSession):
    """
    Lit le flux météo et l'écrit directement dans MongoDB (Passthrough).
    """
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = os.getenv("KAFKA_TOPIC_WEATHER", "weather")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    # 1. Lecture depuis Kafka
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false") # Requis par le README
        .load())
        
    # 2. Parsing du JSON avec le StructType
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), WEATHER_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())) # Ajout d'un timestamp d'ingestion
        
    # 3. Écriture vers MongoDB en mode 'append'
    return (parsed_stream.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/weather")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "weather")
        .outputMode("append")
        .start())