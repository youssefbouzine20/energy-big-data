import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from processing.schemas import RSS_SCHEMA, MARKET_SCHEMA
from processing.spark_session import get_mongo_uri

def start_rss_stream(spark: SparkSession):
    """Flux passthrough pour les actualités RSS."""
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", os.getenv("KAFKA_TOPIC_RSS", "rss-feeds"))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
        
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), RSS_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp()))
        
    return (parsed_stream.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/rss_feeds")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "rss_feeds")
        .outputMode("append")
        .start())

def start_market_stream(spark: SparkSession):
    """Flux passthrough pour les prix du marché."""
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", os.getenv("KAFKA_TOPIC_PRICES", "market-prices"))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
        
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), MARKET_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp()))
        
    return (parsed_stream.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/market_prices")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "market_prices")
        .outputMode("append")
        .start())