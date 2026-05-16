import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from processing.schemas import INCIDENT_SCHEMA
from processing.spark_session import get_mongo_uri

def start_incidents_stream(spark: SparkSession):
    """
    Lit les incidents, écrit une copie brute, et extrait les mots-clés (NLP)
    pour la collection enrichie.
    """
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    # 1. Lecture
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", os.getenv("KAFKA_TOPIC_INCIDENTS", "incident-reports"))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
        
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), INCIDENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp()))
        
    # 2. Flux A : Écriture de la copie brute
    query_raw = (parsed_stream.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/incidents_raw")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "incidents")
        .outputMode("append")
        .start())
        
    # 3. Flux B : NLP (Extraction des mots-clés via MLlib)
    tok = Tokenizer(inputCol="description", outputCol="words")
    sw = StopWordsRemover(inputCol="words", outputCol="tokens")
    
    incidents_nlp = tok.transform(parsed_stream)
    incidents_nlp = sw.transform(incidents_nlp)
    # slice(tokens, 1, 8) garde uniquement les 8 premiers mots (exigence Section 5, Step 8)
    incidents_nlp = incidents_nlp.withColumn("nlp_keywords", expr("slice(tokens, 1, 8)"))
    incidents_nlp = incidents_nlp.drop("words") # On nettoie la colonne temporaire
    
    query_enriched = (incidents_nlp.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/incidents_enriched")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "incidents_enriched")
        .outputMode("append")
        .start())
        
    # On retourne les deux requêtes pour que le main.py puisse les tracker
    return query_raw, query_enriched