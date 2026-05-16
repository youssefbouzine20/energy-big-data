import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from processing.schemas import FEEDBACK_SCHEMA
from processing.spark_session import get_mongo_uri

# Instance globale partagée sur le worker pour éviter la surcharge mémoire
_sia = None

def analyze_sentiment(text):
    global _sia
    if not text:
        return "NEUTRAL"
    
    if _sia is None:
        from nltk.sentiment.vader import SentimentIntensityAnalyzer
        _sia = SentimentIntensityAnalyzer()
        
    score = _sia.polarity_scores(text)["compound"]
    if score > 0.2:
        return "POSITIVE"
    elif score < -0.2:
        return "NEGATIVE"
    else:
        return "NEUTRAL"

sentiment_udf = udf(analyze_sentiment, StringType())

def start_feedback_stream(spark: SparkSession):
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
    
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", os.getenv("KAFKA_TOPIC_FEEDBACK", "user-feedback"))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
        
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), FEEDBACK_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("computed_at", current_timestamp()))
        
    feedback_prepared = parsed_stream.withColumnRenamed("sentiment", "sentiment_input")
    feedback_with_sent = feedback_prepared.withColumn("sentiment_predicted", sentiment_udf(col("text")))
    
    tok = Tokenizer(inputCol="text", outputCol="words")
    sw = StopWordsRemover(inputCol="words", outputCol="tokens")
    
    feedback_tokenized = tok.transform(feedback_with_sent)
    feedback_final = sw.transform(feedback_tokenized).drop("words")
    
    return (feedback_final.writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{checkpoint_dir}/feedback_nlp")
        .option("spark.mongodb.connection.uri", get_mongo_uri())
        .option("spark.mongodb.database", db_name)
        .option("spark.mongodb.collection", "feedback_nlp")
        .outputMode("append")
        .start())