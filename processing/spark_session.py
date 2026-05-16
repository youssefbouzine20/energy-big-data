import os
from pyspark.sql import SparkSession

# python-dotenv is present in the host venv but NOT inside the apache/spark
# Docker image. When running in-container, env vars come from docker-compose's
# `environment:` block, so dotenv is unnecessary. Skip gracefully if missing.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def get_mongo_uri() -> str:
    """
    Construit l'URI MongoDB en respectant les consignes de la Section 4 du README.
    Utilise 'localhost' par défaut pour le développement local hors Docker.
    """
    host = os.getenv("MONGO_HOST", "localhost") 
    port = os.getenv("MONGO_PORT", "27017")
    user = os.getenv("MONGO_SPARK_USER", "spark_writer")
    password = os.getenv("MONGO_SPARK_PASS", "change-me-before-deploy")
    db_name = os.getenv("MONGO_DB_NAME", "energy_db")
    
    return f"mongodb://{user}:{password}@{host}:{port}/{db_name}?authSource=admin"

def build_session(app_name: str = "energy-streaming") -> SparkSession:
    """
    Initialise Spark avec les connecteurs Kafka et MongoDB (Option A de la Section 3).
    """
    master = os.getenv("SPARK_MASTER", "local[*]")
    
    # Packages exacts demandés dans le README
    packages = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )

    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.jars.packages", packages)
             .config("spark.mongodb.write.batchSize", "1000")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    return spark