import os
from processing.spark_session import build_session
from processing.streams_weather import start_weather_stream
from processing.streams_external import start_rss_stream, start_market_stream
from processing.streams_incidents import start_incidents_stream
from processing.streams_feedback import start_feedback_stream
from processing.streams_meters import start_meters_stream, read_kafka_stream
from processing.data_quality import start_data_quality_stream
from processing.schemas import (
    METER_SCHEMA, WEATHER_SCHEMA, INCIDENT_SCHEMA,
    RSS_SCHEMA, MARKET_SCHEMA, FEEDBACK_SCHEMA,
)


def main():
    print("Initialisation de la session Spark...")
    spark = build_session("energy-streaming-main")

    print("Lancement des flux Passthrough (Météo, RSS, Marché)...")
    start_weather_stream(spark)
    start_rss_stream(spark)
    start_market_stream(spark)

    print("Lancement des flux NLP (Incidents, Feedback)...")
    start_incidents_stream(spark)
    start_feedback_stream(spark)

    print("Lancement du flux principal (Compteurs & Agrégations 15min)...")
    start_meters_stream(spark)

    print("Lancement du flux de Qualité des Données (6 topics)...")
    # On parse séparément chaque topic pour le module qualité.
    # Spark ouvre des consumer groups distincts pour ne pas concurrencer les
    # streams de traitement principaux.
    df_per_topic = {
        "smart-meters":     read_kafka_stream(spark, "KAFKA_TOPIC_METERS",    "smart-meters",     METER_SCHEMA),
        "weather":          read_kafka_stream(spark, "KAFKA_TOPIC_WEATHER",   "weather",          WEATHER_SCHEMA),
        "incident-reports": read_kafka_stream(spark, "KAFKA_TOPIC_INCIDENTS", "incident-reports", INCIDENT_SCHEMA),
        "rss-feeds":        read_kafka_stream(spark, "KAFKA_TOPIC_RSS",       "rss-feeds",        RSS_SCHEMA),
        "market-prices":    read_kafka_stream(spark, "KAFKA_TOPIC_PRICES",    "market-prices",    MARKET_SCHEMA),
        "user-feedback":    read_kafka_stream(spark, "KAFKA_TOPIC_FEEDBACK",  "user-feedback",    FEEDBACK_SCHEMA),
    }
    start_data_quality_stream(spark, df_per_topic)

    print("Tous les flux sont démarrés avec succès !")
    print("En écoute continue sur Kafka... (Appuyez sur Ctrl+C pour arrêter)")

    # La commande magique exigée par le README : empêche le script de s'arrêter
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
