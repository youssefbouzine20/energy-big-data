import os
from processing.spark_session import build_session
from processing.streams_weather import start_weather_stream
from processing.streams_external import start_rss_stream, start_market_stream
from processing.streams_incidents import start_incidents_stream
from processing.streams_feedback import start_feedback_stream
from processing.streams_meters import start_meters_stream, read_kafka_stream
from processing.data_quality import start_data_quality_stream
from processing.schemas import METER_SCHEMA

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

    print("Lancement du flux de Qualité des Données...")
    # On récupère le dataframe parsé des compteurs pour le donner au script de qualité
    df_meters_parsed = read_kafka_stream(spark, "KAFKA_TOPIC_METERS", "smart-meters", METER_SCHEMA)
    start_data_quality_stream(spark, df_meters_parsed)

    print("Tous les flux sont démarrés avec succès !")
    print("En écoute continue sur Kafka... (Appuyez sur Ctrl+C pour arrêter)")
    
    # La commande magique exigée par le README : empêche le script de s'arrêter
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()