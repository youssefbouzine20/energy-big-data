from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType, ArrayType
)

# 1.1 smart-meters
METER_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("meter_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("zone", StringType(), True),
    StructField("consumption_kwh", DoubleType(), True),
    StructField("voltage_v", DoubleType(), True),
    StructField("frequency_hz", DoubleType(), True),
    StructField("power_factor", DoubleType(), True),
    StructField("is_anomaly", BooleanType(), True),
    StructField("anomaly_reason", StringType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_peak_hour", BooleanType(), True),
    StructField("zone_lat", DoubleType(), True),
    StructField("zone_lon", DoubleType(), True)
])

# 1.2 weather
WEATHER_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("feels_like_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("solar_irradiance_wm2", DoubleType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
    StructField("weather_severity", StringType(), True)
])

# 1.3 incident-reports
INCIDENT_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("incident_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("zone", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("affected_meters", ArrayType(StringType()), True),
    StructField("estimated_duration_min", IntegerType(), True),
    StructField("resolved", BooleanType(), True)
])

# 1.4 rss-feeds
RSS_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("feed_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("source", StringType(), True),
    StructField("category", StringType(), True),
    StructField("title", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("impact_score", DoubleType(), True),
    StructField("keywords", ArrayType(StringType()), True)
])

# 1.5 market-prices
MARKET_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("market", StringType(), True),
    StructField("price_mad_mwh", DoubleType(), True),
    StructField("price_eur_mwh", DoubleType(), True),
    StructField("demand_forecast_mw", DoubleType(), True),
    StructField("renewable_share_pct", DoubleType(), True),
    StructField("trend", StringType(), True)
])

# 1.6 user-feedback
FEEDBACK_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("feedback_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("category", StringType(), True),
    StructField("text", StringType(), True),
    StructField("resolved", BooleanType(), True)
])