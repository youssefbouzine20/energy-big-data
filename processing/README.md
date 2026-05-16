# P2 — Spark Structured Streaming  (FULL WORKFLOW + WHAT TO BUILD)

> Owner: P2 teammate.
> Reads from: Kafka (6 topics produced by P1).
> Writes to:  MongoDB (collections defined by P3).
> Used by:    P4 dashboard (reads the aggregated collections), P5 ML (reads `meters_raw`).

---

## 0. The pipeline in one picture

```
  P1 producers            ┌─── KAFKA (6 topics) ─────────┐                P3 MongoDB
  ─────────────           │                              │              ──────────────
  smart_meter ─┐  json    │ smart-meters     (3 part.)   │  parsed +    meters_raw
  weather    ──┼──────►   │ weather          (1 part.)   │  validated   weather
  incident   ──┤          │ incident-reports (1 part.)   │              incidents
  rss        ──┤          │ rss-feeds        (1 part.)   │              rss_feeds
  market     ──┤          │ market-prices    (1 part.)   │              market_prices
  feedback   ──┘          │ user-feedback    (3 part.)   │              feedback_raw
                          └──────────┬───────────────────┘                       │
                                     │ readStream                                │
                                     ▼                                           │
                          ┌──────────────────────────────┐                       │
                          │   YOU ARE HERE   (Spark)     │                       │
                          │                              │                       │
                          │  1. parse JSON → typed cols  │                       │
                          │  2. apply schema (StructType)│                       │
                          │  3. 15-min tumbling windows  │                       │
                          │     groupBy(window, zone)    │                       │
                          │  4. broadcast-join weather   │                       │
                          │  5. lookup-join incidents    │                       │
                          │  6. NLP on incident.descrip. │                       │
                          │     + user_feedback.text     │                       │
                          │  7. compute data-quality     │                       │
                          │     metrics per window       │                       │
                          └──────────┬───────────────────┘                       │
                                     │ writeStream (mongo-spark-connector)       │
                                     ▼                                           ▼
                                                                         meters_aggregated_15min
                                                                         incidents_enriched
                                                                         feedback_nlp
                                                                         data_quality_metrics
                                     │
                                     └──── P4 dashboard reads aggregates
                                           P5 ML reads meters_raw + features
```

**Your job in one sentence:** read 6 raw streams from Kafka, transform them
into ready-to-query MongoDB documents (aggregated, joined, NLP-processed,
quality-checked), keep the streams running 24/7 with exactly-once guarantees.

---

## 1. The 6 input streams — exact schemas

Every Kafka message is a JSON object. P1 validates each message against a
JSON Schema before producing, so you can trust the structure 100%. Schemas
are in `../ingestion/schemas/`. Here's a copy-pasted summary:

### 1.1 `smart-meters` — 3 partitions, ~1 msg/s with 20 meters

```json
{
  "schema_version": "1.0",
  "meter_id":       "SM-007",                      // SM-NNN, your join key
  "timestamp":      "2026-05-16T18:00:30Z",        // UTC ISO 8601 with Z
  "zone":           "B",                           // A | B | C | D
  "consumption_kwh": 0.0234,                       // [0.001, 0.12] — REGRESSION TARGET
  "voltage_v":      230.5,                         // [210, 250]
  "frequency_hz":   50.01,                         // [48, 52]
  "power_factor":   0.95,                          // [0, 1]
  "is_anomaly":     false,                         // CLASSIFICATION TARGET
  "anomaly_reason": null,                          // null | VOLTAGE_LOW | VOLTAGE_HIGH | OVERCONSUMPTION | FREQUENCY_DEVIATION
  "hour_of_day":    18,                            // 0..23 UTC
  "day_of_week":    5,                             // 0=Mon, 6=Sun
  "is_peak_hour":   true,                          // 7-9 or 17-21 UTC
  "zone_lat":       35.575,                        // for dashboard heatmap
  "zone_lon":       -5.370
}
```

### 1.2 `weather` — 1 partition, 1 msg / 30 s

```json
{
  "schema_version": "1.0",
  "station_id":     "WS-MAIN",                     // pattern WS-[A-Z]+
  "timestamp":      "2026-05-16T18:00:30Z",
  "temperature_c":      27.5,
  "feels_like_c":       29.0,
  "humidity_pct":       65.0,
  "solar_irradiance_wm2": 520.5,
  "wind_speed_ms":      5.2,
  "weather_severity":   "NORMAL"                   // NORMAL | HOT | COLD | STORM | EXTREME
}
```

### 1.3 `incident-reports` — 1 partition, 1 msg / 60 s

```json
{
  "schema_version": "1.0",
  "incident_id":    "INC-20260516-001",            // INC-YYYYMMDD-NNN
  "timestamp":      "2026-05-16T18:00:30Z",
  "zone":           "B",
  "severity":       "HIGH",                        // LOW | MEDIUM | HIGH | CRITICAL
  "type":           "POWER_OUTAGE",                // 6 enum values
  "description":    "Critical power outage in zone B. 12 meters offline. Grid fault traced to transformer T-B2. Crew dispatched.",
  "affected_meters": ["SM-001", "SM-002", "..."],  // 1..20 meters
  "estimated_duration_min": 180,                   // 1..480
  "resolved":       false
}
```

### 1.4 `rss-feeds` — 1 partition, 1 msg / 120 s

```json
{
  "schema_version": "1.0",
  "feed_id":        "RSS-20260516-001",
  "timestamp":      "2026-05-16T18:00:30Z",
  "source":         "masen.ma",
  "category":       "RENEWABLE",                   // 5 enum values
  "title":          "Solar farm of 200MW connects to grid in Tetouan",
  "summary":        "A new 200MW solar photovoltaic farm has been ...",
  "impact_score":   0.65,                          // [0, 1] ML feature weight
  "keywords":       ["solar", "renewable", "photovoltaic", "grid"]
}
```

### 1.5 `market-prices` — 1 partition, 1 msg / hour

```json
{
  "schema_version": "1.0",
  "timestamp":      "2026-05-16T18:00:30Z",
  "market":         "MASEN",                       // MASEN | EU_SPOT | DAY_AHEAD
  "price_mad_mwh":  857.5,
  "price_eur_mwh":  80.14,                         // ML feature
  "demand_forecast_mw":  4500.0,
  "renewable_share_pct": 28.5,
  "trend":          "RISING"                       // RISING | FALLING | STABLE
}
```

### 1.6 `user-feedback` — 3 partitions, 1 msg / 45 s

```json
{
  "schema_version": "1.0",
  "feedback_id":    "FB-20260516-001",
  "timestamp":      "2026-05-16T18:00:30Z",
  "user_id":        "U-12345",                     // pseudonymous (GDPR)
  "zone":           "A",                           // A | B | C | D
  "channel":        "WEB",                         // WEB | MOBILE | CALL_CENTER | EMAIL
  "sentiment":      "NEGATIVE",                    // POSITIVE | NEGATIVE | NEUTRAL
  "category":       "OUTAGE",                      // OUTAGE | BILLING | QUALITY | GENERAL
  "text":           "Power has been out in zone A for 90 minutes now. ...",
  "resolved":       false
}
```

**Important guarantee from P1:** the `text` field always references the same
zone as the `zone` field. If you see a feedback message that violates this,
report it (regression bug).

---

## 2. The output collections — what YOU write to MongoDB

P3's `storage/init/init-mongo.js` already creates these collections, indexes,
and TTL policies. You write to them; you don't define them. Here's the
contract for what each document must look like.

### 2.1 `meters_raw`  (TTL 90 days)

Pass-through of every smart-meter message + ingestion timestamp. P5 ML reads
this for training.

```json
{
  "_id":          "<auto>",
  "meter_id":     "SM-007",
  "timestamp":    ISODate("2026-05-16T18:00:30Z"),
  "zone":         "B",
  "consumption_kwh": 0.0234, "voltage_v": 230.5, "frequency_hz": 50.01,
  "power_factor": 0.95, "is_anomaly": false, "anomaly_reason": null,
  "hour_of_day":  18, "day_of_week": 5, "is_peak_hour": true,
  "zone_lat":     35.575, "zone_lon": -5.370,
  "ingested_at":  ISODate("2026-05-16T18:00:31.123Z")
}
```

### 2.2 `meters_aggregated_15min`  (TTL 1 year)

15-minute tumbling-window aggregate per zone. P4 dashboard reads this for
heatmap + KPI cards. Composite unique key for idempotency: `(zone, window_start)`.

```json
{
  "_id":          "<auto>",
  "zone":         "B",
  "window_start": ISODate("2026-05-16T18:00:00Z"),
  "window_end":   ISODate("2026-05-16T18:15:00Z"),
  "avg_consumption":  0.0231,
  "max_consumption":  0.045,
  "min_consumption":  0.012,
  "total_consumption_kwh": 25.4,        // sum across all meters in zone
  "anomaly_count":    3,                // count of is_anomaly=true
  "anomaly_rate_pct": 7.5,              // % of readings flagged anomalous
  "voltage_min":      214.5,
  "voltage_max":      245.2,
  "voltage_avg":      230.1,
  "frequency_stddev": 0.04,
  "meter_count":      5,                // # distinct meters in window
  "weather_temperature_c": 27.5,        // joined from latest weather
  "weather_severity":      "NORMAL",
  "active_incidents":      1,           // joined from incidents (if any)
  "zone_lat":     35.575, "zone_lon": -5.370,    // for heatmap
  "computed_at":  ISODate("2026-05-16T18:15:02.456Z")
}
```

### 2.3 `weather`  (TTL 1 year)

Pass-through.

### 2.4 `incidents`  (TTL 2 years)

Pass-through.

### 2.5 `incidents_enriched`  (TTL 2 years)

Original incident + NLP-extracted keywords + correlation with concurrent
voltage anomalies in the same zone.

```json
{
  "_id":             "<auto>",
  "incident_id":     "INC-20260516-001",
  "zone":            "B", "timestamp": ISODate("..."),
  "severity":        "HIGH", "type": "POWER_OUTAGE",
  "description":     "...",
  "nlp_keywords":    ["voltage", "outage", "transformer", "crew"],   // top-K terms
  "nlp_sentiment":   "NEGATIVE",                                     // optional
  "correlated_anomalies": 12,    // # of meter readings with is_anomaly=true in same zone+window
  "correlated_voltage_drops": 8, // # of those that were VOLTAGE_LOW or VOLTAGE_HIGH
  "computed_at":     ISODate("...")
}
```

### 2.6 `feedback_nlp`  (TTL 1 year)

Original feedback + NLP token frequency + sentiment confirmation.

```json
{
  "_id":               "<auto>",
  "feedback_id":       "FB-20260516-001",
  "zone":              "A", "channel": "WEB",
  "category":          "OUTAGE",
  "sentiment_input":   "NEGATIVE",            // ground truth from producer
  "sentiment_predicted": "NEGATIVE",          // your NLP output
  "tokens":            ["power", "out", "zone", "minutes", "unacceptable"],
  "text":              "Power has been out ...",
  "computed_at":       ISODate("...")
}
```

### 2.7 `data_quality_metrics`  (TTL 1 year)

One document per topic per 15-min window. Required by Section E of the spec.

```json
{
  "_id":           "<auto>",
  "topic":         "smart-meters",
  "window_start":  ISODate("2026-05-16T18:00:00Z"),
  "completeness_pct":   99.8,    // (non-null required fields) / expected
  "noise_rate_pct":     0.3,     // (schema-invalid + out-of-range) / total
  "anomaly_rate_pct":   7.5,
  "zone_balance_ratio": 0.92,    // min/max meter count per zone
  "temporal_coverage_pct": 100,  // windows_with_data / expected
  "alert":         null          // null | "WARN" | "CRIT" with reason
}
```

---

## 3. ⚠️ Spark connector packaging — read FIRST

The `apache/spark:3.5.0` image in our `docker-compose.*.yml` ships ONLY the
Spark core. It does NOT include Kafka or MongoDB connectors. Without them,
`spark.readStream.format("kafka")` and `writeStream.format("mongodb")` throw
`ClassNotFoundException` at runtime.

### Option A — Pull connectors at submit time via `--packages` (simpler, recommended)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  processing/spark_streaming.py
```

Or programmatic (no `--packages` needed when set in code):

```python
spark = (SparkSession.builder
    .appName("energy-streaming")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .getOrCreate())
```

First job downloads the JARs from Maven Central into `~/.ivy2/`, cached
afterwards. Pro: zero image changes. Con: first run needs internet.

### Option B — Bake connectors into a custom image (production-grade)

Create `docker/spark/Dockerfile`:

```dockerfile
FROM apache/spark:3.5.0
USER root
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
 && curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
 && curl -L -o /opt/spark/jars/kafka-clients-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar \
 && curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && curl -L -o /opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar \
      https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar \
 && curl -L -o /opt/spark/jars/mongodb-driver-sync-4.11.1.jar \
      https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar
USER spark
```

In each compose file replace `image: apache/spark:3.5.0` with
`build: { context: .., dockerfile: docker/spark/Dockerfile }` for both
`spark-master` and `spark-worker*`. Pro: offline, deterministic. Con: ~600 MB image, slower first build.

**Recommended:** Option A for the demo, Option B if you have a free afternoon.

---

## 4. Connection details — exactly what URL goes where

| What you want to connect to | Inside Docker network | From the host (running locally) |
|---|---|---|
| Kafka broker  | `kafka:29092` (local/pseudo) or `kafka1:29092,kafka2:29092,kafka3:29092` (distributed) | `localhost:9092` (or `:9094,:9096` for distributed brokers 2/3) |
| MongoDB | `mongodb:27017` | `localhost:${MONGO_PORT}` (default `27017`) |
| Schema Registry (optional) | `http://schema-registry:8081` | `http://localhost:8081` |

Spark workers run **inside** the Docker network, so they see Kafka as
`kafka:29092` and Mongo as `mongodb:27017`. If you submit a Spark job from
the host with `--master local[*]` for testing, use `localhost:9092` and
`localhost:27017`.

### Required env vars (read from `.env`)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092           # inside compose; localhost:9092 from host
KAFKA_TOPIC_METERS=smart-meters
KAFKA_TOPIC_WEATHER=weather
KAFKA_TOPIC_INCIDENTS=incident-reports
KAFKA_TOPIC_RSS=rss-feeds
KAFKA_TOPIC_PRICES=market-prices
KAFKA_TOPIC_FEEDBACK=user-feedback

# Mongo (use the spark_writer service account, NOT the root admin)
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_DB_NAME=energy_db
MONGO_SPARK_USER=spark_writer
MONGO_SPARK_PASS=change-me-before-deploy       # rotate from default!

# Spark
SPARK_MASTER=spark://spark-master:7077
SPARK_CHECKPOINT_DIR=/tmp/spark-checkpoints    # bind-mount to a host path for HA
```

---

## 5. The complete workflow — step by step

For each batch (every micro-batch trigger, default ~1s):

### Step 1: Read raw bytes from Kafka

```python
raw_meters = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "smart-meters")
    .option("startingOffsets", "latest")          # 'earliest' for backfill
    .option("failOnDataLoss", "false")
    .load())
# raw_meters columns: key, value, topic, partition, offset, timestamp, timestampType
```

### Step 2: Parse JSON to typed columns using StructType

```python
from pyspark.sql.types import *
METER_SCHEMA = StructType([
    StructField("schema_version",  StringType()),
    StructField("meter_id",        StringType()),
    StructField("timestamp",       TimestampType()),     # cast from ISO string
    StructField("zone",            StringType()),
    StructField("consumption_kwh", DoubleType()),
    StructField("voltage_v",       DoubleType()),
    StructField("frequency_hz",    DoubleType()),
    StructField("power_factor",    DoubleType()),
    StructField("is_anomaly",      BooleanType()),
    StructField("anomaly_reason",  StringType()),
    StructField("hour_of_day",     IntegerType()),
    StructField("day_of_week",     IntegerType()),
    StructField("is_peak_hour",    BooleanType()),
    StructField("zone_lat",        DoubleType()),
    StructField("zone_lon",        DoubleType()),
])
parsed = (raw_meters
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), METER_SCHEMA).alias("d"))
    .select("d.*"))
```

Repeat for each topic with its own StructType. **Tip:** put all 6 schemas
in `processing/schemas.py` as one module so any imports stay tidy.

### Step 3: Write the raw stream to `meters_raw` (passthrough)

```python
(parsed.withColumn("ingested_at", current_timestamp())
    .writeStream
    .format("mongodb")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/meters_raw")
    .option("connection.uri", MONGO_URI)
    .option("database", "energy_db")
    .option("collection", "meters_raw")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start())
```

### Step 4: Window-aggregate by zone

```python
windowed = (parsed
    .withWatermark("timestamp", "2 minutes")           # tolerate 2-min lag
    .groupBy(window(col("timestamp"), "15 minutes"), col("zone"))
    .agg(
        avg("consumption_kwh").alias("avg_consumption"),
        max("consumption_kwh").alias("max_consumption"),
        min("consumption_kwh").alias("min_consumption"),
        sum("consumption_kwh").alias("total_consumption_kwh"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        count("*").alias("reading_count"),
        min("voltage_v").alias("voltage_min"),
        max("voltage_v").alias("voltage_max"),
        avg("voltage_v").alias("voltage_avg"),
        stddev("frequency_hz").alias("frequency_stddev"),
        countDistinct("meter_id").alias("meter_count"),
        first("zone_lat").alias("zone_lat"),
        first("zone_lon").alias("zone_lon"),
    )
    .withColumn("anomaly_rate_pct", col("anomaly_count") / col("reading_count") * 100)
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end",   col("window.end"))
    .drop("window"))
```

### Step 5: Broadcast-join with latest weather row

Weather is low-volume (1 msg / 30 s) so broadcast it.

```python
latest_weather = (weather_parsed
    .withWatermark("timestamp", "5 minutes")
    .groupBy(window(col("timestamp"), "5 minutes"))
    .agg(last("temperature_c").alias("weather_temperature_c"),
         last("weather_severity").alias("weather_severity"))
    .select(col("window.start").alias("w_start"), col("w_start").cast("timestamp"),
            "weather_temperature_c", "weather_severity"))

aggregated = windowed.join(broadcast(latest_weather),
    windowed.window_start == latest_weather.w_start, "left")
```

### Step 6: Lookup-join active incidents per zone+window

```python
incident_counts = (incidents_parsed
    .withWatermark("timestamp", "5 minutes")
    .groupBy(window(col("timestamp"), "15 minutes"), col("zone"))
    .agg(count("*").alias("active_incidents")))

aggregated = aggregated.join(incident_counts,
    (aggregated.window_start == incident_counts.window.start) &
    (aggregated.zone == incident_counts.zone), "left")
```

### Step 7: Write aggregates to Mongo

```python
(aggregated
    .withColumn("computed_at", current_timestamp())
    .writeStream
    .format("mongodb")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/meters_agg")
    .option("connection.uri", MONGO_URI)
    .option("database", "energy_db")
    .option("collection", "meters_aggregated_15min")
    .outputMode("update")          # idempotent upsert per (zone, window_start)
    .start())
```

### Step 8: NLP enrichment (separate stream)

```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover

tok = Tokenizer(inputCol="description", outputCol="words")
sw  = StopWordsRemover(inputCol="words", outputCol="tokens")

incidents_nlp = (incidents_parsed
    .transform(lambda df: tok.transform(df))
    .transform(lambda df: sw.transform(df))
    .withColumn("nlp_keywords", expr("slice(tokens, 1, 8)")))   # top-8 terms

(incidents_nlp.writeStream
    .format("mongodb")
    .option("collection", "incidents_enriched")
    .outputMode("append")
    .start())
```

Same approach for `user-feedback` text. For sentiment use NLTK's VADER
(`pip install nltk` already in `requirements.txt`):

```python
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()
sentiment_udf = udf(lambda t: "POSITIVE" if sia.polarity_scores(t)["compound"]>0.2
                              else "NEGATIVE" if sia.polarity_scores(t)["compound"]<-0.2
                              else "NEUTRAL", StringType())
feedback_with_sent = feedback_parsed.withColumn("sentiment_predicted", sentiment_udf("text"))
```

### Step 9: Data quality metrics (REQUIRED — Section E)

For every 15-minute window, compute one document per topic:

```python
def quality_metrics(df, topic_name):
    return (df.groupBy(window(col("timestamp"), "15 minutes"))
        .agg(
            (count("*") / lit(EXPECTED_PER_15MIN[topic_name]) * 100).alias("temporal_coverage_pct"),
            (count(when(col("is_anomaly"), 1)) / count("*") * 100).alias("anomaly_rate_pct"),
            # ... etc
        )
        .withColumn("topic", lit(topic_name)))
```

Write to `data_quality_metrics`. The dashboard reads this for the
"Data Quality: 98%" badge.

---

## 6. What you must build — explicit task list

| # | File | Purpose | Done when |
|---|---|---|---|
| 1 | `processing/__init__.py` | Empty marker | File exists |
| 2 | `processing/schemas.py` | All 6 StructType definitions | Each schema parses one Kafka topic without nulls |
| 3 | `processing/spark_session.py` | Builder with packages, configs, MongoDB URI helper | `spark = build_session()` returns a working SparkSession |
| 4 | `processing/streams_meters.py` | Read smart-meters → write `meters_raw` + `meters_aggregated_15min` | Documents appear in Mongo within 15s of producing |
| 5 | `processing/streams_weather.py` | Read weather → write `weather` collection | Pass-through works |
| 6 | `processing/streams_incidents.py` | Read incidents → NLP → write `incidents` + `incidents_enriched` | NLP keywords are non-empty |
| 7 | `processing/streams_feedback.py` | Read user-feedback → sentiment → `feedback_nlp` | sentiment_predicted matches sentiment_input ≥ 70% of the time |
| 8 | `processing/streams_external.py` | Read rss + market-prices → pass-through | Documents land in Mongo |
| 9 | `processing/data_quality.py` | Per-topic per-window quality metrics | One doc per topic per 15-min window in `data_quality_metrics` |
| 10 | `processing/main.py` | Orchestrate all 6 streams via `spark.streams.awaitAnyTermination()` | `python -m processing.main` runs them all together |
| 11 | `processing/README.md` (this file) | Already written | — |

---

## 7. Required justifications for the defense (Section D + grading)

The professor explicitly grades on **tool justification**. Be ready to answer:

1. **Why Spark Structured Streaming and not Flink, Storm, or Kafka Streams?**
   - Tight Python integration (PySpark) matches the team's skill set
   - Mature MongoDB connector (mongo-spark-connector_2.12)
   - Micro-batch model is sufficient for 15-min windows (no need for true ms-latency)
   - Unified batch+stream API — same code can re-process historical data
2. **Why 15-minute tumbling windows?**
   - Matches the European energy market settlement interval (industry standard)
   - Aligns with professor's explicit Section B requirement
3. **Why tumbling, not sliding windows?**
   - Tumbling = non-overlapping aggregates; sliding would double-count and confuse hourly KPIs
4. **What is your watermark policy?**
   - 2-minute watermark: tolerates ingestion lag while bounding state size
5. **What exactly-once guarantee do you offer?**
   - MongoDB sink with idempotent upsert on `(zone, window_start)` composite key
   - Spark checkpointing + Kafka offset commits ensure no double-processing on restart
6. **How do you handle late data?**
   - Watermark drops events older than 2 min → counted in `data_quality_metrics.late_event_count`

---

## 8. Run + verify

### Run inside Docker (production-like)

```bash
docker exec -it spark-master-pseudo /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /workspace/processing/main.py
```

(Mount the project into the container in compose: `volumes: ../:/workspace`.)

### Run from host (fast dev iteration)

```bash
.venv/bin/python -m processing.main
# Set KAFKA_BOOTSTRAP_SERVERS=localhost:9092 and MONGO_HOST=localhost
```

### Verify

- Spark UI at http://localhost:8080 shows running streaming queries
- `db.meters_aggregated_15min.countDocuments({})` grows by 4 per 15 min (one per zone)
- `db.data_quality_metrics.find().sort({window_start:-1}).limit(1)` shows recent window
- `db.incidents_enriched.findOne()` has non-empty `nlp_keywords`
- Cross-check: aggregated `reading_count` ≈ raw `meters_raw.countDocuments({zone:'B'})`
  in the same 15-min window (within ±2% for watermark drops)

---

## 9. Common pitfalls

1. **`ClassNotFoundException: KafkaSourceProvider`** → you forgot `--packages` (see §3)
2. **Aggregates double-counted** → using sliding windows; use tumbling
3. **Mongo writes are slow** → set `spark.mongodb.write.batchSize=1000`
4. **Stream won't start** → check `KAFKA_BOOTSTRAP_SERVERS` resolves from the Spark container (`kafka:29092`, not `localhost`)
5. **Checkpoint corruption after failed restart** → `rm -rf $SPARK_CHECKPOINT_DIR/*`, accept lose-pending-batch
6. **Watermark too aggressive** → late events dropped silently → check `data_quality_metrics.late_event_count`
7. **Authentication failure on Mongo** → use `spark_writer` user, not the root admin (root is for ops only)

---

## 10. Dependencies (already in root `requirements.txt`)

`pyspark==3.5.0`, `pymongo==4.7.3`, `nltk==3.8.1`. Run once: `python -m nltk.downloader vader_lexicon stopwords punkt` to download NLTK data.
