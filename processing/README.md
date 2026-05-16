# P2 — Spark Structured Streaming

Spark Structured Streaming job that consumes raw Kafka topics, aggregates them
per zone in 15-minute windows, runs NLP on incident reports + user feedback,
correlates textual anomalies with physical voltage drops, and writes results to
MongoDB.

## Purpose in the pipeline

```
Kafka (P1) ──► Spark Streaming (P2 — this folder) ──► MongoDB (P3)
                            │
                            ▼
                  Data Quality metrics
```

## Inputs (Kafka topics — all 6 now produced by P1)

| Topic | Partitions | Schema |
|---|---|---|
| `smart-meters` | 3 | [smart_meter.schema.json](../ingestion/schemas/smart_meter.schema.json) |
| `weather` | 1 | [weather.schema.json](../ingestion/schemas/weather.schema.json) |
| `incident-reports` | 1 | [incident_report.schema.json](../ingestion/schemas/incident_report.schema.json) |
| `rss-feeds` | 1 | [rss_feed.schema.json](../ingestion/schemas/rss_feed.schema.json) |
| `market-prices` | 1 | [market_price.schema.json](../ingestion/schemas/market_price.schema.json) |
| `user-feedback` | 3 | [user_feedback.schema.json](../ingestion/schemas/user_feedback.schema.json) |

Bootstrap servers:
- **Inside Docker:** `kafka:29092` (local/pseudo) or `kafka1:29092,kafka2:29092,kafka3:29092` (distributed)
- **From host:** `localhost:9092`

## ⚠️ Spark connector packaging (read FIRST)

The Spark images in `docker-compose.*.yml` are the bare `apache/spark:3.5.0`
distribution. They do NOT ship with the Kafka or MongoDB connector JARs.
Without these, `spark.readStream.format("kafka")` and
`writeStream.format("mongodb")` will throw `ClassNotFoundException` at runtime.

You have two options. Pick one and stick to it.

### Option A — Pull connectors at submit time via `--packages` (simpler)

Run every `spark-submit` with the `--packages` flag:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  processing/spark_streaming.py
```

Or set it once in `SparkSession.builder.config("spark.jars.packages", ...)` (see
starter snippet below). On the very first job submission Spark downloads the
JARs from Maven Central into `~/.ivy2/` (cached for subsequent runs).
Pro: zero image changes. Con: needs internet on first run.

### Option B — Bake connectors into a custom Spark image (production-grade)

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

Then in each `docker-compose.*.yml`, replace `image: apache/spark:3.5.0` with
`build: { context: .., dockerfile: docker/spark/Dockerfile }` for both
`spark-master` and `spark-worker*` services. Pro: offline-capable, predictable.
Con: image ~600 MB, slower CI builds.

**Recommended:** Option A for the demo, Option B if you have time.

## Outputs (MongoDB collections — coordinate with P3)

| Collection | Content | Used by |
|---|---|---|
| `meters_aggregated_15min` | Per-zone per-window: avg `consumption_kwh`, anomaly count, voltage min/max, frequency stddev | P4 dashboard, P5 ML |
| `incidents_enriched` | Incidents with NLP-extracted keywords + correlated voltage drops | P4 word cloud |
| `feedback_nlp` | User feedback texts after tokenize + sentiment | P4 |
| `data_quality_metrics` | Completeness/noise/bias metrics per window | P5 ML, defense report |

## Required env vars

- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_METERS`, `KAFKA_TOPIC_WEATHER`, `KAFKA_TOPIC_INCIDENTS`
- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`
- `SPARK_MASTER` (e.g. `local[*]` or `spark://spark-master:7077`)

## What to build

1. **`spark_streaming.py`** — main job: read 3 topics, parse JSON, 15-min tumbling windows with watermark, aggregate by `zone`, broadcast-join with latest `weather` row, write to MongoDB
2. **`nlp_processing.py`** — tokenize `incidents.description` AND simulated user feedback; extract keywords (`nltk`); compute term frequency per zone
3. **`anomaly_correlation.py`** — cross-reference `incidents.description` text (mentions of "voltage drop", "surge") with concurrent `is_anomaly=true` voltage events in `meters_raw` for the same zone+time window
4. **`data_quality.py`** — see Data Quality section below
5. **`schemas.py`** — Python module that loads JSON schemas into Spark `StructType`

## Starter snippet

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, when, max, min, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

spark = (SparkSession.builder
    .appName("energy-streaming")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .getOrCreate())

meters = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "smart-meters")
    .option("startingOffsets", "latest")
    .load())

parsed = (meters
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), METER_SCHEMA).alias("d"))
    .select("d.*"))

windowed = (parsed
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "15 minutes"), col("zone"))
    .agg(avg("consumption_kwh").alias("avg_consumption"),
         count(when(col("is_anomaly"), 1)).alias("anomaly_count"),
         min("voltage_v").alias("voltage_min"),
         max("voltage_v").alias("voltage_max"),
         stddev("frequency_hz").alias("frequency_stddev")))

query = (windowed.writeStream
    .format("mongodb")
    .option("uri", f"mongodb://{user}:{pw}@mongodb:27017/energy_db.meters_aggregated_15min")
    .option("checkpointLocation", "/tmp/spark-checkpoints")
    .outputMode("update")
    .start())
```

## Data Quality Framework (professor's Section E — REQUIRED)

P2 owns computing data quality metrics per 15-min window. Output to `data_quality_metrics`:

| Metric | Formula | Threshold (alert if) |
|---|---|---|
| Completeness | `(non-null fields) / (expected fields)` per topic | < 99% |
| Noise rate | `(schema validation failures + out-of-range values) / total` | > 1% |
| Anomaly class balance | `count(is_anomaly=true) / total` | < 3% or > 15% (drift from ~7%) |
| Zone distribution balance | min/max meter count per zone | ratio < 0.5 |
| Temporal coverage | `windows_with_data / expected_windows` over last hour | < 95% |

These feed both the dashboard alerts and the defense Rapport.

## Required justifications for the defense (Section D and grading)

The professor explicitly grades on tool justification. P2 must be able to answer:

1. **Why Spark Structured Streaming and not Flink, Storm, or Kafka Streams?**
   → Suggested: tight Python integration (PySpark) matches team's skill set, mature MongoDB connector, micro-batch model is sufficient for 15-min windows (no need for true ms-latency stream processing), unified batch+stream API
2. **Why 15-minute windows?** → Matches professor's spec Section B and energy grid 15-min settlement intervals (industry standard)
3. **Why tumbling vs sliding windows?** → Tumbling = non-overlapping aggregates; sliding would double-count and confuse hourly KPIs
4. **What is your watermark policy?** → 2-min watermark allows for ingestion lag while bounding state size
5. **What exactly-once guarantee?** → MongoDB sink supports idempotent upserts on `(zone, window_start)` composite key

## Dependencies (already in `requirements.txt`)

- `pyspark==3.5.0`, `pymongo==4.7.3`, `nltk==3.8.1`

## Run

```bash
.venv/bin/python -m processing.spark_streaming
```

## Verification

- Spark UI at http://localhost:8080 shows the streaming query
- `db.meters_aggregated_15min.count()` grows by 4 documents (one per zone) every 15 min
- `db.data_quality_metrics.find().sort({timestamp: -1}).limit(1)` shows latest quality scores
- Cross-check against `verify_topics.py` raw message counts — aggregated counts must match within ±2% (allowing for watermark drop)
