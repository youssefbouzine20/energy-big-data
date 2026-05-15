# P2 — Spark Structured Streaming

This folder is for the Spark Structured Streaming job that consumes raw Kafka
topics, aggregates them into 15-minute windows per zone, and writes results
to MongoDB.

## Purpose in the pipeline

```
Kafka (P1) ──► Spark Streaming (P2 — this folder) ──► MongoDB (P3)
```

## Inputs (read from)

| Kafka topic | Partitions | Schema location |
|---|---|---|
| `smart-meters` | 3 | `ingestion/schemas/smart_meter.schema.json` |
| `weather` | 1 | `ingestion/schemas/weather.schema.json` |
| `incident-reports` | 1 | `ingestion/schemas/incident_report.schema.json` |

Bootstrap servers:
- **Inside Docker network:** `kafka:29092` (or `kafka1:29092,kafka2:29092,kafka3:29092` in distributed mode)
- **From host machine:** `localhost:9092`

## Outputs (write to)

MongoDB collections (defined by P3):
- `meters_aggregated_15min` — per-zone per-window aggregates (avg consumption, anomaly count, voltage min/max, frequency stddev)
- `incidents_enriched` — incidents with NLP-extracted keywords
- (anything else P2 decides to derive)

## Required env vars

Read from project-root `.env`:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_METERS`, `KAFKA_TOPIC_WEATHER`, `KAFKA_TOPIC_INCIDENTS`
- `MONGO_HOST`, `MONGO_PORT`, `MONGO_USERNAME`, `MONGO_PASSWORD`, `MONGO_DB_NAME`
- `SPARK_MASTER` (e.g. `local[*]` for dev, `spark://spark-master:7077` for cluster)

## What to build

1. **`spark_streaming.py`** — main job: read 3 topics, parse JSON via schemas, apply 15-min tumbling windows with watermark, aggregate per `zone`, broadcast-join `weather`, write to MongoDB.
2. **`nlp_incidents.py`** — tokenize `incident_reports.description`, count term frequency, output top keywords per zone (uses `nltk` which is already in `requirements.txt`).
3. **`schemas.py`** — Python module that loads the JSON schemas and produces matching Spark `StructType` definitions.

## Starter snippet

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, when
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

# parse JSON value -> typed columns, then window aggregate, then write to mongo
```

## Dependencies

All needed packages are already in `requirements.txt`:
- `pyspark==3.5.0`
- `pymongo==4.7.3`
- `nltk==3.8.1`

The Spark-Kafka and Spark-Mongo connector JARs are downloaded automatically via `spark.jars.packages` at runtime.

## Run

```bash
# From project root
.venv/bin/python -m processing.spark_streaming
```

## Verification

- Spark UI: http://localhost:8080
- Check MongoDB collection growth: `db.meters_aggregated_15min.count()`
- Cross-check against `verify_topics.py` raw counts
