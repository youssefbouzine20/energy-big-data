# energy-big-data — Real-Time Predictive Energy Load Analysis

> University project — M126 Big Data, ENSA Tétouan, Pr. Imad Sassi

## Architecture

```
Python Producers ──► Kafka (KRaft, 3 topics) ◄──► Schema Registry
                          │                       (Confluent)
                          │
                          │              ◄── Kafka UI (Provectus)
                          ▼
               Spark Structured Streaming
               (15-min windows, NLP)
                          │
                          ▼
                      MongoDB 7.0
                          │
                    ┌─────┴─────┐
                    ▼           ▼
               Streamlit    ML Models
              Dashboard   (3 compared)
```

## Stack

| Component | Version | Notes |
|---|---|---|
| Python | 3.10.11 | |
| Confluent Kafka | 7.5.0 | KRaft — no ZooKeeper |
| Apache Spark | 3.5.0 | Structured Streaming |
| MongoDB | 7.0 | NoSQL sink |
| Streamlit | 1.35.0 | Real-time dashboard |
| Confluent Schema Registry | 7.5.0 | Central JSON schema catalog |
| Kafka UI (Provectus) | latest | Web UI for topics + messages + schemas |
| Docker | Desktop | All services containerized |

## Prerequisites

- Docker Desktop running
- Python 3.10.11 installed
- Virtual environment created: `python -m venv .venv`
- Dependencies installed: `.venv/bin/pip install --default-timeout=100 -r requirements.txt`

## Quick Start

### 1. Environment Setup

```powershell
# Copy example config
cp .env.example .env

# Generate a Kafka Cluster ID and paste into .env → KAFKA_CLUSTER_ID
docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid
```

### 2. Local Mode (development, no data persistence)

```powershell
# Start infrastructure
docker compose -f docker/docker-compose.local.yml --env-file .env up -d

# Start all 6 producers (6 separate terminals, from project root)
.venv\Scripts\python -m ingestion.producers.weather_producer          # terminal 1
.venv\Scripts\python -m ingestion.producers.smart_meter_producer      # terminal 2
.venv\Scripts\python -m ingestion.producers.incident_producer         # terminal 3
.venv\Scripts\python -m ingestion.producers.rss_producer              # terminal 4
.venv\Scripts\python -m ingestion.producers.market_price_producer     # terminal 5
.venv\Scripts\python -m ingestion.producers.user_feedback_producer    # terminal 6
```

**WSL / Linux:**

```bash
docker compose -f docker/docker-compose.local.yml --env-file .env up -d

source .venv/bin/activate
python -m ingestion.producers.weather_producer          # terminal 1
python -m ingestion.producers.smart_meter_producer      # terminal 2
python -m ingestion.producers.incident_producer         # terminal 3
python -m ingestion.producers.rss_producer              # terminal 4
python -m ingestion.producers.market_price_producer     # terminal 5
python -m ingestion.producers.user_feedback_producer    # terminal 6
```

### 3. Pseudo-Distributed Mode (data persists across restarts)

```powershell
docker compose -f docker/docker-compose.pseudo.yml --env-file .env up -d
```

> ⚠️ Use `docker compose down` to stop (preserves data).
> `docker compose down -v` **destroys all data**.

### 4. Fully Distributed Mode (3 Kafka brokers, RF=3)

```powershell
docker compose -f docker/docker-compose.distributed.yml --env-file .env up -d
```

Update `.env`:
```
KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29092,kafka3:29092
KAFKA_REPLICATION_FACTOR=3
```

> **Note on distributed mode:** This compose file runs 3 Kafka brokers as
> separate Docker containers on a single host, demonstrating the RF=3 /
> MIN_ISR=2 configuration and broker quorum behavior. For a true multi-node
> deployment, each broker would run on a separate physical machine with the
> `KAFKA_ADVERTISED_LISTENERS` updated to the host's reachable IP.

## Kafka Topics (6 total — covers all professor's Section A requirements)

| Topic | Partitions | Interval | Description | Spec section |
|---|---|---|---|---|
| `smart-meters` | 3 | 30s | Virtual smart meters (configurable `NUM_METERS`), 4 zones (A/B/C/D) | A.1 |
| `weather` | 1 | 30s | WS-MAIN weather station (Tétouan climate: temp, humidity, irradiance, wind) | A.2 |
| `incident-reports` | 1 | 60s | Grid incidents weighted by weather severity | feeds B.3 NLP |
| `rss-feeds` | 1 | 120s | Industrial news RSS items (5 categories: news/regulation/market/renewable/infra) | A.3 |
| `market-prices` | 1 | 3600s | Hourly energy market prices (MASEN/EU_SPOT/DAY_AHEAD), MAD + EUR/MWh | "facteurs externes (prix du marché)" |
| `user-feedback` | 3 | 45s | User support feedback (4 channels, 4 categories, 3 sentiments) — biased by weather | feeds B.4 NLP |

## Smart Meter Features (per message)

| Field | Type | Purpose |
|---|---|---|
| `consumption_kwh` | float | ML regression target |
| `voltage_v` | float | Anomaly detection feature |
| `frequency_hz` | float | Grid stability feature |
| `power_factor` | float | Efficiency feature |
| `is_anomaly` | bool | ML classification label |
| `anomaly_reason` | enum/null | 4-class anomaly type |
| `hour_of_day` | int (UTC) | Temporal ML feature |
| `day_of_week` | int | Temporal ML feature |
| `is_peak_hour` | bool | Temporal ML feature |
| `zone_lat` / `zone_lon` | float | Geographic heatmap |

## Verify Topics Are Running

```powershell
# Check message counts
docker exec kafka-local kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic smart-meters

# Consume live messages
docker exec kafka-local kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic smart-meters --max-messages 5
```

### Live Consumer Verification

Subscribe to all 6 topics, validate every message against its JSON schema, run
sanity checks, and exit with a non-zero code on any failure (suitable for CI).

**Windows:**
```powershell
.venv\Scripts\python -m ingestion.consumers.verify_topics
```

**WSL / Linux:**
```bash
source .venv/bin/activate && python -m ingestion.consumers.verify_topics
```

Useful flags:

| Flag | Purpose |
|---|---|
| `--max-seconds 60` | Stop after 60 seconds (good for CI / quick demo) |
| `--from-beginning` | Replay from earliest offset (default: only new messages) |
| `--no-require-each` | Don't fail when a slow topic (e.g. `market-prices`, hourly) hasn't ticked yet |

Exit codes: `0` = all topics delivered schema-valid messages; `1` = at least
one schema or sanity-check failure; `2` = at least one topic produced zero
messages during the run.

Press Ctrl-C to stop. A per-topic summary (valid / schema_invalid / sanity_failed)
is printed on shutdown.

### Kafka UI & Schema Registry

After `docker compose up`, these web UIs are available on the host:

| Service | URL | Purpose |
|---|---|---|
| **Kafka UI** | http://localhost:8090 | Browse topics, messages, consumer groups, partitions, schemas |
| **Schema Registry** | http://localhost:8081 | REST API for schemas (`/subjects` lists registered schemas) |
| Spark Master UI | http://localhost:8080 | Spark cluster status (when Spark jobs run) |
| Streamlit | http://localhost:8501 | P4 dashboard (when running) |

### Register schemas with Schema Registry (one-time after startup)

```powershell
# Windows
.venv\Scripts\python -m ingestion.schemas.register_schemas
```
```bash
# WSL / Linux
source .venv/bin/activate && python -m ingestion.schemas.register_schemas
```

Expected output:
```
[OK]    smart-meters-value             id=1
[OK]    weather-value                  id=2
[OK]    incident-reports-value         id=3
[OK]    rss-feeds-value                id=4
[OK]    market-prices-value            id=5
[OK]    user-feedback-value            id=6
```

Re-running is safe — it only registers a new version if the schema changed.

### MongoDB initialization

`storage/init/init-mongo.js` runs **once** on first MongoDB startup
(`mongo:7.0` executes every `*.js` in `/docker-entrypoint-initdb.d/` against
an empty data dir). It creates 5 collections (`meters_raw`,
`meters_aggregated_15min`, `weather`, `incidents`, `ml_predictions`), the
`spark_writer` service account, query indexes, and TTL indexes that enforce
GDPR retention automatically (90 d raw / 1 y aggregated / 2 y incidents).

To re-run (for example after editing the script): `docker compose -f docker/docker-compose.<mode>.yml down -v` (this **destroys** the volume), then `up -d`.

## Team

| Role | Responsibility |
|---|---|
| P1 | Infrastructure & Ingestion (Kafka, Docker, Producers, Schemas) |
| P2 | Spark Structured Streaming (15-min windows, zone aggregation) |
| P3 | MongoDB Storage & Indexing |
| P4 | Streamlit Dashboard (heatmap, word cloud, alerts) |
| P5 | ML Models (3 algorithms compared by Precision/Recall/F1) |

## Project Structure

```
energy-big-data/
├── docker/
│   ├── docker-compose.local.yml        # Dev mode, no persistence
│   ├── docker-compose.pseudo.yml       # Single broker + volumes
│   └── docker-compose.distributed.yml # 3 brokers, RF=3
├── ingestion/
│   ├── config/kafka_config.py          # Shared Kafka configuration
│   ├── schemas/                        # JSON Schema validation
│   │   ├── smart_meter.schema.json
│   │   ├── weather.schema.json
│   │   └── incident_report.schema.json
│   └── producers/
│       ├── smart_meter_producer.py     # 20 meters, 4 zones, anomalies
│       ├── weather_producer.py         # Tétouan climate simulation
│       ├── incident_producer.py        # Weather-weighted incidents
│       └── shared_state.py            # Weather → consumption coupling
├── processing/                         # P2: Spark jobs (TODO)
├── storage/                            # P3: MongoDB indexes (TODO)
├── ml/                                 # P5: ML models (TODO)
├── dashboard/                          # P4: Streamlit app (TODO)
├── ethics/GDPR.md                      # GDPR compliance documentation
├── .env.example                        # Environment template
└── requirements.txt                    # Pinned Python dependencies
```