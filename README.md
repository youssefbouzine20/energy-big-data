# energy-big-data — Real-Time Predictive Energy Load Analysis

> University project — M126 Big Data, ENSA Tétouan, Pr. Imad Sassi

## Architecture

```
Python Producers ──► Kafka (KRaft, 3 topics)
                          │
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
| Docker | Desktop | All services containerized |

## Prerequisites

- Docker Desktop running
- Python 3.10.11 installed
- Virtual environment created: `python -m venv .venv`
- Dependencies installed: `.venv\Scripts\pip install -r requirements.txt`

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

# Start producers (3 separate terminals, from project root)
.venv\Scripts\python -m ingestion.producers.weather_producer      # terminal 1
.venv\Scripts\python -m ingestion.producers.smart_meter_producer  # terminal 2
.venv\Scripts\python -m ingestion.producers.incident_producer     # terminal 3
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

## Kafka Topics

| Topic | Partitions | Interval | Description |
|---|---|---|---|
| `smart-meters` | 3 | 30s | 20 virtual meters, 4 zones (A/B/C/D) |
| `weather` | 1 | 30s | WS-MAIN weather station (Tétouan climate) |
| `incident-reports` | 1 | 60s | Grid incidents weighted by weather severity |

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

Subscribe to all 3 topics and watch messages flow in real time:

```powershell
.venv\Scripts\python -m ingestion.consumers.verify_topics
```

Press Ctrl-C to stop. A per-topic message count summary is printed on shutdown.

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