import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

# ── Helpers ───────────────────────────────────────────────────────────────────
def _get_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except ValueError:
        raise ValueError(f"[kafka_config] {key} must be an integer, got: {os.getenv(key)!r}")

# ── Broker ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '9092')}"
)

# ── Topics ────────────────────────────────────────────────────────────────────
TOPIC_METERS    = os.getenv("KAFKA_TOPIC_METERS",    "smart-meters")
TOPIC_WEATHER   = os.getenv("KAFKA_TOPIC_WEATHER",   "weather")
TOPIC_INCIDENTS = os.getenv("KAFKA_TOPIC_INCIDENTS", "incident-reports")
TOPIC_RSS       = os.getenv("KAFKA_TOPIC_RSS",       "rss-feeds")
TOPIC_PRICES    = os.getenv("KAFKA_TOPIC_PRICES",    "market-prices")
TOPIC_FEEDBACK  = os.getenv("KAFKA_TOPIC_FEEDBACK",  "user-feedback")

# ── Topic config ──────────────────────────────────────────────────────────────
REPLICATION_FACTOR = _get_int("KAFKA_REPLICATION_FACTOR", 1)

TOPIC_CONFIG = {
    TOPIC_METERS:    {"num_partitions": 3, "replication_factor": REPLICATION_FACTOR},
    TOPIC_WEATHER:   {"num_partitions": 1, "replication_factor": REPLICATION_FACTOR},
    TOPIC_INCIDENTS: {"num_partitions": 1, "replication_factor": REPLICATION_FACTOR},
    TOPIC_RSS:       {"num_partitions": 1, "replication_factor": REPLICATION_FACTOR},
    TOPIC_PRICES:    {"num_partitions": 1, "replication_factor": REPLICATION_FACTOR},
    TOPIC_FEEDBACK:  {"num_partitions": 3, "replication_factor": REPLICATION_FACTOR},
}

# ── Producer config ───────────────────────────────────────────────────────────
PRODUCER_CONFIG = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "client.id":          os.getenv("HOSTNAME", "energy-producer"),
    "acks":               "all",
    "retries":            10,
    "retry.backoff.ms":   500,
    "linger.ms":          10,
    # Disabled: idempotent producers + retries=3 caused _OUT_OF_ORDER_SEQUENCE
    # fatals on low-volume topics (incidents/rss/market/feedback). At-least-once
    # is sufficient — Spark window aggregation absorbs duplicates.
    "enable.idempotence": "false",
}

# ── Consumer config ───────────────────────────────────────────────────────────
CONSUMER_CONFIG = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "group.id":           os.getenv("KAFKA_CONSUMER_GROUP", "energy-spark-consumer"),
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": "false",
}

# ── Intervals (seconds) ───────────────────────────────────────────────────────
PRODUCER_INTERVAL = _get_int("PRODUCER_INTERVAL_SECONDS", 30)    # smart meters + weather
INCIDENT_INTERVAL = _get_int("INCIDENT_INTERVAL_SECONDS", 60)    # incident reports
RSS_INTERVAL      = _get_int("RSS_INTERVAL_SECONDS",      120)   # RSS feed items
PRICE_INTERVAL    = _get_int("PRICE_INTERVAL_SECONDS",    3600)  # hourly market prices
FEEDBACK_INTERVAL = _get_int("FEEDBACK_INTERVAL_SECONDS", 45)    # user feedback
NUM_METERS        = _get_int("NUM_METERS", 10)