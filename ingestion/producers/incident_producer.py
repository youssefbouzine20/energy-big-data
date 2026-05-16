import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
from confluent_kafka import KafkaException, Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import (
    PRODUCER_CONFIG, TOPIC_INCIDENTS, INCIDENT_INTERVAL, NUM_METERS
)
from producers.shared_state import get_weather

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "incident_report.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Templates ─────────────────────────────────────────────────────────────────
TEMPLATES = [
    "Voltage drop detected in zone {zone} substation 3. Meters {meters} reporting undervoltage. Estimated cause: overload on feeder line.",
    "Critical power outage in zone {zone}. {count} meters offline. Grid fault traced to transformer T-{zone}2. Crew dispatched.",
    "Voltage surge in zone {zone} following switching operation. Meters {meters} exceed threshold. Duration: {duration} minutes.",
    "Equipment failure at zone {zone} distribution panel. Breaker CB-{zone}4 tripped. Affected meters: {meters}.",
    "Overload condition detected in zone {zone}. Peak consumption exceeded capacity by 15%. Meters {meters} flagged.",
    "Grid fault in zone {zone} caused by line contact. Isolation in progress. Meters {meters} affected. Severity: {severity}.",
    "Scheduled maintenance overrun in zone {zone}. Restoration delayed by {duration} minutes. Meters {meters} still offline.",
    "Unexpected load spike in zone {zone}. Meters {meters} reporting abnormal consumption. Investigating industrial feeder.",
    "Voltage instability in zone {zone} due to reactive power imbalance. Meters {meters} showing fluctuations.",
    "Power quality event in zone {zone}. Harmonic distortion detected. Meters {meters} affected. Duration: {duration} minutes.",
    "Zone {zone} substation cooling failure. Emergency shutdown initiated. Meters {meters} on backup supply.",
    "Lightning strike near zone {zone} caused transient voltage spike. Meters {meters} triggered protection relay.",
]

DURATION_BY_SEVERITY = {
    "LOW":      (1,  30),
    "MEDIUM":   (15, 90),
    "HIGH":     (30, 240),
    "CRITICAL": (60, 480),
}

ALL_METERS = [f"SM-{i:03d}" for i in range(1, NUM_METERS + 1)]

# Severity-tiered cap on affected meters. Schema allows up to 20 meters per
# incident — a CRITICAL grid-wide outage realistically hits a whole zone, not
# 5 meters. LOW incidents stay small (single feeder).
MAX_AFFECTED_BY_SEVERITY = {
    "LOW":      min(2,  len(ALL_METERS)),
    "MEDIUM":   min(5,  len(ALL_METERS)),
    "HIGH":     min(10, len(ALL_METERS)),
    "CRITICAL": min(20, len(ALL_METERS)),
}

# ── Counter ───────────────────────────────────────────────────────────────────
# UTC dates so the ID date always matches the message timestamp date, even if
# the producer runs across local midnight in a non-UTC timezone.
_counter     = 0
_counter_day = datetime.now(timezone.utc).date()

def next_incident_id() -> str:
    global _counter, _counter_day
    today = datetime.now(timezone.utc).date()
    if today != _counter_day:
        _counter     = 0
        _counter_day = today
    _counter += 1
    return f"INC-{today.strftime('%Y%m%d')}-{_counter:03d}"

# ── Severity weighted by weather ──────────────────────────────────────────────
def get_severity() -> str:
    ws = get_weather()["weather_severity"]
    if ws in ("STORM", "EXTREME"):
        weights = ["LOW"] * 10 + ["MEDIUM"] * 25 + ["HIGH"] * 40 + ["CRITICAL"] * 25
    elif ws == "HOT":
        weights = ["LOW"] * 30 + ["MEDIUM"] * 40 + ["HIGH"] * 25 + ["CRITICAL"] * 5
    else:
        weights = ["LOW"] * 40 + ["MEDIUM"] * 35 + ["HIGH"] * 20 + ["CRITICAL"] * 5
    return random.choice(weights)

# ── Builder ───────────────────────────────────────────────────────────────────
def build_message() -> dict:
    zone     = random.choice(["A", "B", "C", "D"])
    severity = get_severity()
    inc_type = random.choice([
        "VOLTAGE_DROP", "VOLTAGE_SURGE", "POWER_OUTAGE",
        "OVERLOAD", "EQUIPMENT_FAILURE", "GRID_FAULT"
    ])
    cap         = MAX_AFFECTED_BY_SEVERITY[severity]
    affected    = random.sample(ALL_METERS, random.randint(1, cap))
    duration    = random.randint(*DURATION_BY_SEVERITY[severity])
    description = random.choice(TEMPLATES).format(
        zone     = zone,
        meters   = ", ".join(affected),
        count    = len(affected),
        severity = severity,
        duration = duration,
    )

    msg = {
        "schema_version":         "1.0",
        "incident_id":            next_incident_id(),
        "timestamp":              datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zone":                   zone,
        "severity":               severity,
        "type":                   inc_type,
        "description":            description,
        "affected_meters":        affected,
        "estimated_duration_min": duration,
        "resolved":               False,
    }
    jsonschema.validate(instance=msg, schema=SCHEMA)
    return msg

# ── Metrics ───────────────────────────────────────────────────────────────────
_sent_count  = 0
_error_count = 0
_start_time  = time.time()


# ── Delivery callback ─────────────────────────────────────────────────────────
def on_delivery(err, msg):
    global _sent_count, _error_count
    if err:
        _error_count += 1
        print(f"[ERROR] {msg.topic()} | {err}")
    else:
        _sent_count += 1


def print_metrics():
    elapsed_min = (time.time() - _start_time) / 60
    rate = _sent_count / elapsed_min if elapsed_min > 0 else 0
    print(f"[METRICS] sent={_sent_count} errors={_error_count} "
          f"rate={rate:.1f}/min uptime={elapsed_min:.1f}m")


# ── Graceful shutdown ─────────────────────────────────────────────────────────
producer = Producer(PRODUCER_CONFIG)
running  = True
_cycle   = 0

def shutdown(sig, frame):
    global running
    print("\n[INFO] Shutting down...")
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Incident producer — interval={INCIDENT_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
_backoff = 1

while running:
    _cycle += 1
    try:
        msg = build_message()
        producer.produce(
            topic    = TOPIC_INCIDENTS,
            key      = msg["zone"],
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        producer.poll(0)
        _backoff = 1
        print(f"  → {msg['incident_id']} | zone={msg['zone']} | "
              f"{msg['severity']} | {msg['type']} | "
              f"duration={msg['estimated_duration_min']}min")
        # No per-cycle flush — poll(0) services callbacks; final flush on shutdown.
    except BufferError:
        producer.poll(1)
    except KafkaException as e:
        _error_count += 1
        print(f"[KAFKA ERROR] {e} — backoff {_backoff}s")
        time.sleep(_backoff)
        _backoff = min(_backoff * 2, 30)
    except jsonschema.ValidationError as e:
        print(f"[SCHEMA ERROR] {e.message}")
    except Exception as e:
        print(f"[ERROR] {e}")

    if _cycle % 10 == 0:
        print_metrics()

    time.sleep(INCIDENT_INTERVAL)

producer.flush(timeout=5)
print_metrics()
print("[INFO] Producer stopped.")