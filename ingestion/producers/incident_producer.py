import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import PRODUCER_CONFIG, TOPIC_INCIDENTS, INCIDENT_INTERVAL
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

ALL_METERS = [f"SM-{i:03d}" for i in range(1, 21)]

# ── Counter ───────────────────────────────────────────────────────────────────
_counter     = 0
_counter_day = datetime.now().date()

def next_incident_id() -> str:
    global _counter, _counter_day
    today = datetime.now().date()
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
    affected    = random.sample(ALL_METERS, random.randint(1, 5))
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

# ── Delivery callback ─────────────────────────────────────────────────────────
def on_delivery(err, msg):
    if err:
        print(f"[ERROR] {msg.topic()} | {err}")
    else:
        print(f"[OK] {msg.topic()} | partition={msg.partition()}")

# ── Graceful shutdown ─────────────────────────────────────────────────────────
producer = Producer(PRODUCER_CONFIG)
running  = True

def shutdown(sig, frame):
    global running
    print("\n[INFO] Shutting down...")
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Incident producer — interval={INCIDENT_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
while running:
    try:
        msg = build_message()
        producer.produce(
            topic    = TOPIC_INCIDENTS,
            key      = msg["zone"],
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        print(f"  → {msg['incident_id']} | zone={msg['zone']} | "
              f"{msg['severity']} | {msg['type']} | "
              f"duration={msg['estimated_duration_min']}min")
        producer.flush()
    except jsonschema.ValidationError as e:
        print(f"[SCHEMA ERROR] {e.message}")
    except Exception as e:
        print(f"[ERROR] {e}")

    time.sleep(INCIDENT_INTERVAL)

producer.flush()
print("[INFO] Producer stopped.")