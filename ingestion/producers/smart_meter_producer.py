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
    PRODUCER_CONFIG, TOPIC_METERS, PRODUCER_INTERVAL, NUM_METERS
)
from producers.shared_state import get_weather

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "smart_meter.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Meter setup ───────────────────────────────────────────────────────────────
METERS = [f"SM-{i:03d}" for i in range(1, NUM_METERS + 1)]


def assign_zone(idx: int, total: int) -> str:
    # Round-robin meters across 4 zones (A/B/C/D) regardless of NUM_METERS
    return ["A", "B", "C", "D"][(idx - 1) * 4 // total]


ZONES = {f"SM-{i:03d}": assign_zone(i, NUM_METERS) for i in range(1, NUM_METERS + 1)}

# ── Meter personality (fixed seed for reproducibility) ────────────────────────
random.seed(42)
METER_PROFILE = {
    f"SM-{i:03d}": round(random.uniform(0.6, 1.4), 2)
    for i in range(1, NUM_METERS + 1)
}
random.seed()

# ── Zone coordinates (Tétouan) — enables dashboard heatmap ──────────────────
ZONE_COORDS = {
    "A": (35.5785, -5.3684),
    "B": (35.5750, -5.3700),
    "C": (35.5720, -5.3650),
    "D": (35.5800, -5.3720),
}

# ── State ─────────────────────────────────────────────────────────────────────
_message_count = 0
_sent_count    = 0
_error_count   = 0
_start_time    = time.time()
previous_consumption = {
    m: round(0.015 * METER_PROFILE[m], 4) for m in METERS
}

# ── Simulation helpers ────────────────────────────────────────────────────────
def get_voltage(force_anomaly: bool = False) -> float:
    if force_anomaly:
        return round(random.choice([
            random.uniform(210.0, 214.9),
            random.uniform(245.1, 250.0),
        ]), 1)
    return round(random.uniform(218.0, 242.0), 1)


def get_base_consumption(force_anomaly: bool = False) -> float:
    if force_anomaly:
        return random.uniform(0.046, 0.120)
    # Use UTC consistently — the timestamp + hour_of_day fields stamped on the
    # message are UTC, so the simulated peak pattern must use UTC too. Mixing
    # local time here and UTC there shifted the apparent peak by 1 hour for
    # Tetouan (UTC+1) and made `is_peak_hour` mis-aligned with consumption.
    hour = datetime.now(timezone.utc).hour
    if   0  <= hour < 6:  return random.uniform(0.002, 0.008)
    elif 7  <= hour < 10: return random.uniform(0.030, 0.050)
    elif 10 <= hour < 17: return random.uniform(0.010, 0.030)
    elif 17 <= hour < 22: return random.uniform(0.030, 0.050)
    else:                 return random.uniform(0.005, 0.015)


def get_consumption(meter_id: str, previous: float, force_anomaly: bool = False) -> float:
    base     = get_base_consumption(force_anomaly)
    weather  = get_weather()
    severity = weather["weather_severity"]

    if   severity == "HOT":     base *= random.uniform(1.2, 1.5)
    elif severity == "EXTREME": base *= random.uniform(1.4, 1.8)
    elif severity == "COLD":    base *= random.uniform(1.1, 1.3)

    base        *= METER_PROFILE[meter_id]
    consumption  = 0.7 * previous + 0.3 * base
    return round(min(0.12, max(0.001, consumption)), 4)


def get_frequency() -> float:
    if random.random() < 0.05:
        return round(random.uniform(48.0, 49.0), 2)
    return round(random.uniform(49.9, 50.1), 2)


def get_anomaly_reason(voltage: float, consumption: float, frequency: float):
    if frequency < 49.0 or frequency > 51.0: return "FREQUENCY_DEVIATION"
    if voltage   < 215.0:                    return "VOLTAGE_LOW"
    if voltage   > 245.0:                    return "VOLTAGE_HIGH"
    if consumption > 0.045:                  return "OVERCONSUMPTION"
    return None


def build_message(meter_id: str, previous: float, frequency: float,
                  force_anomaly: bool = False) -> dict:
    now          = datetime.now(timezone.utc)  # single source of truth for all time fields
    voltage      = get_voltage(force_anomaly)
    consumption  = get_consumption(meter_id, previous, force_anomaly)
    power_factor = round(random.uniform(0.92, 0.99), 2)
    reason       = get_anomaly_reason(voltage, consumption, frequency)
    zone         = ZONES[meter_id]

    msg = {
        "schema_version":  "1.0",
        "meter_id":        meter_id,
        "timestamp":       now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zone":            zone,
        "consumption_kwh": consumption,
        "voltage_v":       voltage,
        "frequency_hz":    frequency,
        "power_factor":    power_factor,
        "is_anomaly":      reason is not None,
        "anomaly_reason":  reason,
        "hour_of_day":     now.hour,
        "day_of_week":     now.weekday(),
        "is_peak_hour":    now.hour in range(7, 10) or now.hour in range(17, 22),
        "zone_lat":        ZONE_COORDS[zone][0],
        "zone_lon":        ZONE_COORDS[zone][1],
    }
    jsonschema.validate(instance=msg, schema=SCHEMA)
    return msg

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

def shutdown(sig, frame):
    global running
    print("\n[INFO] Shutting down...")
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Smart meter producer — {NUM_METERS} meters, interval={PRODUCER_INTERVAL}s")
print(f"[INFO] Meter profiles: {METER_PROFILE}")

# ── Main loop ─────────────────────────────────────────────────────────────────
_backoff = 1

while running:
    _message_count   += 1
    force             = (_message_count % 15 == 0)
    current_frequency = get_frequency()

    for meter_id in METERS:
        try:
            msg = build_message(
                meter_id,
                previous_consumption[meter_id],
                current_frequency,
                force_anomaly=force,
            )
            previous_consumption[meter_id] = msg["consumption_kwh"]  # update state

            producer.produce(
                topic    = TOPIC_METERS,
                key      = meter_id,
                value    = json.dumps(msg),
                callback = on_delivery,
            )
            producer.poll(0)  # serve delivery callbacks
            _backoff = 1
            print(f"  → {meter_id} | {msg['consumption_kwh']} kWh | "
                  f"freq={current_frequency}Hz | anomaly={msg['anomaly_reason']}")
        except BufferError:
            producer.poll(1)  # local queue full — drain
        except KafkaException as e:
            _error_count += 1
            print(f"[KAFKA ERROR] {meter_id}: {e} — backoff {_backoff}s")
            time.sleep(_backoff)
            _backoff = min(_backoff * 2, 30)
        except jsonschema.ValidationError as e:
            print(f"[SCHEMA ERROR] {meter_id}: {e.message}")
        except Exception as e:
            print(f"[ERROR] {meter_id}: {e}")

    try:
        producer.flush(timeout=5)
    except KafkaException as e:
        print(f"[KAFKA ERROR] flush failed: {e}")

    if _message_count % 10 == 0:
        print_metrics()

    time.sleep(PRODUCER_INTERVAL)

producer.flush(timeout=5)
print_metrics()
print("[INFO] Producer stopped.")