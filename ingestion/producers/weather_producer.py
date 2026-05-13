import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import PRODUCER_CONFIG, TOPIC_WEATHER, PRODUCER_INTERVAL
from producers.shared_state import set_weather

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "weather.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Simulation ────────────────────────────────────────────────────────────────
def get_temperature() -> float:
    hour             = datetime.now().hour
    base, amplitude  = 26.0, 7.0
    angle            = 2 * math.pi * (hour - 14) / 24
    temp             = base + amplitude * math.cos(angle) + random.gauss(0, 1.5)
    return round(max(-10.0, min(50.0, temp)), 1)

def get_feels_like(temp: float, humidity: float, wind: float) -> float:
    if temp >= 27:
        feels = temp + 0.33 * (humidity / 100 * 6.105) - 0.70 * wind - 4.0
    else:
        feels = temp - (wind ** 0.16) * 0.5
    return round(max(-15.0, min(55.0, feels)), 1)

def get_humidity(temp: float) -> float:
    base    = 70.0 - (temp - 10.0) * 0.8
    return round(max(0.0, min(100.0, base + random.gauss(0, 5))), 1)

def get_solar_irradiance() -> float:
    hour = datetime.now().hour
    if hour < 6 or hour >= 20:
        return 0.0
    angle      = math.pi * (hour - 6) / 14
    irradiance = 850 * math.sin(angle) + random.gauss(0, 20)
    return round(max(0.0, min(1000.0, irradiance)), 1)

def get_wind_speed(prev: float) -> float:
    hour  = datetime.now().hour
    boost = 1.3 if 12 <= hour <= 18 else 1.0
    wind  = (prev + random.gauss(0, 0.5)) * boost
    return round(max(0.5, min(50.0, wind)), 1)

def get_severity(temp: float, wind: float) -> str:
    if wind > 30 or temp > 45 or temp < -5: return "EXTREME"
    if wind > 20:                            return "STORM"
    if temp > 38:                            return "HOT"
    if temp < 5:                             return "COLD"
    return "NORMAL"

def build_message(prev_wind: float) -> tuple[dict, float]:
    temp     = get_temperature()
    humidity = get_humidity(temp)
    wind     = get_wind_speed(prev_wind)
    severity = get_severity(temp, wind)

    msg = {
        "schema_version":       "1.0",
        "station_id":           "WS-MAIN",
        "timestamp":            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "temperature_c":        temp,
        "feels_like_c":         get_feels_like(temp, humidity, wind),
        "humidity_pct":         humidity,
        "solar_irradiance_wm2": get_solar_irradiance(),
        "wind_speed_ms":        wind,
        "weather_severity":     severity,
    }
    jsonschema.validate(instance=msg, schema=SCHEMA)
    return msg, wind

# ── Delivery callback ─────────────────────────────────────────────────────────
def on_delivery(err, msg):
    if err:
        print(f"[ERROR] {msg.topic()} | {err}")
    else:
        print(f"[OK] {msg.topic()} | partition={msg.partition()}")

# ── Graceful shutdown ─────────────────────────────────────────────────────────
producer = Producer(PRODUCER_CONFIG)
running  = True
wind     = 3.0

def shutdown(sig, frame):
    global running
    print("\n[INFO] Shutting down...")
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Weather producer — interval={PRODUCER_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
while running:
    try:
        msg, wind = build_message(wind)
        set_weather(msg["weather_severity"], msg["temperature_c"])
        producer.produce(
            topic    = TOPIC_WEATHER,
            key      = msg["station_id"],
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        print(f"  → temp={msg['temperature_c']}°C | "
              f"feels={msg['feels_like_c']}°C | "
              f"irr={msg['solar_irradiance_wm2']}W/m² | "
              f"severity={msg['weather_severity']}")
        producer.flush()
    except jsonschema.ValidationError as e:
        print(f"[SCHEMA ERROR] {e.message}")
    except Exception as e:
        print(f"[ERROR] {e}")

    time.sleep(PRODUCER_INTERVAL)

producer.flush()
print("[INFO] Producer stopped.")