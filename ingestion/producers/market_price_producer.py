"""
Market Price Producer — Section A external factors (project Objectif: "facteurs
externes (meteo, prix du marche)").

Simulates hourly energy market prices on Morocco/EU spot markets. Prices follow
diurnal demand patterns and respond to weather (HOT weather -> higher prices
due to AC demand). In production, this would consume a real API like ENTSO-E or
ONEE day-ahead bulletins.

Output topic: market-prices (1 partition, hourly cadence)
Consumed by:  P5 ML as an external feature; P4 dashboard for price-vs-load chart
"""

import json
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
from confluent_kafka import KafkaException, Producer

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import PRODUCER_CONFIG, TOPIC_PRICES, PRICE_INTERVAL
from producers.shared_state import get_weather

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "market_price.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Constants (Morocco/EU energy market) ──────────────────────────────────────
MAD_PER_EUR  = 10.7                # approximate exchange rate
BASE_EUR_MWH = 75.0                # baseline price during normal demand
PEAK_HOURS   = set(range(7, 10)) | set(range(17, 22))   # morning + evening peaks

# ── State ─────────────────────────────────────────────────────────────────────
_previous_price = BASE_EUR_MWH


# ── Simulation helpers ────────────────────────────────────────────────────────
def get_diurnal_multiplier() -> float:
    """Return a price multiplier based on hour of day (peaks at 8am and 7pm)."""
    hour = datetime.now(timezone.utc).hour
    # Two-peak sinusoid centered at 8h and 19h
    morning_peak = math.exp(-((hour - 8)  ** 2) / 8)
    evening_peak = math.exp(-((hour - 19) ** 2) / 8)
    base = 0.85 + 0.30 * max(morning_peak, evening_peak)
    return base


def get_weather_multiplier() -> float:
    """Hot/Cold weather raises prices (AC/heating demand spike)."""
    severity = get_weather()["weather_severity"]
    return {
        "EXTREME": random.uniform(1.30, 1.60),
        "HOT":     random.uniform(1.15, 1.30),
        "COLD":    random.uniform(1.10, 1.25),
        "STORM":   random.uniform(1.05, 1.20),
        "NORMAL":  random.uniform(0.95, 1.05),
    }.get(severity, 1.0)


def get_renewable_share() -> float:
    """Solar share peaks midday; total renewable share oscillates 15-55 percent."""
    hour = datetime.now(timezone.utc).hour
    solar_share = 0.0
    if 6 <= hour < 20:
        # Bell curve centered at solar noon (~13h UTC)
        solar_share = 40 * math.exp(-((hour - 13) ** 2) / 16)
    wind_share = random.uniform(8, 18)        # wind is relatively stable
    return round(max(0.0, min(100.0, solar_share + wind_share + random.gauss(0, 3))), 1)


def get_demand_forecast() -> float:
    """Forecast grid-wide demand in MW. Tracks diurnal pattern."""
    return round(2500 + 2500 * get_diurnal_multiplier() + random.gauss(0, 200), 0)


def get_trend(current: float, previous: float) -> str:
    """Categorical trend vs previous hour. 2 percent threshold to avoid jitter."""
    delta_pct = (current - previous) / previous * 100
    if delta_pct >  2: return "RISING"
    if delta_pct < -2: return "FALLING"
    return "STABLE"


# ── Builder ───────────────────────────────────────────────────────────────────
def build_message() -> dict:
    """Compose a single market price message with realistic coupling."""
    global _previous_price

    # Price = base * diurnal pattern * weather modifier + noise
    multiplier      = get_diurnal_multiplier() * get_weather_multiplier()
    price_eur_mwh   = round(BASE_EUR_MWH * multiplier + random.gauss(0, 3), 2)
    price_eur_mwh   = max(10.0, min(500.0, price_eur_mwh))   # respect schema range

    trend           = get_trend(price_eur_mwh, _previous_price)
    _previous_price = price_eur_mwh

    msg = {
        "schema_version":      "1.0",
        "timestamp":           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market":              random.choice(["MASEN", "EU_SPOT", "DAY_AHEAD"]),
        "price_mad_mwh":       round(price_eur_mwh * MAD_PER_EUR, 2),
        "price_eur_mwh":       price_eur_mwh,
        "demand_forecast_mw":  get_demand_forecast(),
        "renewable_share_pct": get_renewable_share(),
        "trend":               trend,
    }
    jsonschema.validate(instance=msg, schema=SCHEMA)
    return msg


# ── Metrics ───────────────────────────────────────────────────────────────────
_sent_count  = 0
_error_count = 0
_start_time  = time.time()


def on_delivery(err, msg):
    """Kafka delivery callback."""
    global _sent_count, _error_count
    if err:
        _error_count += 1
        print(f"[ERROR] {msg.topic()} | {err}")
    else:
        _sent_count += 1


def print_metrics():
    """Print throughput metrics."""
    elapsed_min = (time.time() - _start_time) / 60
    rate = _sent_count / elapsed_min if elapsed_min > 0 else 0
    print(f"[METRICS] sent={_sent_count} errors={_error_count} "
          f"rate={rate:.1f}/min uptime={elapsed_min:.1f}m")


# ── Graceful shutdown ─────────────────────────────────────────────────────────
producer = Producer(PRODUCER_CONFIG)
running  = True
_cycle   = 0


def shutdown(sig, frame):
    """SIGINT/SIGTERM handler."""
    global running
    print("\n[INFO] Shutting down...")
    running = False


signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Market price producer - interval={PRICE_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
_backoff = 1

while running:
    _cycle += 1
    try:
        msg = build_message()
        producer.produce(
            topic    = TOPIC_PRICES,
            key      = msg["market"],
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        producer.poll(0)
        _backoff = 1
        print(f"  -> {msg['market']:<10} | EUR={msg['price_eur_mwh']}/MWh | "
              f"demand={msg['demand_forecast_mw']}MW | "
              f"renew={msg['renewable_share_pct']}% | {msg['trend']}")
        # No per-cycle flush — poll(0) services callbacks; final flush on shutdown.
    except BufferError:
        producer.poll(1)
    except KafkaException as e:
        _error_count += 1
        print(f"[KAFKA ERROR] {e} - backoff {_backoff}s")
        time.sleep(_backoff)
        _backoff = min(_backoff * 2, 30)
    except jsonschema.ValidationError as e:
        print(f"[SCHEMA ERROR] {e.message}")
    except Exception as e:
        print(f"[ERROR] {e}")

    if _cycle % 10 == 0:
        print_metrics()

    time.sleep(PRICE_INTERVAL)

producer.flush(timeout=5)
print_metrics()
print("[INFO] Producer stopped.")
