"""
End-to-end pipeline verifier.

Subscribes to all 6 P1 topics, validates every message against its JSON schema,
runs basic sanity checks, and exits with a non-zero code if anything looks
wrong. Used as the demo's proof-of-correctness step and as a CI gate.

Run:
    .venv\\Scripts\\python -m ingestion.consumers.verify_topics             (Windows)
    .venv/bin/python -m ingestion.consumers.verify_topics                   (Linux/WSL)

Optional flags:
    --max-seconds N     stop after N seconds (default: run until Ctrl-C)
    --require-each      require >=1 valid msg per topic for exit 0 (default: on)
    --from-beginning    consume from earliest offset (default: latest)

Exit codes:
    0  every subscribed topic produced at least one schema-valid message AND
       no schema violation or sanity-check failure was observed.
    1  at least one schema violation or sanity-check failure was observed.
    2  one or more topics produced zero messages while the consumer was running
       (use --no-require-each if topics with long intervals are expected to
       stay silent during a short verification window).
"""

import argparse
import json
import signal
import sys
import time
from collections import Counter
from pathlib import Path

import jsonschema
from confluent_kafka import Consumer, KafkaError

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import (
    CONSUMER_CONFIG,
    TOPIC_METERS, TOPIC_WEATHER, TOPIC_INCIDENTS,
    TOPIC_RSS, TOPIC_PRICES, TOPIC_FEEDBACK,
)

# ── Schema map ────────────────────────────────────────────────────────────────
SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"
TOPIC_TO_SCHEMA_FILE = {
    TOPIC_METERS:    "smart_meter.schema.json",
    TOPIC_WEATHER:   "weather.schema.json",
    TOPIC_INCIDENTS: "incident_report.schema.json",
    TOPIC_RSS:       "rss_feed.schema.json",
    TOPIC_PRICES:    "market_price.schema.json",
    TOPIC_FEEDBACK:  "user_feedback.schema.json",
}
SCHEMAS = {
    topic: json.loads((SCHEMA_DIR / fname).read_text(encoding="utf-8"))
    for topic, fname in TOPIC_TO_SCHEMA_FILE.items()
}
TOPICS = list(TOPIC_TO_SCHEMA_FILE.keys())


# ── Sanity checks (cheap invariants beyond what the schema can express) ───────
def sanity_check(topic: str, value: dict) -> str | None:
    """Return None if OK, otherwise an error message string."""
    if topic == TOPIC_METERS:
        if value["consumption_kwh"] <= 0:
            return f"non-positive consumption_kwh={value['consumption_kwh']}"
        if value["is_anomaly"] and value["anomaly_reason"] is None:
            return "is_anomaly=True but anomaly_reason is null"
        if not value["is_anomaly"] and value["anomaly_reason"] is not None:
            return f"is_anomaly=False but anomaly_reason={value['anomaly_reason']}"
    elif topic == TOPIC_WEATHER:
        if value["humidity_pct"] < 0 or value["humidity_pct"] > 100:
            return f"humidity_pct out of [0,100]: {value['humidity_pct']}"
    elif topic == TOPIC_INCIDENTS:
        if not value["affected_meters"]:
            return "affected_meters is empty"
    elif topic == TOPIC_PRICES:
        # Sanity-check the cross-currency invariant: price_mad ~= price_eur * 10.7
        ratio = value["price_mad_mwh"] / max(value["price_eur_mwh"], 1e-6)
        if abs(ratio - 10.7) > 1.0:
            return f"price_mad/price_eur ratio={ratio:.2f}, expected ~10.7"
    elif topic == TOPIC_FEEDBACK:
        # The text body should mention the same zone as the zone field — this
        # was a real bug pre-fix; keep the check as a regression guard.
        if f"zone {value['zone']}" not in value["text"]:
            return f"text does not reference zone={value['zone']}"
    return None


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Verify all 6 P1 topics end-to-end.")
    ap.add_argument("--max-seconds", type=int, default=0,
                    help="Stop after N seconds (0 = run until Ctrl-C).")
    ap.add_argument("--from-beginning", action="store_true",
                    help="Consume from earliest offset (default: latest only).")
    ap.add_argument("--no-require-each", dest="require_each",
                    action="store_false", default=True,
                    help="Don't require >=1 message per topic for exit 0.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    config = {
        **CONSUMER_CONFIG,
        "group.id":          "verify-topics",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
    }
    consumer = Consumer(config)
    consumer.subscribe(TOPICS)

    valid   = Counter()
    invalid = Counter()
    sanity  = Counter()
    running = True

    def shutdown(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"[INFO] Subscribed to: {TOPICS}")
    print(f"[INFO] Schema validation: ON  |  Sanity checks: ON")
    if args.max_seconds:
        print(f"[INFO] Will stop after {args.max_seconds}s.")
    print(f"[INFO] Press Ctrl-C to stop and see summary.\n")

    deadline = time.time() + args.max_seconds if args.max_seconds else None

    try:
        while running:
            if deadline and time.time() >= deadline:
                break
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[KAFKA ERROR] {msg.error()}")
                continue

            topic = msg.topic()
            try:
                value = json.loads(msg.value())
            except json.JSONDecodeError as e:
                invalid[topic] += 1
                print(f"[INVALID JSON]  {topic} | offset={msg.offset()} | {e}")
                continue

            try:
                jsonschema.validate(value, SCHEMAS[topic])
            except jsonschema.ValidationError as e:
                invalid[topic] += 1
                print(f"[INVALID SCHEMA] {topic} | offset={msg.offset()} | {e.message}")
                continue

            err = sanity_check(topic, value)
            if err:
                sanity[topic] += 1
                print(f"[SANITY FAIL]   {topic} | offset={msg.offset()} | {err}")
                continue

            valid[topic] += 1
            preview = json.dumps(value)[:120]
            print(f"[OK] {topic:<18} | p{msg.partition()} | off={msg.offset()} | {preview}")
    finally:
        consumer.close()

    # ── Summary + exit code ──────────────────────────────────────────────────
    print("\n=== SUMMARY ===")
    print(f"{'topic':<20} {'valid':>8} {'schema_invalid':>16} {'sanity_failed':>15}")
    for t in TOPICS:
        print(f"{t:<20} {valid[t]:>8} {invalid[t]:>16} {sanity[t]:>15}")

    total_valid    = sum(valid.values())
    total_invalid  = sum(invalid.values())
    total_sanity   = sum(sanity.values())
    missing_topics = [t for t in TOPICS if valid[t] == 0]

    print(f"\nTotals: valid={total_valid}  schema_invalid={total_invalid}  sanity_failed={total_sanity}")
    if missing_topics:
        print(f"Topics with ZERO valid messages: {missing_topics}")

    if total_invalid > 0 or total_sanity > 0:
        print("[FAIL] Schema or sanity violations observed.")
        return 1
    if args.require_each and missing_topics:
        print("[FAIL] At least one topic produced no messages during the run.")
        return 2
    print("[PASS] All subscribed topics delivered schema-valid messages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
