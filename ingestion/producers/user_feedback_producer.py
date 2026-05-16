"""
User Feedback Producer — Section A ingestion for B.4 NLP analysis.

Simulates user feedback from support platforms (web form, mobile app, call
center, email). The producer biases sentiment + category distributions based
on current weather to create realistic correlation:
  - STORM/EXTREME weather  -> more OUTAGE complaints, NEGATIVE sentiment
  - HOT weather            -> more BILLING complaints (AC drives bills up)
  - NORMAL weather         -> mostly GENERAL and POSITIVE

Output topic: user-feedback (3 partitions, keyed by zone)
Consumed by:  P2 Spark for NLP (sentiment, topic modeling, keyword extraction)
"""

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
from config.kafka_config import PRODUCER_CONFIG, TOPIC_FEEDBACK, FEEDBACK_INTERVAL
from producers.shared_state import get_weather

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "user_feedback.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Templates by category and sentiment ───────────────────────────────────────
# Each entry: text template with placeholders. P2 NLP will tokenize these.
FEEDBACK_TEMPLATES = {
    ("OUTAGE", "NEGATIVE"): [
        "Power has been out in zone {zone} for {duration} minutes now. This is unacceptable, I am working from home and losing important deadlines. Please restore service immediately.",
        "Total blackout in our neighborhood {zone} since {hour} oclock. No notification, no estimated restoration time. Customer service line is unreachable.",
        "Third outage this month in zone {zone}. The grid infrastructure is clearly failing. We need answers and compensation for spoiled food in fridges.",
        "Power flickering constantly in zone {zone} for the last hour. Electronics are getting damaged. Voltage seems unstable, need urgent technical intervention.",
    ],
    ("OUTAGE", "NEUTRAL"): [
        "Reporting outage in zone {zone} starting around {hour}. About {duration} households appear affected on our street.",
        "Service interruption in zone {zone} since {hour}. Backup generator activated. Standing by for restoration update.",
    ],
    ("BILLING", "NEGATIVE"): [
        "My bill jumped {percent} percent compared to last month in zone {zone}. I have not changed consumption habits. The meter must be faulty or there is an error.",
        "Charged twice for the same billing period in zone {zone}. Bank statement shows two debits of equal amount. Need immediate refund.",
        "Late payment fee applied despite paying on time. Zone {zone} account number redacted. Customer portal incorrectly flagged my payment as missing.",
        "Tariff change in zone {zone} was not communicated clearly. Bill is now {percent} percent higher with no advance notice. This violates consumer protection rules.",
    ],
    ("BILLING", "POSITIVE"): [
        "Thank you for the discount applied to zone {zone} accounts during the maintenance period. The clear billing breakdown was appreciated.",
    ],
    ("BILLING", "NEUTRAL"): [
        "Requesting a copy of my detailed consumption history for zone {zone}, the last {duration} months. Need it for tax purposes.",
    ],
    ("QUALITY", "NEGATIVE"): [
        "Voltage in zone {zone} is consistently low. My computer reboots randomly. Sensitive equipment is malfunctioning. Need a technician to verify line quality.",
        "Frequent micro-cuts in zone {zone} every {duration} minutes during peak hours. Damaging appliances. Grid is overloaded and needs reinforcement.",
        "Power surges damaged my refrigerator in zone {zone} after last storm. Filing a damage claim. Need confirmation of grid fault on {hour} oclock.",
    ],
    ("QUALITY", "NEUTRAL"): [
        "Minor voltage variations noticed in zone {zone} during evening peak hours. Not severe but worth investigating.",
    ],
    ("GENERAL", "POSITIVE"): [
        "Quick response time from the technical team for the recent issue in zone {zone}. Service was restored within {duration} minutes. Well done.",
        "Smart meter installation in zone {zone} went smoothly. The new app makes it easy to track consumption. Thank you.",
        "Customer service representative was helpful and professional regarding my zone {zone} inquiry. Issue resolved on first contact.",
        "Renewable energy share increase in zone {zone} is encouraging. Glad to see investments in solar capacity for the region.",
    ],
    ("GENERAL", "NEUTRAL"): [
        "Inquiry about smart meter data export options for zone {zone}. Would like to integrate with my home automation system.",
        "When will the planned maintenance window in zone {zone} take place? Need to schedule operations accordingly.",
        "Question regarding the new tariff structure effective next month for zone {zone} residents.",
    ],
    ("GENERAL", "NEGATIVE"): [
        "Mobile app keeps crashing when checking zone {zone} consumption. Multiple users have reported the same issue on social media.",
        "Customer service hold times in zone {zone} are unacceptable, over {duration} minutes wait on average. Need more agents.",
    ],
}

# ── ID counter ────────────────────────────────────────────────────────────────
# UTC dates so the ID date matches the message timestamp date.
_counter     = 0
_counter_day = datetime.now(timezone.utc).date()


def next_feedback_id() -> str:
    """Generate a unique daily-sequential feedback ID like FB-20260515-001."""
    global _counter, _counter_day
    today = datetime.now(timezone.utc).date()
    if today != _counter_day:
        _counter = 0
        _counter_day = today
    _counter += 1
    return f"FB-{today.strftime('%Y%m%d')}-{_counter:03d}"


# ── Weather-driven sampling ───────────────────────────────────────────────────
def sample_category_and_sentiment() -> tuple[str, str]:
    """
    Pick (category, sentiment) biased by current weather.
    Hot weather -> more billing complaints; storm -> more outage complaints.
    """
    severity = get_weather()["weather_severity"]
    if severity in ("STORM", "EXTREME"):
        category = random.choices(
            ["OUTAGE", "QUALITY", "GENERAL", "BILLING"], weights=[55, 25, 15, 5])[0]
    elif severity == "HOT":
        category = random.choices(
            ["BILLING", "QUALITY", "GENERAL", "OUTAGE"], weights=[45, 25, 20, 10])[0]
    elif severity == "COLD":
        category = random.choices(
            ["BILLING", "OUTAGE", "GENERAL", "QUALITY"], weights=[40, 25, 25, 10])[0]
    else:  # NORMAL
        category = random.choices(
            ["GENERAL", "BILLING", "QUALITY", "OUTAGE"], weights=[40, 30, 20, 10])[0]

    # Sentiment heavily depends on category
    if category == "OUTAGE":
        sentiment = random.choices(["NEGATIVE", "NEUTRAL"], weights=[80, 20])[0]
    elif category == "BILLING":
        sentiment = random.choices(["NEGATIVE", "NEUTRAL", "POSITIVE"], weights=[60, 30, 10])[0]
    elif category == "QUALITY":
        sentiment = random.choices(["NEGATIVE", "NEUTRAL"], weights=[70, 30])[0]
    else:  # GENERAL
        sentiment = random.choices(["POSITIVE", "NEUTRAL", "NEGATIVE"], weights=[45, 35, 20])[0]

    # If no template exists for the pair, fall back to whatever templates exist for category
    if (category, sentiment) not in FEEDBACK_TEMPLATES:
        candidates = [k for k in FEEDBACK_TEMPLATES if k[0] == category]
        if candidates:
            category, sentiment = random.choice(candidates)
    return category, sentiment


# ── Builder ───────────────────────────────────────────────────────────────────
def build_message() -> dict:
    """Compose a single feedback message with realistic content."""
    category, sentiment = sample_category_and_sentiment()
    text_template       = random.choice(FEEDBACK_TEMPLATES[(category, sentiment)])

    # Pick zone ONCE so the text body and the zone field are consistent.
    # (Independent picks would silently break P2 NLP↔incident correlation.)
    zone = random.choice(["A", "B", "C", "D"])

    text = text_template.format(
        zone     = zone,
        duration = random.randint(5, 240),
        hour     = random.randint(0, 23),
        percent  = random.randint(15, 80),
    )

    msg = {
        "schema_version": "1.0",
        "feedback_id":    next_feedback_id(),
        "timestamp":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id":        f"U-{random.randint(10000, 99999):05d}",
        "zone":           zone,
        "channel":        random.choice(["WEB", "MOBILE", "CALL_CENTER", "EMAIL"]),
        "sentiment":      sentiment,
        "category":       category,
        "text":           text[:1000],  # respect schema maxLength
        "resolved":       False,
    }
    jsonschema.validate(instance=msg, schema=SCHEMA)
    return msg


# ── Metrics ───────────────────────────────────────────────────────────────────
_sent_count  = 0
_error_count = 0
_start_time  = time.time()


def on_delivery(err, msg):
    """Kafka delivery callback — increment counters."""
    global _sent_count, _error_count
    if err:
        _error_count += 1
        print(f"[ERROR] {msg.topic()} | {err}")
    else:
        _sent_count += 1


def print_metrics():
    """Print throughput metrics every 10 cycles."""
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

print(f"[INFO] User feedback producer - interval={FEEDBACK_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
_backoff = 1

while running:
    _cycle += 1
    try:
        msg = build_message()
        producer.produce(
            topic    = TOPIC_FEEDBACK,
            key      = msg["zone"],       # partition by zone for locality
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        producer.poll(0)
        _backoff = 1
        print(f"  -> {msg['feedback_id']} | zone={msg['zone']} | "
              f"{msg['sentiment']:<8} | {msg['category']:<8} | "
              f"{msg['channel']:<11} | {msg['text'][:60]}...")
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

    time.sleep(FEEDBACK_INTERVAL)

producer.flush(timeout=5)
print_metrics()
print("[INFO] Producer stopped.")
