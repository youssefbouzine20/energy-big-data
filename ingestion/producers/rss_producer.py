"""
RSS Feed Producer — Section A.3 of the professor's spec.

Simulates RSS feed items from energy/industrial news sources that may impact
electricity consumption patterns. In production, a real feed reader (e.g.
feedparser) would pull from RSS endpoints of utilities and regulators. For
the academic demo we generate realistic synthetic items so the pipeline is
self-contained and reproducible.

Output topic: rss-feeds (1 partition, low throughput)
Consumed by:  P2 Spark for NLP analysis + ML feature engineering
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
from config.kafka_config import PRODUCER_CONFIG, TOPIC_RSS, RSS_INTERVAL

# ── Schema ────────────────────────────────────────────────────────────────────
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "rss_feed.schema.json"
with open(SCHEMA_PATH) as f:
    SCHEMA = json.load(f)

# ── Sources ───────────────────────────────────────────────────────────────────
# Real sites that publish energy news (we simulate articles from them).
SOURCES = [
    "energie-info.fr",
    "lemoniteur.fr",
    "lesechos.fr/energie",
    "bloomberg.com/energy",
    "reuters.com/business/energy",
    "masen.ma",
    "one.org.ma",
    "iea.org",
]

REGIONS  = ["Tetouan", "Tanger", "Rabat", "Casablanca", "Marrakech", "Fes", "Agadir", "Oujda"]
COMPANIES = ["ONEE", "Masen", "Acwa Power", "Nareva Holding", "Engie Morocco", "Taqa Morocco", "EDF Renewables"]

# ── Templates by category ─────────────────────────────────────────────────────
# Each template: (title_template, summary_template, keywords, impact_range)
TEMPLATES = {
    "INDUSTRIAL_NEWS": [
        ("Grid upgrade announced in {region}: {company} to invest {amount}M EUR",
         "{company} announced today a major investment of {amount} million euros in grid infrastructure for the {region} region. The project includes substation modernization, smart meter rollout, and capacity expansion. Expected completion in {year}.",
         ["grid", "infrastructure", "modernization", "investment"], (0.4, 0.7)),
        ("Industrial complex commissions {capacity}MW capacity in {region}",
         "A new industrial complex in {region} has commissioned {capacity}MW of dedicated electrical capacity. The facility expects to draw {energy}MWh annually, primarily during off-peak hours to optimize grid load.",
         ["industrial", "capacity", "commissioning", "demand"], (0.5, 0.8)),
    ],
    "REGULATION": [
        ("New energy efficiency regulation takes effect in {region}",
         "Authorities in {region} have enacted new energy efficiency standards requiring industrial consumers above 1MW to report consumption hourly. Penalties for non-compliance reach {amount} EUR per infraction.",
         ["regulation", "compliance", "efficiency", "policy"], (0.3, 0.6)),
        ("Carbon tax adjustment: {rate} percent increase scheduled for {year}",
         "Regulatory authority confirmed a {rate} percent carbon tax increase effective {year}. Energy-intensive industries are expected to adjust consumption patterns, with grid operators anticipating a {rate2} percent reduction in peak-hour demand.",
         ["carbon", "tax", "policy", "emissions"], (0.4, 0.7)),
    ],
    "MARKET": [
        ("Electricity spot prices surge {rate} percent on heatwave forecast",
         "Day-ahead market prices climbed {rate} percent in response to forecasted heatwave conditions across {region}. Traders cite air conditioning demand spike and reduced wind generation as primary drivers. Peak hour prices reached {amount} EUR per MWh.",
         ["market", "prices", "heatwave", "spot"], (0.6, 0.9)),
        ("Energy market consolidation: {company} acquires regional supplier",
         "{company} announced acquisition of a regional electricity supplier serving {amount} customers in {region}. The deal, valued at {rate} million EUR, is expected to close pending regulatory approval.",
         ["market", "acquisition", "consolidation"], (0.2, 0.5)),
    ],
    "RENEWABLE": [
        ("Solar farm of {capacity}MW connects to grid in {region}",
         "A new {capacity}MW solar photovoltaic farm has been commissioned and connected to the grid in {region}. Annual production estimated at {energy}GWh, contributing approximately {rate} percent to regional renewable energy share.",
         ["solar", "renewable", "photovoltaic", "grid"], (0.5, 0.8)),
        ("Wind project receives {amount}M EUR green financing for {region}",
         "Green financing of {amount} million EUR has been secured for a {capacity}MW wind project in {region}. Construction begins {year} with grid connection targeted for {year2}. Expected to displace {rate} percent of regional fossil fuel generation.",
         ["wind", "renewable", "financing", "green"], (0.4, 0.7)),
    ],
    "INFRASTRUCTURE": [
        ("Transmission line modernization completes in {region}",
         "A 4-year transmission line modernization program in {region} has completed. The upgrade replaced {amount}km of aging conductors and added {capacity}MW transmission capacity. Outage frequency expected to drop {rate} percent year-over-year.",
         ["transmission", "modernization", "reliability"], (0.5, 0.8)),
        ("Smart grid pilot launches in {region} covering {amount} households",
         "A smart grid pilot program covering {amount} households has launched in {region}. The pilot tests demand-response automation, real-time pricing signals, and renewable integration. Initial results expected in Q{rate} {year}.",
         ["smartgrid", "pilot", "demand-response", "iot"], (0.4, 0.7)),
    ],
}

# ── Counter for unique IDs ────────────────────────────────────────────────────
# UTC dates so the ID date matches the message timestamp date.
_counter = 0
_counter_day = datetime.now(timezone.utc).date()


def next_feed_id() -> str:
    """Generate a unique daily-sequential feed ID like RSS-20260515-001."""
    global _counter, _counter_day
    today = datetime.now(timezone.utc).date()
    if today != _counter_day:
        _counter = 0
        _counter_day = today
    _counter += 1
    return f"RSS-{today.strftime('%Y%m%d')}-{_counter:03d}"


# ── Builder ───────────────────────────────────────────────────────────────────
def build_message() -> dict:
    """Build a single RSS feed item with realistic content."""
    category = random.choice(list(TEMPLATES.keys()))
    title_tmpl, summary_tmpl, keywords, (impact_min, impact_max) = random.choice(TEMPLATES[category])

    # Realistic fill-ins
    region   = random.choice(REGIONS)
    company  = random.choice(COMPANIES)
    amount   = random.randint(10, 500)
    capacity = random.randint(50, 800)
    energy   = random.randint(100, 2000)
    rate     = random.randint(5, 35)
    rate2    = random.randint(2, 15)
    year     = random.choice([2026, 2027, 2028])
    year2    = year + random.randint(1, 3)

    title   = title_tmpl.format(region=region, company=company, amount=amount,
                                capacity=capacity, energy=energy, rate=rate, year=year)
    summary = summary_tmpl.format(region=region, company=company, amount=amount,
                                  capacity=capacity, energy=energy, rate=rate,
                                  rate2=rate2, year=year, year2=year2)

    msg = {
        "schema_version": "1.0",
        "feed_id":        next_feed_id(),
        "timestamp":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":         random.choice(SOURCES),
        "category":       category,
        "title":          title[:200],     # respect schema maxLength
        "summary":        summary[:1000],
        "impact_score":   round(random.uniform(impact_min, impact_max), 2),
        "keywords":       keywords,
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
    """Print throughput metrics every 10 cycles for the demo."""
    elapsed_min = (time.time() - _start_time) / 60
    rate = _sent_count / elapsed_min if elapsed_min > 0 else 0
    print(f"[METRICS] sent={_sent_count} errors={_error_count} "
          f"rate={rate:.1f}/min uptime={elapsed_min:.1f}m")


# ── Graceful shutdown ─────────────────────────────────────────────────────────
producer = Producer(PRODUCER_CONFIG)
running  = True
_cycle   = 0


def shutdown(sig, frame):
    """SIGINT/SIGTERM handler — flag main loop to exit cleanly."""
    global running
    print("\n[INFO] Shutting down...")
    running = False


signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] RSS feed producer — interval={RSS_INTERVAL}s")

# ── Main loop ─────────────────────────────────────────────────────────────────
_backoff = 1

while running:
    _cycle += 1
    try:
        msg = build_message()
        producer.produce(
            topic    = TOPIC_RSS,
            key      = msg["category"],   # partition by category for related items
            value    = json.dumps(msg),
            callback = on_delivery,
        )
        producer.poll(0)              # serve delivery callbacks
        _backoff = 1                  # reset backoff on success
        print(f"  -> {msg['feed_id']} | {msg['category']:<15} | "
              f"impact={msg['impact_score']} | {msg['title'][:60]}...")
        # No per-cycle flush — poll(0) services callbacks; final flush on shutdown.
    except BufferError:
        producer.poll(1)              # local queue full — wait for drain
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

    time.sleep(RSS_INTERVAL)

producer.flush(timeout=5)
print_metrics()
print("[INFO] Producer stopped.")
