import json
import signal
import sys
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.kafka_config import (
    CONSUMER_CONFIG, TOPIC_METERS, TOPIC_WEATHER, TOPIC_INCIDENTS
)

TOPICS = [TOPIC_METERS, TOPIC_WEATHER, TOPIC_INCIDENTS]

config = {**CONSUMER_CONFIG, "group.id": "verify-topics", "auto.offset.reset": "latest"}
consumer = Consumer(config)
consumer.subscribe(TOPICS)

counts = Counter()
running = True


def shutdown(*_):
    global running
    running = False


signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

print(f"[INFO] Subscribed to: {TOPICS}")
print(f"[INFO] Press Ctrl-C to stop and see summary.\n")

try:
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue
        counts[msg.topic()] += 1
        try:
            value = json.loads(msg.value())
            preview = json.dumps(value)[:120]
        except Exception:
            preview = str(msg.value())[:120]
        print(f"[{msg.topic()} | p{msg.partition()} | off={msg.offset()}] {preview}")
finally:
    consumer.close()
    print("\n=== SUMMARY ===")
    for t in TOPICS:
        print(f"  {t}: {counts[t]} messages")
