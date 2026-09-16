"""Phase 2: read edits back out of Kafka and print them. The simplest consumer."""

import json
import sys

from confluent_kafka import Consumer

sys.stdout.reconfigure(encoding="utf-8")

BROKER = "localhost:9092"
TOPIC = "gossip.wiki.edits"

consumer = Consumer(
    {
        "bootstrap.servers": BROKER,
        "group.id": "gossip-printer",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

print(f"Reading '{TOPIC}'. Press Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        edit = json.loads(msg.value().decode("utf-8"))
        who = "bot " if edit["bot"] else "human"

        print(f"[p{msg.partition()}] {who}  {edit['wiki']:<12} {edit['title']}")

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    # Tells Kafka we're leaving the group so it can hand our work to someone
    # else immediately, instead of waiting for us to time out.
    consumer.close()
