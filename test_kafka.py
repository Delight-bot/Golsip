"""Phase 0 sanity check: send one message to Kafka, then read it back."""

import time

from confluent_kafka import Consumer, Producer

BROKER = "localhost:9092"
TOPIC = "gossip.test"
MESSAGE = "meow, kafka is alive"


def on_delivery(error, message):
    """Kafka calls this once it knows whether our message actually landed."""
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(f"Sent! (topic={message.topic()}, partition={message.partition()})")


# --- Producer: write one message -------------------------------------------
producer = Producer({"bootstrap.servers": BROKER})

# produce() only queues the message in a background buffer; it does not block.
producer.produce(TOPIC, MESSAGE.encode("utf-8"), callback=on_delivery)

# flush() waits for the buffer to empty, so the callback above runs before we move on.
producer.flush()


# --- Consumer: read it back ------------------------------------------------
consumer = Consumer(
    {
        "bootstrap.servers": BROKER,
        # A consumer group is a named team of readers. Kafka remembers how far
        # each group has read, so restarting picks up where it left off.
        "group.id": "gossip-test-reader",
        # Only used the first time this group ever reads: start from the oldest
        # message instead of skipping straight to new ones.
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([TOPIC])

deadline = time.time() + 30
try:
    while time.time() < deadline:
        # poll() asks Kafka "anything for me?" and waits up to 1 second.
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received: {msg.value().decode('utf-8')}")
        break
    else:
        print("Timed out: nothing arrived in 30 seconds.")
finally:
    consumer.close()
