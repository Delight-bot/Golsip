"""Phase 1: stream live Wikipedia edits from Wikimedia EventStreams into Kafka."""

import json
import sys
import time

import requests
from confluent_kafka import Producer

# Windows terminals default to cp1252, which cannot print non-Western page
# titles. Kafka always gets UTF-8; this just stops printing from crashing.
sys.stdout.reconfigure(encoding="utf-8")

BROKER = "localhost:9092"
TOPIC = "gossip.wiki.edits"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
RECONNECT_SECONDS = 5

# Wikimedia asks every client to identify itself. Put your own contact here.
USER_AGENT = "Gossip/0.1 (Kafka learning project; https://github.com/YOUR-USERNAME)"


def on_delivery(error, message):
    """Only shout when something goes wrong; success is the boring normal case."""
    if error:
        print(f"Delivery failed: {error}")


def open_stream(last_event_id):
    """Open the event stream, resuming after `last_event_id` when we have one."""
    headers = {"User-Agent": USER_AGENT}
    if last_event_id:
        # Wikimedia replays from just after this point, so a dropped
        # connection doesn't mean dropped edits.
        headers["Last-Event-ID"] = last_event_id

    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=60)
    response.raise_for_status()
    return response


producer = Producer({"bootstrap.servers": BROKER})

print(f"Connecting to {STREAM_URL} ...")
print(f"Sending edits to '{TOPIC}'. Press Ctrl+C to stop.\n")

sent = 0
last_event_id = None

try:
    while True:
        try:
            response = open_stream(last_event_id)

            for line in response.iter_lines(decode_unicode=True):
                # SSE sends the event's bookmark on its own "id:" line,
                # just before the "data:" line it belongs to.
                if line.startswith("id: "):
                    last_event_id = line[len("id: "):]
                    continue

                # Blank lines and comments are keep-alives; the real payload
                # always starts with "data: ".
                if not line or not line.startswith("data: "):
                    continue

                event = json.loads(line[len("data: "):])

                # The stream carries edits, new pages, log actions and more.
                # For now we only want plain edits.
                if event.get("type") != "edit":
                    continue

                edit = {
                    "title": event.get("title"),
                    "wiki": event.get("wiki"),
                    "user": event.get("user"),
                    "bot": event.get("bot"),
                    # 0 means a real article; everything else is a File:,
                    # Talk:, Category: or other housekeeping page.
                    "namespace": event.get("namespace"),
                    "timestamp": event.get("timestamp"),
                }

                # The key decides which partition the message lands in. Keying
                # by page title means every edit to the same page goes to the
                # same partition, which keeps that page's history in order.
                producer.produce(
                    TOPIC,
                    key=edit["title"].encode("utf-8"),
                    value=json.dumps(edit).encode("utf-8"),
                    callback=on_delivery,
                )

                # Gives the producer a moment to run delivery callbacks.
                # Without this, errors would pile up unseen in the queue.
                producer.poll(0)

                sent += 1
                if sent % 25 == 0:
                    print(f"{sent} edits sent   (latest: {edit['title']})")

            print("Stream closed by Wikimedia.")

        except requests.exceptions.RequestException as error:
            # Wikimedia restarts stream servers regularly, so a dropped
            # connection is routine rather than a failure.
            print(f"Stream dropped: {error}")

        print(f"Reconnecting in {RECONNECT_SECONDS}s...\n")
        time.sleep(RECONNECT_SECONDS)

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    # Push anything still sitting in the buffer before we exit.
    producer.flush()
    print(f"Done. {sent} edits sent in total.")
