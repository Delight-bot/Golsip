"""Phase 4: report edits per minute, the bot/human split, and spot spikes."""

import json
import sys
import time
from collections import deque
from statistics import mean

from confluent_kafka import Consumer, Producer

sys.stdout.reconfigure(encoding="utf-8")

BROKER = "localhost:9092"
SOURCE_TOPIC = "gossip.wiki.edits"
ALERTS_TOPIC = "gossip.alerts"

REPORT_EVERY_SECONDS = 60
HISTORY_MINUTES = 30  # how many past minutes feed the average
WARMUP_MINUTES = 5  # no alerts until we have this much history
SPIKE_MULTIPLIER = 2.0  # "unusual" means this many times the average

# Same definition of gossip as the trend detector: real articles, real people.
WIKIS = {"enwiki"}
ARTICLE_NAMESPACE = 0

# One entry per completed minute: how many gossip edits it held.
history = deque(maxlen=HISTORY_MINUTES)

consumer = Consumer(
    {
        "bootstrap.servers": BROKER,
        # A different group from printer.py, so both receive every edit.
        "group.id": "gossip-stats",
        "auto.offset.reset": "latest",
    }
)
consumer.subscribe([SOURCE_TOPIC])

producer = Producer({"bootstrap.servers": BROKER})

bots = 0
humans = 0
gossip_edits = 0
page_counts = {}  # title -> edits this minute, used to name the spiking page
minute_started = time.time()

print(f"Counting edits on '{SOURCE_TOPIC}'. First report in one minute.")
print(f"Spike alerts start after {WARMUP_MINUTES} minutes of history.\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is not None and not msg.error():
            edit = json.loads(msg.value().decode("utf-8"))

            if edit["bot"]:
                bots += 1
            else:
                humans += 1

            is_gossip = (
                edit["wiki"] in WIKIS
                and edit.get("namespace") == ARTICLE_NAMESPACE
                and not edit["bot"]
            )
            if is_gossip:
                gossip_edits += 1
                title = edit["title"]
                page_counts[title] = page_counts.get(title, 0) + 1

        # Checked on every loop, not only when a message arrives, so a quiet
        # minute still gets reported instead of silently waiting.
        elapsed = time.time() - minute_started
        if elapsed < REPORT_EVERY_SECONDS:
            continue

        total = bots + humans
        rate = total / elapsed * 60
        gossip_rate = gossip_edits / elapsed * 60

        if total:
            bot_share = bots / total * 100
            print(
                f"{time.strftime('%H:%M:%S')}  "
                f"{rate:6.1f} edits/min   "
                f"bots {bot_share:4.1f}%  humans {100 - bot_share:4.1f}%   "
                f"gossip {gossip_rate:5.1f}/min"
            )
        else:
            print(f"{time.strftime('%H:%M:%S')}  no edits this minute")

        # A spike is only meaningful against enough normal minutes to compare.
        if len(history) >= WARMUP_MINUTES:
            average = mean(history)

            if average > 0 and gossip_rate > SPIKE_MULTIPLIER * average:
                # Blame the page with the most edits in the spiking minute.
                busiest = max(page_counts, key=page_counts.get, default=None)

                alert = {
                    "page": busiest,
                    "edits_this_minute": page_counts.get(busiest, 0),
                    "rate": round(gossip_rate, 1),
                    "average": round(average, 1),
                    "detected_at": time.time(),
                }
                print(
                    f"           SPIKE! {gossip_rate:.0f}/min vs "
                    f"{average:.0f}/min average  -  {busiest}"
                )
                producer.produce(
                    ALERTS_TOPIC,
                    key=b"spike",
                    value=json.dumps(alert).encode("utf-8"),
                )
                producer.poll(0)

        history.append(gossip_rate)

        bots = 0
        humans = 0
        gossip_edits = 0
        page_counts = {}
        minute_started = time.time()

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    producer.flush()
    consumer.close()
