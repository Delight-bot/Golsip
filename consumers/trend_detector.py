"""Phase 2: rank the pages drawing the most different editors in the last hour."""

import json
import sys
import time
from collections import deque

from confluent_kafka import Consumer, Producer

sys.stdout.reconfigure(encoding="utf-8")

BROKER = "localhost:9092"
SOURCE_TOPIC = "gossip.wiki.edits"
TRENDING_TOPIC = "gossip.trending"

WINDOW_SECONDS = 3600  # one hour
TOP_N = 10
REPORT_EVERY_SECONDS = 30

# Which wikis count as gossip. Wikidata and Commons are machine-facing
# catalogues, so they stay out. Add more codes here (e.g. "frwiki").
WIKIS = {"enwiki"}

# Namespace 0 is the real article space; everything else is housekeeping.
ARTICLE_NAMESPACE = 0

# Every edit currently inside the window, oldest first: (arrival_time, title, user).
window = deque()

# title -> {username: how many edits they made inside the window}. The number
# of names in here is what we rank by: 20 people beats 20 edits by one person.
editors = {}


def remember(title, user, now):
    window.append((now, title, user))
    people = editors.setdefault(title, {})
    people[user] = people.get(user, 0) + 1


def forget_old_edits(now):
    """Drop anything that has aged out of the back of the window."""
    cutoff = now - WINDOW_SECONDS
    while window and window[0][0] < cutoff:
        _, old_title, old_user = window.popleft()
        people = editors[old_title]

        people[old_user] -= 1
        if people[old_user] == 0:
            del people[old_user]
        if not people:
            del editors[old_title]


def leaderboard():
    """The TOP_N pages with the most distinct editors, biggest first."""
    # Most editors wins; total edits breaks ties so pages don't sit at the
    # top just because they arrived first.
    ranked = sorted(
        editors.items(),
        key=lambda pair: (len(pair[1]), sum(pair[1].values())),
        reverse=True,
    )
    return [
        {
            "title": title,
            "editors": len(people),
            "edits": sum(people.values()),
        }
        for title, people in ranked[:TOP_N]
    ]


consumer = Consumer(
    {
        "bootstrap.servers": BROKER,
        "group.id": "gossip-trend-detector",
        # Trends are about right now, so skip the backlog and read live edits.
        "auto.offset.reset": "latest",
    }
)
consumer.subscribe([SOURCE_TOPIC])

producer = Producer({"bootstrap.servers": BROKER})

print(
    f"Ranking the top {TOP_N} {'/'.join(sorted(WIKIS))} articles of the last "
    f"{WINDOW_SECONDS // 60} minutes, by number of different editors.\n"
    f"First report in {REPORT_EVERY_SECONDS}s. Press Ctrl+C to stop.\n"
)

started = time.time()
last_report = time.time()

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        now = time.time()
        forget_old_edits(now)

        if msg is not None and not msg.error():
            edit = json.loads(msg.value().decode("utf-8"))

            # Bots and batch tools are activity, not public interest, so the
            # ranking ignores them along with other wikis and housekeeping pages.
            interesting = (
                edit["wiki"] in WIKIS
                and edit.get("namespace") == ARTICLE_NAMESPACE
                and not edit["bot"]
            )
            if interesting:
                remember(edit["title"], edit["user"], now)

        if now - last_report < REPORT_EVERY_SECONDS:
            continue
        last_report = now

        top = leaderboard()

        # Until an hour has passed the window only holds what we've seen so
        # far, so say how much history these numbers actually cover.
        covered = min(now - started, WINDOW_SECONDS)
        print(
            f"\n🔥 Hot right now  (last {covered / 60:.0f} min, "
            f"{len(editors)} articles tracked)"
        )
        if top:
            for rank, page in enumerate(top, start=1):
                print(
                    f"  {rank:2}. {page['editors']:3} editors "
                    f"/{page['edits']:3} edits   {page['title']}"
                )
        else:
            print("  (nothing yet)")

        # One snapshot of the whole leaderboard, which is exactly what the
        # Phase 4 dashboard will want to draw. A fixed key sends every
        # snapshot to the same partition, so they stay in order.
        snapshot = {
            "generated_at": now,
            "window_minutes": WINDOW_SECONDS // 60,
            "top": top,
        }
        producer.produce(
            TRENDING_TOPIC,
            key=b"leaderboard",
            value=json.dumps(snapshot).encode("utf-8"),
        )
        producer.poll(0)

except KeyboardInterrupt:
    print("\nStopping...")
finally:
    producer.flush()
    consumer.close()
