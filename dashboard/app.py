"""Phase 4: live dashboard for the gossip topics, with a cat that gasps."""

import base64
import json
import time
from collections import deque
from itertools import islice
from pathlib import Path

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer

BROKER = "localhost:9092"
EDITS_TOPIC = "gossip.wiki.edits"
TRENDING_TOPIC = "gossip.trending"
ALERTS_TOPIC = "gossip.alerts"

GASP_SECONDS = 10  # how long the cat stays shocked after an alert
REFRESH_SECONDS = 2
CHART_MINUTES = 15
RECENT_EDITS = 12

# Kept long enough that changing the watchlist re-filters recent history
# instead of only affecting edits that arrive from now on.
BUFFER_SIZE = 3000
WATCHLIST_MATCHES = 10

DEFAULT_WATCHLIST = "election\nfootball\nfilm\nmusic"

# The watchlist only looks at real articles written by people, same as trending.
WIKIS = {"enwiki"}
ARTICLE_NAMESPACE = 0

ASSETS = Path(__file__).parent / "assets"

st.set_page_config(page_title="Gol-sip", page_icon="🐱", layout="wide")


@st.cache_resource
def get_consumer():
    """Built once and reused. Streamlit reruns this whole file on every
    refresh, so without caching we'd rejoin the group twice a second."""
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "gossip-dashboard",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([EDITS_TOPIC, TRENDING_TOPIC, ALERTS_TOPIC])
    return consumer


@st.cache_data
def load_cat(filename):
    """Read an SVG off disk and wrap it as a data URI the browser can show."""
    svg = (ASSETS / filename).read_text(encoding="utf-8")
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


state = st.session_state
if "edits_per_minute" not in state:
    state.edits_per_minute = {}  # minute number -> how many edits
    state.bots = 0
    state.humans = 0
    state.recent = deque(maxlen=BUFFER_SIZE)
    state.leaderboard = []
    state.alert = None


def absorb(messages):
    """Fold a batch of Kafka messages into the numbers we display."""
    for msg in messages:
        if msg.error():
            continue

        payload = json.loads(msg.value().decode("utf-8"))
        topic = msg.topic()

        if topic == EDITS_TOPIC:
            minute = int(time.time() // 60)
            state.edits_per_minute[minute] = state.edits_per_minute.get(minute, 0) + 1

            if payload["bot"]:
                state.bots += 1
            else:
                state.humans += 1

            state.recent.appendleft(payload)

        elif topic == TRENDING_TOPIC:
            # A topic keeps everything ever written to it, including messages
            # from older versions of the detector that had a different shape.
            # Skip anything that isn't a leaderboard snapshot.
            if "top" in payload:
                # Each snapshot replaces the last, so we draw the newest.
                state.leaderboard = payload["top"]

        elif topic == ALERTS_TOPIC:
            state.alert = payload


absorb(get_consumer().consume(num_messages=3000, timeout=0.3))

# Forget minutes that have scrolled off the chart.
cutoff = int(time.time() // 60) - CHART_MINUTES
state.edits_per_minute = {
    minute: count for minute, count in state.edits_per_minute.items() if minute > cutoff
}

st.title("Gol-sip")
st.caption("Live Wikipedia chatter, streamed through Apache Kafka")

with st.sidebar:
    st.subheader("Watchlist")
    raw_watchlist = st.text_area(
        "One topic per line",
        value=DEFAULT_WATCHLIST,
        height=160,
        help="Articles whose titles contain these words get their own panel.",
    )

watchlist = [word.strip().lower() for word in raw_watchlist.splitlines() if word.strip()]


def watched(edit):
    """True if this edit is an article a person edited that we're watching."""
    if edit["wiki"] not in WIKIS or edit.get("namespace") != ARTICLE_NAMESPACE:
        return False
    if edit["bot"]:
        return False

    title = edit["title"].lower()
    return any(word in title for word in watchlist)

cat_column, data_column = st.columns([1, 2], gap="large")

with cat_column:
    gasping = (
        state.alert is not None
        and time.time() - state.alert["detected_at"] < GASP_SECONDS
    )
    cat = load_cat("cat_gasp.svg" if gasping else "cat_calm.svg")

    st.markdown(
        f"<div style='text-align:center'><img src='{cat}' width='220'></div>",
        unsafe_allow_html=True,
    )

    if gasping:
        st.error(f"**GOSSIP!**  {state.alert['page']}")
        st.caption(
            f"{state.alert['rate']:.0f} edits/min vs "
            f"{state.alert['average']:.0f} normally"
        )
    else:
        st.caption(
            "<div style='text-align:center'>nothing to report</div>",
            unsafe_allow_html=True,
        )

    seen = state.bots + state.humans
    st.metric("Edits seen", f"{seen:,}")

    if seen:
        human_share = state.humans / seen
        st.caption(f"Humans {human_share:.0%}  -  Bots {1 - human_share:.0%}")
        st.progress(human_share)

with data_column:
    if watchlist:
        st.subheader("On your watchlist")

        matches = list(islice((e for e in state.recent if watched(e)), WATCHLIST_MATCHES))
        if matches:
            for edit in matches:
                st.text(f"{edit['user']:<20} {edit['title']}")
        else:
            st.caption("Nothing matching yet.")

    st.subheader("Trending now")

    if state.leaderboard:
        table = pd.DataFrame(state.leaderboard)
        table.index = range(1, len(table) + 1)
        st.dataframe(
            table.rename(
                columns={"title": "Article", "editors": "Editors", "edits": "Edits"}
            ),
            use_container_width=True,
        )
    else:
        st.info("Waiting for the first leaderboard snapshot (every 30s).")

    st.subheader("Edits per minute")

    if state.edits_per_minute:
        minutes = sorted(state.edits_per_minute)
        chart = pd.DataFrame(
            {
                "minute": [
                    time.strftime("%H:%M", time.localtime(m * 60)) for m in minutes
                ],
                "edits": [state.edits_per_minute[m] for m in minutes],
            }
        ).set_index("minute")
        st.bar_chart(chart, height=220)
        st.caption("The last bar is the current minute, still filling up.")
    else:
        st.info("Waiting for edits. Is the producer running?")

    st.subheader("Latest edits")
    for edit in islice(state.recent, RECENT_EDITS):
        who = "bot" if edit["bot"] else "human"
        st.text(f"{who:<6} {edit['wiki']:<14} {edit['title']}")

time.sleep(REFRESH_SECONDS)
st.rerun()
