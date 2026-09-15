# Gossip – Live Internet Chatter, Powered by Kafka

## About me (read this first)
- Beginner Python student. I know basics: reading CSVs, loops, mean/median/min/max in Jupyter.
- I'm on Windows, using VS Code, PowerShell terminal, and Docker Desktop.
- Goal: a portfolio project that proves I understand Apache Kafka.

## How I want you to work with me
- Go one phase at a time. Don't jump ahead or build later phases early.
- Explain what each file and each important line does in plain language.
- Keep code simple and readable over clever.
- Before writing a file, tell me what it is and why we need it.
- When something breaks, help me understand *why*, not just fix it.
- Remind me to commit to git at the end of each phase.

---

## Pitch
A real-time app that streams live activity from Wikipedia (and later Reddit) through Apache Kafka, detects what's trending, and shows it on a live dashboard. When there's a spike, a cartoon cat's face switches to a wide-open-mouth gasp.

Name bonus: "gossip protocol" is a real distributed-systems term (used by Cassandra, not Kafka) — a nod, not a claim.

## Tech stack
- Python 3 (virtual env in `.venv`)
- Apache Kafka 4.x in Docker (KRaft mode, no Zookeeper), image `apache/kafka`
- `confluent-kafka` Python client, `requests`
- Streamlit for the dashboard (Phases 4–6)
- Later (Phase 7): FastAPI + WebSockets + React frontend

Decision: Streamlit first because browsers can't talk to Kafka directly; React needs a backend bridge (FastAPI + WebSockets). Keep focus on Kafka first, upgrade the UI later.

## Architecture
```
Wikipedia EventStreams ──► wiki_producer.py ──┐
                                               ├──► Kafka topics ──► consumers ──► Gossip dashboard
Reddit API ────────────► reddit_producer.py ──┘
```
Producers only write to Kafka; consumers only read. Neither knows the other exists.

## Kafka topics
| Topic | Contents |
|---|---|
| `gossip.wiki.edits` | Raw Wikipedia edit events |
| `gossip.reddit.posts` | New posts from chosen subreddits (Phase 5) |
| `gossip.trending` | Pages/topics flagged as hot (e.g. 10+ edits in 5 min) |
| `gossip.alerts` | Spike alerts that trigger the gasping cat |

## Folder structure
```
gossip/
├── CLAUDE.md
├── docker-compose.yml
├── requirements.txt
├── .gitignore               # includes .venv/
├── test_kafka.py            # Phase 0 sanity check
├── producers/
│   ├── wiki_producer.py
│   └── reddit_producer.py
├── consumers/
│   ├── printer.py           # prints events (testing)
│   ├── trend_detector.py    # sliding-window counts → gossip.trending
│   └── stats.py             # edits/min, bot vs human, spikes → gossip.alerts
├── dashboard/
│   ├── app.py               # Streamlit UI
│   └── assets/
│       ├── cat_calm.png
│       └── cat_gasp.png
├── experiments/
│   └── notes.md             # results of breaking things on purpose
└── README.md
```

---

## Phases

### Phase 0 – Setup
- VS Code extensions: Python, Container Tools.
- Create `.venv`, activate (`.venv\Scripts\Activate.ps1`), `pip install confluent-kafka requests`.
  - If scripts are disabled: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.
- Select the `.venv` interpreter in VS Code.
- `docker-compose.yml`:
```yaml
services:
  kafka:
    image: apache/kafka:4.1.0
    container_name: gossip-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_NUM_PARTITIONS: 3
```
- Start: `docker compose up -d`. Stop: `docker compose down`.
- `test_kafka.py` sends "meow, kafka is alive" to `gossip.test` and reads it back.
- **Done when:** the test script prints `Sent!` and `Received: meow, kafka is alive`.

### Phase 1 – Wikipedia producer
- Connect to Wikimedia EventStreams (recent changes, server-sent events).
- Send a descriptive User-Agent header (Wikimedia requires it).
- Keep fields: page title, wiki, user, bot flag, timestamp.
- Produce JSON to `gossip.wiki.edits`.
- **Done when:** edits flow into the topic nonstop.

### Phase 2 – Consumers
- `printer.py`: print each event.
- `trend_detector.py`: count edits per page over a sliding 5-minute window; publish hot pages to `gossip.trending`.
- `stats.py`: edits per minute, bot vs human split.
- **Done when:** terminal shows "🔥 Hot right now: [page]".

### Phase 3 – Break it on purpose (most important)
Record results in `experiments/notes.md`:
1. Stop a consumer for 2 minutes, restart. Does it catch up?
2. Run `printer.py` and `stats.py` together (different groups). Interference?
3. Two copies of the same consumer in one group. How is work split?
4. Topic with 3 partitions, repeat #3.
5. Stop Kafka while the producer runs, then restart Kafka.

### Phase 4 – Streamlit dashboard + the cat
- Live "trending now" list, edits-per-minute chart, bot vs human split, scrolling latest edits.
- **Spike logic** (in `stats.py`): keep a running average of edits/min. If the current minute is > 2× the average (or > mean + 2 std dev), publish to `gossip.alerts` with the page that caused it.
- **Cat:** dashboard consumes `gossip.alerts`; shows `cat_gasp.png` (mouth wide open) for a few seconds plus the page name, otherwise `cat_calm.png`. Cat images must be original drawings (no copyrighted characters).
- **Done when:** dashboard updates live in the browser and the cat gasps on a spike.

### Phase 5 – Reddit as second source
- Register a Reddit app for API credentials; poll chosen subreddits (check current API terms/rate limits).
- Alternative if Reddit is a hassle: Bluesky Jetstream (free, push-based).
- Produce to `gossip.reddit.posts`; show both sources on the dashboard.
- Talking point: Wikipedia pushes, Reddit is polled — Kafka absorbs the difference.

### Phase 6 – Polish
- README: diagram, setup steps, dashboard GIF, experiment findings, "what I'd do next" (Spark, Airflow).

### Phase 7 – React upgrade (optional)
- FastAPI backend consumes Kafka and pushes updates over WebSockets.
- React frontend with an animated cat on spikes.

---

## Resume line
> **Gossip** – Real-time social trend tracker. Built a streaming pipeline in Python using Apache Kafka (Docker) to ingest live Wikipedia and Reddit activity, detect trending topics with sliding-window counts, and display them in a live Streamlit dashboard with spike alerts.

## Current status
- [x] Phase 0 – Setup
- [ ] Phase 1 – Wikipedia producer
- [ ] Phase 2 – Consumers
- [ ] Phase 3 – Experiments
- [ ] Phase 4 – Dashboard + cat
- [ ] Phase 5 – Reddit
- [ ] Phase 6 – Polish
- [ ] Phase 7 – React (optional)
