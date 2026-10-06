# One image for every Python part of Gossip. Railway runs it four times:
# the producer, two consumers and the dashboard. GOSSIP_SCRIPT picks which.

FROM python:3.13-slim

WORKDIR /app

# Print logs straight away instead of buffering them, so Railway shows them live.
ENV PYTHONUNBUFFERED=1

# Install packages before copying the code. Docker reuses this step on the next
# build as long as requirements.txt hasn't changed, which makes deploys faster.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# GOSSIP_SCRIPT set (e.g. producers/wiki_producer.py) -> run that script.
# Not set -> run the dashboard on the port Railway gives us in $PORT.
CMD ["sh", "-c", "if [ -n \"$GOSSIP_SCRIPT\" ]; then exec python \"$GOSSIP_SCRIPT\"; else exec streamlit run dashboard/app.py --server.port ${PORT:-8501} --server.address 0.0.0.0 --server.headless true; fi"]
