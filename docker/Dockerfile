FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates && rm -rf /var/lib/apt/lists/*


WORKDIR /app
COPY pyproject.toml .
RUN pip install .

COPY src/ src/

ENV REDIS_URL= \
    N_SHARDS=\
    STREAM_PREFIX=trips:s\
    HOT_END_STATIONS=7,8,9\
    MAX_BIKES=200000\
    MAX_TRIPS_PER_BIKE=2200000

# CMD ["python", "-m", "scalable_system.cli", "--help"]