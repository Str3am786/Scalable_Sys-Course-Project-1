FROM python:3.13-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/

RUN pip install -e .



ENV REDIS_URL=redis\
    N_SHARDS=8\
    STREAM_PREFIX=trips:\
    HOT_END_STATIONS=7,8,9\
    MAX_BIKES=200000\
    MAX_TRIPS_PER_BIKE=2200000

CMD ["/bin/bash"]
# CMD ["python", "-m", "scalable_system.cli", "--help"]