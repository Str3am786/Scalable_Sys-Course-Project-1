FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy project first, then install
COPY pyproject.toml .
COPY src/ src/

# Install the package
RUN pip install .

# Role-based entrypoint:
#   ROLE=ingester  -> run the CSV producer
#   ROLE=workers   -> run all shard workers
#   ROLE=shedding  -> run the shedding controller
# Extra args can be passed via CMD/compose if needed.
CMD ["/bin/sh", "-lc", "\
  if [ \"$ROLE\" = \"ingester\" ]; then \
      python -m scalable_system.ingestion.input_reader /data; \
  elif [ \"$ROLE\" = \"workers\" ]; then \
      python -c 'from scalable_system.workers.supervisor import run_all_shards; run_all_shards(\"0-0\")'; \
  elif [ \"$ROLE\" = \"shedding\" ]; then \
      python -m scalable_system.shedding.controller; \
  else \
      echo 'Set ROLE=ingester|workers|shedding'; exit 1; \
  fi \
"]
