import os, json

def env(key: str, default: str = "") -> str:
    return os.getenv(key, default)

# Streams & shards
REDIS_URL        = env("REDIS_URL", "redis://redis:6379/0")
STREAM_PREFIX    = env("STREAM_PREFIX", "trips:s")   
MATCHES_STREAM   = env("MATCHES_STREAM", "matches")
N_SHARDS         = int(env("N_SHARDS", "8"))

# Monitored end stations
HOT_END_STATIONS = set(int(x) for x in env("HOT_END_STATIONS", "519,497,402").split(","))

# Pattern/window config
ONE_HOUR_MS      = 60 * 60 * 1000
MAX_BIKES        = int(env("MAX_BIKES", "200000"))
MAX_TRIPS_PER_BIKE = int(env("MAX_TRIPS_PER_BIKE", "16"))
DEDUPE_TTL_MS    = int(env("DEDUPE_TTL_MS", f"{24*3600*1000}"))

# Monitoring + shedding
CONTROL_STREAM   = env("CONTROL_STREAM", "control:cmds")
METRICS_STREAM   = env("METRICS_STREAM", "metrics:workers")
LATENCY_SLA_MS   = int(env("LATENCY_SLA_MS", "3"))  # avg processing latency target
BACKLOG_SLA      = int(env("BACKLOG_SLA", "200"))    # shard backlog trigger (not used currently)

# Simple score weights
SCORE_WEIGHTS    = json.loads(env("SCORE_WEIGHTS", '{"w_len":0.6,"w_time":0.3,"w_hour":0.1}'))

# Observability
LOG_EVERY_MATCH_N      = int(env("LOG_EVERY_MATCH_N", "1000"))   # 0 disables
LOG_SCORE_EVERY_N      = int(env("LOG_SCORE_EVERY_N", "10000"))  # 0 disables
INCLUDE_SCORE_METRICS  = env("INCLUDE_SCORE_METRICS", "1") == "1"

# optional stream for shedding events
SHEDDING_EVENTS_STREAM = env("SHEDDING_EVENTS_STREAM", "shedding:events")

def dump_config(prefix="[config]"):
    keys = [
        "REDIS_URL","STREAM_PREFIX","MATCHES_STREAM","N_SHARDS",
        "HOT_END_STATIONS","MAX_BIKES","MAX_TRIPS_PER_BIKE","DEDUPE_TTL_MS",
        "CONTROL_STREAM","METRICS_STREAM","LATENCY_SLA_MS","BACKLOG_SLA",
        "LOG_EVERY_MATCH_N","LOG_SCORE_EVERY_N","INCLUDE_SCORE_METRICS",
        "SHEDDING_EVENTS_STREAM"
    ]
    parts = []
    for k in keys:
        v = globals()[k]
        src = "ENV" if k in os.environ else "DEFAULT"
        parts.append(f"{k}={v} ({src})")
    print(prefix, " | ".join(parts))