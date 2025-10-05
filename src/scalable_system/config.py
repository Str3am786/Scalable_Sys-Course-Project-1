import os

def env(key: str, default: str = "") -> str:
    return os.getenv(key, default)

REDIS_URL        = env("REDIS_URL", "redis://redis:6379/0")
STREAM_PREFIX    = env("STREAM_PREFIX", "trips:")   
MATCHES_STREAM   = env("MATCHES_STREAM", "matches")
N_SHARDS         = int(env("N_SHARDS", "8"))
HOT_END_STATIONS = set(int(x) for x in env("HOT_END_STATIONS", "519,497,402").split(","))
ONE_HOUR_MS      = 60 * 60 * 1000
MAX_BIKES        = int(env("MAX_BIKES", "200000"))
MAX_TRIPS_PER_BIKE = int(env("MAX_TRIPS_PER_BIKE", "16"))
DEDUPE_TTL_MS    = int(env("DEDUPE_TTL_MS", f"{24*3600*1000}"))
