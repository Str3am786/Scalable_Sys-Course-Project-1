# config.py
import os
from typing import Optional, Set

# --- helpers ---
def env(key: str, default: str = "") -> str:
    return os.getenv(key, default)

def _env_int(
    name: str,
    default: int,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Read an int from the environment with fallback and optional clamping."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        val = default
    else:
        try:
            val = int(raw)
        except ValueError:
            val = default
    if min_value is not None and val < min_value:
        val = min_value
    if max_value is not None and val > max_value:
        val = max_value
    return val

def _env_csv_ints(name: str, default: str) -> Set[int]:
    """Parse a comma-separated list of ints from env, ignoring bad tokens."""
    raw = os.getenv(name, default)
    out: Set[int] = set()
    for token in (t.strip() for t in raw.split(",")):
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            # ignore non-integer entries
            pass
    return out

# --- MONITOR ---
MONITOR_PREFIX = env("MONITOR_PREFIX", "metrics:latency")
PENDING_TH = _env_int("PENDING_TH", 3)
LATENCY_TH = _env_int(name= "LATENCY_TH", default=1)
RESPONSIVNESS = _env_int(name="RESPONSIVINESS",default=3)
# --- Output file for the writer ---
OUTFILE    = env("MATCHES_FILE", "/app/matches/matches.txt")

# --- REDIS ---
REDIS_HOST = env("REDIS_HOST", "redis")
REDIS_PORT = _env_int("REDIS_PORT", 6379, min_value=1)
REDIS_DB   = _env_int("REDIS_DB", 0, min_value=0)

BLOCK_MS   = _env_int("MATCH_WRITER_BLOCK_MS", 5000, min_value=0)
COUNT      = _env_int("MATCH_WRITER_READ_COUNT", 200, min_value=1)

# --- Core config ---
REDIS_URL          = env("REDIS_URL") or f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
STREAM_PREFIX      = env("STREAM_PREFIX", "trips:")
MATCHES_STREAM     = env("MATCHES_STREAM", "matches")
N_SHARDS           = _env_int("N_SHARDS", 10, min_value=1)
HOT_END_STATIONS   = _env_csv_ints("HOT_END_STATIONS", "7,8,9")
ONE_HOUR_MS        = 60 * 60 * 1000
MAX_BIKES          = _env_int("MAX_BIKES", 200_000, min_value=1)
MAX_TRIPS_PER_BIKE = _env_int("MAX_TRIPS_PER_BIKE", 16, min_value=1)
DEDUPE_TTL_MS      = _env_int("DEDUPE_TTL_MS", 24 * 3600 * 1000, min_value=0)
INPUT_CSV          = env("INPUT_CSV", default="/app/data/1_January/d.csv")

THRESHOLD: int = _env_int("THRESHOLD", 2, min_value=0)
SERIES_PREFIX: str = env(key="SERIES_PREFIX", default="metrics:latency").strip()

# --- Load shedding ---
SHEDDING_MECH: str = env("SHEDDING_MECH", "None").strip()

# Keep the last N trips at most when "Drop_oldest" is enabled (0 allowed)
SHED_KEEP_LAST_N: int = _env_int("SHED_KEEP_LAST_N", 1, min_value=0)

# Drop K trips from the left when "Drop_k" is enabled (0 allowed)
SHED_DROP_K: int = _env_int("SHED_DROP_K", 3, min_value=0)

# When "Shrink_window" is enabled, prune to this window in ms (defaults to half of ONE_HOUR_MS; 0 allowed)
SHED_WINDOW_MS: int = _env_int("SHED_WINDOW_MS", ONE_HOUR_MS // 2, min_value=0)



__all__ = [
    # helpers
    "env", "_env_int", "_env_csv_ints",
    # simple envs
    "OUTFILE", "REDIS_HOST", "REDIS_PORT", "REDIS_DB", "BLOCK_MS", "COUNT",
    # core
    "REDIS_URL", "STREAM_PREFIX", "MATCHES_STREAM", "N_SHARDS",
    "HOT_END_STATIONS", "ONE_HOUR_MS", "MAX_BIKES", "MAX_TRIPS_PER_BIKE",
    "DEDUPE_TTL_MS",
    # load shedding
    "SHEDDING_MECH", "SHED_KEEP_LAST_N", "SHED_DROP_K", "SHED_WINDOW_MS",
]
