# config.py
import os
from typing import Optional, Set

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

# --- Core config ---
REDIS_URL           = env("REDIS_URL", "redis://redis:6379/0")
STREAM_PREFIX       = env("STREAM_PREFIX", "trips:")
MATCHES_STREAM      = env("MATCHES_STREAM", "matches")
N_SHARDS            = _env_int("N_SHARDS", 10, min_value=1)
HOT_END_STATIONS    = _env_csv_ints("HOT_END_STATIONS", "7,8,9")
ONE_HOUR_MS         = 60 * 60 * 1000
MAX_BIKES           = _env_int("MAX_BIKES", 200_000, min_value=1)
MAX_TRIPS_PER_BIKE  = _env_int("MAX_TRIPS_PER_BIKE", 16, min_value=1)
DEDUPE_TTL_MS       = _env_int("DEDUPE_TTL_MS", 24 * 3600 * 1000, min_value=0)

# --- Load shedding ---
SHEDDING_MECH: str  = os.getenv("SHEDDING_MECH", "None").strip()

# Keep the last N trips at most when "Drop_oldest" is enabled (0 allowed)
SHED_KEEP_LAST_N: int = _env_int("SHED_KEEP_LAST_N", 1, min_value=0)

# Drop K trips from the left when "Drop_k" is enabled (0 allowed)
SHED_DROP_K: int      = _env_int("SHED_DROP_K", 3, min_value=0)

# When "Shrink_window" is enabled, prune to this window in ms (defaults to half of ONE_HOUR_MS; 0 allowed)
SHED_WINDOW_MS: int   = _env_int("SHED_WINDOW_MS", ONE_HOUR_MS // 2, min_value=0)
