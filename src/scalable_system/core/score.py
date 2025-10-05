import math, time
from datetime import datetime
from ..config import ONE_HOUR_MS, SCORE_WEIGHTS

# Hour-of-day weights. Flat for now, can be updated at runtime
HOUR_WEIGHTS = {h: 1.0 for h in range(24)}

def set_hour_weights(new_weights: dict):
    # expected: {"0":1.0, "1":1.1, ...}
    for k, v in new_weights.items():
        try:
            HOUR_WEIGHTS[int(k)] = float(v)
        except Exception:
            pass

def now_ms() -> int:
    return int(time.time() * 1000)

def compute_score(chain, now_ms_val: int | None = None) -> float:
    """
    Tiny, fast proxy for 'utility' of keeping this chain.
    Uses only in-memory attributes from the Chain object.
    """
    now = now_ms_val or now_ms()

    # progress: favor longer chains (cap length to 10)
    L = getattr(chain, "length_a", 1) or 1
    L = max(1, min(10, int(L)))
    progress = 1.0 - math.exp(-0.5 * L)  # rises quickly, saturates

    # time-left in 1h window (0..1)
    first_ts = getattr(chain, "first_ts_ms", now)
    last_ts  = getattr(chain, "last_ts_ms", now)
    elapsed  = max(0, last_ts - first_ts)
    time_left = max(0.0, 1.0 - (elapsed / ONE_HOUR_MS))

    # hour factor (simple boost)
    h = datetime.utcfromtimestamp((last_ts or now) / 1000).hour
    hfactor = HOUR_WEIGHTS.get(h, 1.0)

    w = SCORE_WEIGHTS
    score = w["w_len"]*progress + w["w_time"]*time_left + w["w_hour"]*hfactor
    return float(score)

def compute_score_dbg(chain, now_ms_val: int | None = None):
    import math
    from datetime import datetime
    now = now_ms_val or now_ms()
    L = getattr(chain, "length_a", 1) or 1
    Lc = max(1, min(10, int(L)))
    progress = 1.0 - math.exp(-0.5 * Lc)
    first_ts = getattr(chain, "first_ts_ms", now)
    last_ts  = getattr(chain, "last_ts_ms", now)
    elapsed  = max(0, last_ts - first_ts)
    time_left = max(0.0, 1.0 - (elapsed / ONE_HOUR_MS))
    h = datetime.utcfromtimestamp((last_ts or now) / 1000).hour
    hfactor = HOUR_WEIGHTS.get(h, 1.0)
    w = SCORE_WEIGHTS
    score = w["w_len"]*progress + w["w_time"]*time_left + w["w_hour"]*hfactor
    return float(score), {
        "progress": progress, "time_left": time_left, "hour": hfactor,
        "len": L, "hour_num": h
    }

