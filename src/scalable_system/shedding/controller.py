import time
from ..io.redis_client import get_client
from ..config import METRICS_STREAM, CONTROL_STREAM, LATENCY_SLA_MS
from ..io.redis_client import wait_for_redis

def _b(fields, key):
    v = fields.get(key.encode()); 
    return v.decode() if v is not None else None

def run_controller():
    r = get_client()
    wait_for_redis(r)
    last = "0-0"

    HOT_TICKS = 3     # consecutive hot samples to trigger
    T_SCORE   = 0.35  # score threshold for DROP_BELOW
    K_DROP    = 5000  # if still hot, drop worst K
    hot_counts = {}   # shard -> consecutive hot ticks

    print("[shedding] controller running…")
    while True:
        resp = r.xread({METRICS_STREAM: last}, block=1000, count=200)
        if not resp:
            continue
        _, entries = resp[0]
        for msg_id, fields in entries:
            shard = int(_b(fields, "shard") or -1)
            over  = (_b(fields, "over_sla") == "1")
            if shard >= 0:
                c = hot_counts.get(shard, 0)
                if over:
                    c += 1
                    if c == HOT_TICKS:
                        print(f"[controller] shard={shard} over SLA for {HOT_TICKS} ticks → DROP_BELOW {T_SCORE}")
                        r.xadd(CONTROL_STREAM, {b"cmd": b"DROP_BELOW", b"arg": str(T_SCORE).encode()})
                    elif c > HOT_TICKS:
                        print(f"[controller] shard={shard} still hot → DROP_K {K_DROP}")
                        r.xadd(CONTROL_STREAM, {b"cmd": b"DROP_K", b"arg": str(K_DROP).encode()})
                else:
                    c = 0
                hot_counts[shard] = c
            last = msg_id

if __name__ == "__main__":
    run_controller()
