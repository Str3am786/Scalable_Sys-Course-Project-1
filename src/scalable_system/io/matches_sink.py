import time
from ..config import MATCHES_STREAM, DEDUPE_TTL_MS


def now_ms() -> int: return int(time.time()*1000)

def dedupe_key(bike_id: str, first_ts_ms: int, b_ended_ms: int, b_end: int)->str:
    return f"match:dedupe:{bike_id}:{first_ts_ms}:{b_ended_ms}:{b_end}"

def emit_match(r, bike_id: str, a1_start: int, ai_end: int,
               b_end: int, first_ts_ms: int, b_ended_ms: int, length_a: int, ingest_ts_ms: int):
    dk = dedupe_key(bike_id, first_ts_ms, b_ended_ms, b_end)
    if r.setnx(dk, b"1"):
        r.pexpire(dk, DEDUPE_TTL_MS)
        r.xadd(MATCHES_STREAM,
               {
                   b"bike_id": bike_id.encode(),
                   b"a1_start": str(a1_start).encode(),
                   b"ai_end": str(ai_end).encode(),
                   b"b_end": str(b_end).encode(),
                   b"first_ts_ms": str(first_ts_ms).encode(),
                   b"b_ended_ms": str(b_ended_ms).encode(),
                   b"length_a": str(length_a).encode(),
                   b"ingest_ts_ms": str(ingest_ts_ms).encode(),
                   b"emit_ts_ms": str(now_ms()).encode(),
               })