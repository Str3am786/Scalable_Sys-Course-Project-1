from pathlib import Path
from src.scalable_system.core.matching import process_trip_for_bike
from src.scalable_system.core.lru import LRU
from src.scalable_system.core.models import Chain, Trip
from src.scalable_system.config import ONE_HOUR_MS

from src.scalable_system.workers.worker import get_match_stream


# tests/utils.py or inline in the test file
# Go from .../src/tests/your_test.py -> project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
matches_path = PROJECT_ROOT / "matches" / "matches.txt"
_output_stream = get_match_stream(str(matches_path))

class DummyRedis:
    def __init__(self):
        # simulate a tiny KV store used for dedupe
        self._kv = set()           # store keys that were set via setnx
        self._ttl = {}             # track TTLs if you care (optional)
        # capture emitted matches if emit_match calls xadd
        self.stream_events = []    # (stream, fields) tuples
        # convenience: high-level list of matches recorded by our fake xadd
        self.matches = []

    # Redis-like methods used by emit_match

    def setnx(self, key, value):
        # behave like Redis: set only if not exists; return True if set, False otherwise
        if key in self._kv:
            return False
        self._kv.add(key)
        return True

    def pexpire(self, key, ttl_ms):
        # optional; no-op is fine for tests
        self._ttl[key] = ttl_ms

    def xadd(self, stream, fields):
        # capture written events (if your emit_match uses xadd)
        self.stream_events.append((stream, fields))
        # optional: record a simple tuple to assert on later
        # adjust field names if your emit_match uses different keys
        self.matches.append(fields)
        return "0-1"  # dummy stream ID


def test_bike_19728_two_hops_hot_b():
    state = {}
    lru = LRU(100)
    r = DummyRedis()

    # 00:08:00 -> 00:15:59
    t1 = Trip("19728", 474, 3259, 1483222080000, 1483222559000, 0)
    # 00:18:00 -> 00:25:59
    t2 = Trip("19728", 3259, 3258, 1483222680000, 1483223159000, 0)

    t3 = Trip("19728", 3258, 3250, 1483223159000, 1483223169000, 0)





    process_trip_for_bike(state, lru, "19728", t1, r, output_stream=_output_stream)
    process_trip_for_bike(state, lru, "19728", t2, r, output_stream=_output_stream)
    process_trip_for_bike(state, lru, "19728", t3, r, output_stream=_output_stream)

    # assert one match recorded
    assert len(r.matches) == 1
    m = r.matches[0]
    # optionally, assert key fields your emit_match writes
    # e.g., assert m["bike_id"] == "19728"
