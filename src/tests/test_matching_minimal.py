# tests/test_matching_window.py
from pathlib import Path
import pytest
from src.scalable_system.core.matching import process_trip_for_bike
from src.scalable_system.core.lru import LRU
from src.scalable_system.core.models import Trip
from src.scalable_system.config import ONE_HOUR_MS
from src.scalable_system.workers.worker import get_match_stream


# ---------- test scaffolding ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
matches_path = PROJECT_ROOT / "matches" / "matches.txt"
_output_stream = get_match_stream(str(matches_path))


class DummyRedis:
    def __init__(self):
        self._kv = set()
        self._ttl = {}
        self.stream_events = []
        self.matches = []

    def setnx(self, key, value):
        if key in self._kv:
            return False
        self._kv.add(key)
        return True

    def pexpire(self, key, ttl_ms):
        self._ttl[key] = ttl_ms

    def xadd(self, stream, fields):
        self.stream_events.append((stream, fields))
        self.matches.append(fields)
        return "0-1"


@pytest.fixture
def r():
    return DummyRedis()

@pytest.fixture
def lru():
    return LRU(100)

@pytest.fixture
def state():
    return {}


# Helpers
BASE = 1_483_220_000_000  # arbitrary epoch (ms) to make times readable

def t_minutes(m):  # minutes → ms offset
    return m * 60 * 1000

def make_trip(bike, start_id, end_id, start_min, end_min, ingest=0):
    return Trip(
        bike_id=bike,
        start_station_id=start_id,
        end_station_id=end_id,
        started_at_ms=BASE + t_minutes(start_min),
        ended_at_ms=BASE + t_minutes(end_min),
        ingest_ts_ms=ingest,
    )


# ---------- tests ----------

def test_within_window_basic_emit(state, lru, r):
    """
    Two contiguous trips within 1h; second ends at a 'hot' station (…7/8/9).
    Expect exactly one match.
    """
    # a[1]: 00:08 → 00:15:59
    t1 = make_trip("B1", 100, 200, 8, 16)
    # b:     00:18 → 00:25:59 (contiguous: start=200), ends with ...327 (hot, %10==7)
    t2 = make_trip("B1", 200, 327, 18, 26)

    process_trip_for_bike(state, lru, "B1", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B1", t2, r, _output_stream)

    assert len(r.matches) == 1
    m = r.matches[0]
    # sanity on fields your emit_match likely writes (adjust if names differ)
    # assert m["bike_id"] == "B1"


def test_window_evict_left_preserve_chain(state, lru, r):
    """
    a[1] and a[2] exist; new b would exceed 1h if we keep a[1].
    We should pop a[1], keep a[2], and still emit a match using a[2]…b.
    """
    # a[1]: 00:00 → 00:10
    t1 = make_trip("B2", 10, 11, 0, 10)
    # a[2]: 00:12 → 00:20 (contiguous)
    t2 = make_trip("B2", 11, 12, 12, 20)
    # b:    01:15 → 01:18 (contiguous with a[2], end station hot ...339 -> %10==9)
    # Span if using a[1]: 75-0=75min > 60 → should evict a[1]
    # Span if using a[2]: 75-12=63min? still >60; make it 72-12=60 to be on the limit
    t3 = make_trip("B2", 12, 339, 72, 75)  # ended_at at 75, first_ts after evict is 12 → 63; too big.
    # Fix to ensure <= 60: end at 72 instead (ended_at=72; started 69)
    t3 = make_trip("B2", 12, 339, 69, 72)  # 72 - 12 = 60 → on the boundary

    process_trip_for_bike(state, lru, "B2", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B2", t2, r, _output_stream)
    process_trip_for_bike(state, lru, "B2", t3, r, _output_stream)

    assert len(r.matches) == 1, "Should emit using a[2] (after evicting a[1]) and b"
    # Also verify state kept continuity (optional)


def test_window_empty_then_restart(state, lru, r):
    """
    Incoming trip is so late that even after evicting all 'a' trips, the chain empties.
    We restart from this trip and DO NOT emit on this event.
    """
    # a[1]: 00:00 → 00:05
    t1 = make_trip("B3", 100, 200, 0, 5)
    # a[2]: 00:06 → 00:10
    t2 = make_trip("B3", 200, 300, 6, 10)
    # b:    02:30 → 02:35 (well beyond 1h from both a[1] and a[2])
    t3 = make_trip("B3", 300, 317, 150, 155)  # hot end ...317

    process_trip_for_bike(state, lru, "B3", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B3", t2, r, _output_stream)
    process_trip_for_bike(state, lru, "B3", t3, r, _output_stream)

    assert len(r.matches) == 0, "No match: chain empties then restarts from t3"


def test_continuity_break_resets_chain(state, lru, r):
    """
    If start station of incoming trip != last_end_station, continuity breaks and we restart.
    No emit on this event.
    """
    # a[1]: 00:00 → 00:10
    t1 = make_trip("B4", 10, 11, 0, 10)
    # break: next trip starts at 99 (not 11)
    t2 = make_trip("B4", 99, 107, 12, 20)  # hot end ...107

    process_trip_for_bike(state, lru, "B4", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B4", t2, r, _output_stream)

    assert len(r.matches) == 0, "No emit on continuity break; chain restarts at t2"


def test_out_of_order_is_dropped(state, lru, r):
    """
    If an event arrives with started_at < current last_ts, we drop it (per current policy).
    """
    # a[1]: 00:10 → 00:20
    t1 = make_trip("B5", 1, 2, 10, 20)
    # next arrives out-of-order: starts at 00:05 -> earlier than last_ts of chain (20)
    t2 = make_trip("B5", 2, 307, 5, 15)  # would have been hot, but should be dropped

    process_trip_for_bike(state, lru, "B5", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B5", t2, r, _output_stream)

    assert len(r.matches) == 0, "OOO event dropped, no matches"


def test_hot_end_filter(state, lru, r):
    """
    Same continuity and window, but end station not hot ⇒ no match.
    """
    t1 = make_trip("B6", 50, 60, 0, 10)
    t2 = make_trip("B6", 60, 1234, 12, 20)  # 1234 % 10 == 4, not in {7,8,9}

    process_trip_for_bike(state, lru, "B6", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B6", t2, r, _output_stream)

    assert len(r.matches) == 0, "End station not hot ⇒ no match"


def test_boundary_exactly_one_hour(state, lru, r):
    """
    Window boundary case: b.ended_at - a[1].started_at == ONE_HOUR_MS is allowed.
    """
    # a[1]: 00:00 → 00:10
    t1 = make_trip("B7", 1, 2, 0, 10)
    # a[2]: 00:12 → 00:20
    t2 = make_trip("B7", 2, 3, 12, 20)
    # b ends exactly at 60 minutes from a[1] start => allowed (<= 1h)
    # Keep continuity with a[2] (start=3). End is hot ...329 (%10==9)
    t3 = make_trip("B7", 3, 329, 58, 60)

    process_trip_for_bike(state, lru, "B7", t1, r, _output_stream)
    process_trip_for_bike(state, lru, "B7", t2, r, _output_stream)
    process_trip_for_bike(state, lru, "B7", t3, r, _output_stream)

    assert len(r.matches) == 1, "Boundary at exactly 1h should emit"
    # optional: verify no second phantom match
    assert len(r.matches) == 1
