from typing import Dict
from .models import Chain, Trip
from .lru import LRU
from ..config import ONE_HOUR_MS, HOT_END_STATIONS, SHEDDING_MECH, MATCHES_STREAM, SHED_WINDOW_MS, SHED_DROP_K, \
    SHED_KEEP_LAST_N
from ..io.matches_sink import emit_match
from collections import deque

def _init_chain_from_trip(t: Trip) -> Chain:
    ch = Chain(
        first_start_station=t.start_station_id,
        first_ts_ms=t.started_at_ms,
        last_end_station=t.end_station_id,
        last_ts_ms=t.ended_at_ms,
        length_a=1,
    )
    ch.trips.append((t.start_station_id, t.end_station_id, t.started_at_ms, t.ended_at_ms))
    return ch


def _prune_chain_in_place(ch: Chain, b_ended_ms: int, window_ms: int) -> bool:
    """
    Pop from left until (b_ended_ms - ch.first_ts_ms) <= window_ms.
    Returns True if chain still has at least one trip; False if emptied.
    """
    while ch.trips and ch.first_ts_ms < b_ended_ms - window_ms:
        ch.trips.popleft()
        if ch.trips:
            ns, ne, ns_ms, ne_ms = ch.trips[0]
            ch.first_start_station = ns
            ch.first_ts_ms = ns_ms
            ch.length_a = len(ch.trips)
        else:
            return False
    return True

def _recalc_chain_meta(ch: Chain) -> None:
    """
    Bring Chain's derived fields back in sync with ch.trips.
    Safe to call after any in-place mutation of ch.trips.
    """
    if not ch.trips:
        # Leave first/last as-is; length becomes 0 (downstream guards handle empties)
        ch.length_a = 0
        return

    # First trip
    fs, fe, fs_ms, fe_ms = ch.trips[0]
    ch.first_start_station = fs
    ch.first_ts_ms = fs_ms

    # Last trip
    ls, le, ls_ms, le_ms = ch.trips[-1]
    ch.last_end_station = le
    ch.last_ts_ms = le_ms

    ch.length_a = len(ch.trips)

def shedding_mech(chain: Chain) -> None:
    """
    Rudimentary load shedding. Mutates the chain in-place to reduce
    memory/CPU under pressure. Strategy is selected by SHEDDING_MECH:

      - "None"            : do nothing
      - "Drop_oldest"     : cap chain to last N trips (env SHED_KEEP_LAST_N, default 1)
      - "Drop_k"          : drop K trips from the left (env SHED_DROP_K, default 3)
      - "Shrink_window"   : prune to a smaller time window (env SHED_WINDOW_MS, default ONE_HOUR_MS/2)

    All strategies keep the chain consistent with your existing logic.
    """
    mech = (SHEDDING_MECH or "None").strip().lower()
    print(mech)

    if mech == "none":
        return

    if mech == "drop_oldest":
        # Keep only the last N trips; fast, deterministic, very cheap
        n = max(0, SHED_KEEP_LAST_N)
        print("IN dropping")
        if n == 0:
            # Keep exactly the very last trip (avoids degenerate empty chain issues)
            if chain.trips:
                last = chain.trips[-1]
                chain.trips = deque([last])
        else:
            # Pop from the left until length <= n
            while len(chain.trips) > n:
                chain.trips.popleft()
        _recalc_chain_meta(chain)
        return

    if mech == "drop_k":
        # Drop a fixed number of oldest trips (once per call)
        k = max(0, SHED_DROP_K)
        for _ in range(min(k, len(chain.trips))):
            print("DROPPED CHAIN:", chain.trips.popleft())

        if not chain.trips and k > 0:
            # Preserve the latest element if we nuked everything
            # (this is safer for downstream continuity checks)
            # NOTE: If you really want to allow empty chains, remove this block.
            # Clear meta so downstream checks don't see stale continuity
            chain.first_start_station = -1
            chain.first_ts_ms = 0
            chain.last_end_station = -1
            chain.last_ts_ms = 0
            chain.length_a = 0
            return
        _recalc_chain_meta(chain)
        return

    if mech == "shrink_window":
        # Reuse your existing pruning but with a tighter window
        # Use the chain's current last_ts as the reference right edge
        window = max(0, SHED_WINDOW_MS)
        # If window == 0, keep just the last trip
        if window == 0:
            if chain.trips:
                last = chain.trips[-1]
                chain.trips = deque([last])
            _recalc_chain_meta(chain)
            return

        # Pop from left while outside the tighter window
        while chain.trips and (chain.trips[0][2] < chain.last_ts_ms - window):
            chain.trips.popleft()
        if not chain.trips:
            # Keep the most recent to avoid empty chain edge cases
            # (If you prefer empties, drop this block.)
            pass
        _recalc_chain_meta(chain)
        return

    # Unknown strategy -> no-op (fail safe)
    return

def process_trip_for_bike(state: Dict[str, Chain], lru: LRU,
                          bike_id: str, trip: Trip, r, output_stream, shedding_status:bool):
    # LRU eviction
    evicted = lru.touch(bike_id)
    if evicted:
        state.pop(evicted, None)

    ch = state.get(bike_id)

    # First time we see this bike
    if ch is None:
        state[bike_id] = _init_chain_from_trip(trip)
        return

    if shedding_status:
        shedding_mech(ch)
        state[bike_id] = _init_chain_from_trip(trip)
        return

    # Drop out-of-order (or buffer if you later add a reorder heap)
    if trip.started_at_ms < ch.last_ts_ms:
        return

    # Enforce 1h window by pruning from the left. If empty, restart.
    if not _prune_chain_in_place(ch, trip.ended_at_ms, ONE_HOUR_MS):
        state[bike_id] = _init_chain_from_trip(trip)
        return

    # Continuity: a[i+1].start == a[i].end; otherwise restart from this trip
    if trip.start_station_id != ch.last_end_station:
        state[bike_id] = _init_chain_from_trip(trip)
        return

    # Candidate b
    if (trip.end_station_id % 10) in HOT_END_STATIONS \
       and (trip.ended_at_ms - ch.first_ts_ms) <= ONE_HOUR_MS \
       and ch.length_a >= 1:
        n = len(ch.trips)
        a_last_end = ch.last_end_station
        for i in range(n):
            a1_start, _, a1_started_ms, _ = ch.trips[i]
            emit_match(
                r, bike_id,
                a1_start,             # a[1].start of this suffix
                a_last_end,           # a[i].end (end of last A)
                trip.end_station_id,  # b.end
                a1_started_ms,        # a[1].started_at
                trip.ended_at_ms,     # b.ended_at
                n - i,                # |a| = length of this suffix
                trip.ingest_ts_ms,
                output_stream
            )

    # Extend chain (this trip becomes new rightmost a[i])
    ch.trips.append((trip.start_station_id, trip.end_station_id, trip.started_at_ms, trip.ended_at_ms))
    ch.last_end_station = trip.end_station_id
    ch.last_ts_ms = trip.ended_at_ms
    ch.length_a = len(ch.trips)
