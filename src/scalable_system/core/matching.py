from typing import Dict
from .models import Chain, Trip
from .lru import LRU
from ..config import ONE_HOUR_MS, HOT_END_STATIONS
from ..io.matches_sink import emit_match


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

def shedding_mech():
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
        emit_match(
            r, bike_id,
            ch.first_start_station,      # a[1].start
            ch.last_end_station,         # a[i].end
            trip.end_station_id,         # b.end
            ch.first_ts_ms,              # a[1].started_at
            trip.ended_at_ms,            # b.ended_at
            ch.length_a,                 # |a|
            trip.ingest_ts_ms,
            output_stream
        )

    # Extend chain (this trip becomes new rightmost a[i])
    ch.trips.append((trip.start_station_id, trip.end_station_id, trip.started_at_ms, trip.ended_at_ms))
    ch.last_end_station = trip.end_station_id
    ch.last_ts_ms = trip.ended_at_ms
    ch.length_a = len(ch.trips)
