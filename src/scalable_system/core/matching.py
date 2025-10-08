from typing import Dict
from .models import Chain, Trip
from .lru import LRU
from ..config import ONE_HOUR_MS, HOT_END_STATIONS
from ..io.matches_sink import emit_match


def process_trip_for_bike(state: Dict[str, Chain], lru: LRU,
                          bike_id: str, trip: Trip, r, output_stream ):
    evicted = lru.touch(bike_id)
    if evicted: state.pop(evicted, None)
    ch = state.get(bike_id)

    # start new chain if none
    if ch is None:
        ch = Chain(trip.start_station_id, trip.started_at_ms,
                   trip.end_station_id, trip.ended_at_ms)
        ch.length_a = 1
        ch.trips.append((trip.start_station_id, trip.end_station_id, trip.ended_at_ms))
        state[bike_id] = ch
        return

    # window reset if span would exceed 1h with this trip as b
    if ch.first_ts_ms < trip.ended_at_ms - ONE_HOUR_MS:
        ch = Chain(trip.start_station_id, trip.started_at_ms,
                   trip.end_station_id, trip.ended_at_ms)
        ch.length_a = 1

        ch.trips.clear(); ch.trips.append((trip.start_station_id, trip.end_station_id, trip.ended_at_ms))
        state[bike_id] = ch
        return

    # continuity a[i+1].start == a[i].end
    if trip.start_station_id == ch.last_end_station:
        # candidate b
        if (int(trip.end_station_id%10) in HOT_END_STATIONS and
            (trip.ended_at_ms - ch.first_ts_ms) <= ONE_HOUR_MS and
            ch.length_a >= 1):
            emit_match(r, bike_id, ch.first_start_station, ch.last_end_station,
                       trip.end_station_id, ch.first_ts_ms, trip.ended_at_ms, ch.length_a, trip.ingest_ts_ms, output_stream)
        # extend chain
        ch.last_end_station = trip.end_station_id
        ch.last_ts_ms = trip.ended_at_ms
        ch.length_a += 1
        ch.trips.append((trip.start_station_id, trip.end_station_id, trip.ended_at_ms))
    else:
        # break in continuity
        ch = Chain(trip.start_station_id, trip.started_at_ms,
                   trip.end_station_id, trip.ended_at_ms)
        ch.trips.clear(); ch.trips.append((trip.start_station_id, trip.end_station_id, trip.ended_at_ms))
        state[bike_id] = ch
