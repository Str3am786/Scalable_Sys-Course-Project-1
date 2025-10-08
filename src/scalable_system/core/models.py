from dataclasses import dataclass, field
from collections import deque
from typing import Deque, Tuple

@dataclass
class Trip:
    bike_id: str
    start_station_id: int
    end_station_id: int
    started_at_ms: int
    ended_at_ms: int
    ingest_ts_ms: int

@dataclass
class Chain:
    first_start_station: int
    first_ts_ms: int
    last_end_station: int
    last_ts_ms: int
    length_a: int = 1
    trips: Deque[Tuple[int, int, int, int]] = field(
        default_factory=lambda: deque(maxlen=64)
    )

