import csv, time
from dateutil import parser as dtparse
from datetime import timezone
from typing import Optional
from ..io.redis_client import get_client
from ..config import N_SHARDS, STREAM_PREFIX
from ..core.sharding import shard_for_bike, stream_name

def now_ms(): return int(time.time()*1000)

def parse_ts_ms(s: str) -> int:
    dt = dtparse.parse(s)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def to_int_safe(v): 
    try: return int(v)
    except: return None

def produce_csv(path: str, mapping: dict, n_shards: Optional[int] = None, max_rows: Optional[int] = None):
    r = get_client()
    n = 0
    nsh = n_shards or N_SHARDS
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bike_id = row.get(mapping["bike_id"])
            if not bike_id: continue
            start_sid = to_int_safe(row.get(mapping["start_station_id"]))
            end_sid   = to_int_safe(row.get(mapping["end_station_id"]))
            if start_sid is None or end_sid is None: continue
            started_ms = parse_ts_ms(row.get(mapping["started_at"]))
            ended_ms   = parse_ts_ms(row.get(mapping["ended_at"]))
            shard = shard_for_bike(bike_id, nsh)
            sname = stream_name(STREAM_PREFIX, shard)
            r.xadd(sname, {
                b"bike_id": bike_id.encode(),
                b"start_station_id": str(start_sid).encode(),
                b"end_station_id": str(end_sid).encode(),
                b"started_at_ms": str(started_ms).encode(),
                b"ended_at_ms": str(ended_ms).encode(),
                b"ingest_ts_ms": str(now_ms()).encode(),
            })
            n += 1
            if max_rows and n >= max_rows: break
    print(f"Produced {n} events into {nsh} shards.")
