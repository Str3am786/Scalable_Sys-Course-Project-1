import json, os, fcntl
from typing import Dict
from ..io.redis_client import get_client, get_client_in_docker_net
from ..config import STREAM_PREFIX, N_SHARDS, MAX_BIKES, MAX_TRIPS_PER_BIKE
from ..core.sharding import stream_name
from ..core.lru import LRU
from ..core.models import Chain, Trip
from ..core.matching import process_trip_for_bike
import time
from datetime import datetime

def _b(fields, key):
    v = fields.get(key.encode())
    return v.decode() if v is not None else None

# TODO remove this fully
def get_match_stream(filename : str = "/app/matches/matches.txt"):
    return open(filename,"a", buffering=1)

THRESHOLD = 2
SERIES_PREFIX = "metrics:latency"


def ensure_series(r, key, labels):
    try:
        r.ts().create(key, retention_msecs=3600000, labels=labels, duplicate_policy='last')
        
    except Exception as e:
        # Already exists is fine; only re-raise real errors
        msg = str(e).lower()
        if "key already exists" not in msg and "already exists" not in msg:
            raise



def ts_key(shard_idx: int, bike_id: str):
    # one series per bike (per shard), easy to filter in RedisInsight
    return f"{SERIES_PREFIX}:{shard_idx}"

def run_worker(shard_idx: int, start_id: str = "0-0"):
    
    start = datetime.now()


    try: 
        r = get_client_in_docker_net()
        r_stats = get_client_in_docker_net("redisstats")
        if r.ping() & r_stats.ping():
            # print(r_stats.ts().create(f"metrics:{shard_idx}",retention_msecs=3600000,duplicate_policy='LAST'),labels=labels)
            print("Worker Connected to Redis")
    except Exception as e:
        raise ConnectionError(f"Not able to connect to Redis container: {e}")

    sname = stream_name(STREAM_PREFIX, shard_idx)
    state: Dict[str, Chain] = {}
    lru = LRU(MAX_BIKES)
    last_id = start_id
    # print(f"[worker {shard_idx}] reading {sname} from {start_id}")
    output_stream = get_match_stream()
    counter_slow = 0
    
    while True:
        # print(f"[worker {shard_idx}] waiting for events from {sname}")
        resp = r.xread({sname:last_id}, block=1000, count=1000)
        # print(resp)
        
        # Ingest events from redis stream
        if not resp: 
            print("NOTHINGGGG")
            end = datetime.now()
            print(f" TIME WORKER {shard_idx} ------------------------------------------------ {((end - start )/ 60)}----------------------------------------------------------------------------SLOW : {counter_slow}")
            time.sleep(10)
            continue
        
        _, entries = resp[0]
        for msg_id, fields in entries:
            
            bike_id = _b(fields, "bike_id")
            ss = _b(fields, "start_station_id"); es = _b(fields, "end_station_id")
            st = _b(fields, "started_at_ms");   et = _b(fields, "ended_at_ms")
            ig = _b(fields, "ingest_ts_ms")
            if not (bike_id and ss and es and st and et):
                last_id = msg_id; continue
            trip = Trip(
                bike_id=bike_id,
                start_station_id=int(ss),
                end_station_id=int(es),
                started_at_ms=int(st),
                ended_at_ms=int(et),
                ingest_ts_ms=int(ig or 0),
            )
            
            # Evaluate events --> Match system
            process_trip_for_bike(state, lru, bike_id, trip, r, output_stream)
            last_id = msg_id
            
                    
            # Write Stats in Timeseries for system analysis
            now_ms = time.time() * 1000
            latency = now_ms - trip.ingest_ts_ms
            
            # status = "SLOW" if latency > THRESHOLD else "OK"
            
            key = ts_key(shard_idx, bike_id)
            
            labels = {
            "shard": str(shard_idx),
            "bike_id": bike_id,
            "metric": "latency_ms",
            "component": "consumer",
            }


            # ensure series exists with correct labels
            try:
                r_stats.ts().add(key, "*", latency, duplicate_policy='last', labels=labels)
            except Exception:
                ensure_series(r_stats, key, labels)
                r_stats.ts().add(key, "*", latency, duplicate_policy='last', labels=labels)
                
            # status = "SLOW" if latency > THRESHOLD else "OK"
            now_ms = time.time() * 1000
            l = now_ms - trip.ingest_ts_ms
            
            if l > THRESHOLD:
                counter_slow += 1
                print(f"WORKER {shard_idx} --- L: {l}")
            
                
            
            
            
        
        
