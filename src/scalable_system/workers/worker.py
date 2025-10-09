import json, os, fcntl
from typing import Dict
from ..io.redis_client import get_client, get_client_in_docker_net
from ..config import STREAM_PREFIX, N_SHARDS, MAX_BIKES, MAX_TRIPS_PER_BIKE
from ..core.sharding import stream_name
from ..core.lru import LRU
from ..core.models import Chain, Trip
from ..core.matching import process_trip_for_bike
import time

def _b(fields, key):
    v = fields.get(key.encode())
    return v.decode() if v is not None else None

# TODO remove this fully
def get_match_stream(filename : str = "/app/matches/matches.txt"):
    return open(filename,"a", buffering=1)

THRESHOLD = 2


def run_worker(shard_idx: int, start_id: str = "0-0"):
    try: 
        r = get_client_in_docker_net()
        r_stats = get_client_in_docker_net("redisstats")
        if r.ping() & r_stats.ping():
            
            print(r_stats.ts().create(f"metrics:{shard_idx}",retention_msecs=3600000,duplicate_policy='LAST'))
            
            print("Worker Connected to Redis")
    except Exception as e:
        raise ConnectionError(f"Not able to connect to Redis container: {e}")

    sname = stream_name(STREAM_PREFIX, shard_idx)
    state: Dict[str, Chain] = {}
    lru = LRU(MAX_BIKES)
    last_id = start_id
    # print(f"[worker {shard_idx}] reading {sname} from {start_id}")
    output_stream = get_match_stream()
    while True:
        # print(f"[worker {shard_idx}] waiting for events from {sname}")
        resp = r.xread({sname:last_id}, block=1000, count=1000)
        # print(resp)
        if not resp: continue
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
            process_trip_for_bike(state, lru, bike_id, trip, r, output_stream)
            last_id = msg_id
            
            now_ms = int(time.time() * 1000)
            latency = now_ms - trip.ingest_ts_ms
            
            status = "OK"
            if latency<THRESHOLD:
                status = "SLOW"
            
            r_stats.ts().add(f"metrics:{shard_idx}:{bike_id},", "*" , latency, labels={"bike_id":bike_id,"status":status},duplicate_policy='LAST')

            
            
        
        
