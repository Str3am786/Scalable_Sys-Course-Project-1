import csv, time
from dateutil import parser as dtparse
from datetime import timezone
from typing import Optional
from ..io.redis_client import get_client
from ..config import N_SHARDS, STREAM_PREFIX
from ..core.sharding import shard_for_bike, stream_name
from pathlib import Path
import os
from typing import List
from glob import glob

def now_ms(): return int(time.time()*1000)

def parse_ts_ms(s: str) -> int:
    dt = dtparse.parse(s)
    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def to_int_safe(v): 
    try: return int(v)
    except: return None

def get_input_files(dir: str) -> List[str]:
    files = []
    for d in sorted(os.listdir(path=dir)):
        full = Path(dir, d)
        if not full.is_dir():
            continue
        # add ALL csvs (sorted for determinism)
        for f in sorted(glob(str(full / "*.csv"))):
            files.append(f)
    return files

    
    
def produce_csv(dir_path : str, n_shards: Optional[int] = None, max_rows: Optional[int] = None):
    
    mapping = {
        "bike_id": "Bike ID",
        "start_station_id": "Start Station ID",
        "end_station_id": "End Station ID",
        "started_at": "Start Time",
        "ended_at" :  "Stop Time"
        
    }

    i_files = get_input_files(dir_path)
    r = get_client()

    for _ in range(30):  # ~30s max
        try:
            r.ping()
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Redis not reachable")

    print(r.ping())
    n = 0
    nsh = n_shards or N_SHARDS
    for file in i_files:
        print(f"[ingester] reading {file}")
        with open(file, newline="") as f:
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
                
                res = r.xadd(sname, {
                    b"bike_id": bike_id.encode(),
                    b"start_station_id": str(start_sid).encode(),
                    b"end_station_id": str(end_sid).encode(),
                    b"started_at_ms": str(started_ms).encode(),
                    b"ended_at_ms": str(ended_ms).encode(),
                    b"ingest_ts_ms": str(int(time.time()*1000)).encode(),
                })
                                
                n += 1

                if n % 100000 == 0:
                    print(f"[ingester] produced {n} events so far")

                if max_rows and n >= max_rows: break
            
    print(f"Produced {n} events into {nsh} shards.")


if __name__ == "__main__":
    import sys, os
    base = sys.argv[1] if len(sys.argv) > 1 else os.getenv("DATA_DIR", "/data")
    # Rows can be capped for quick tests with produce_csv(base, max_rows=5000)
    produce_csv(base)
