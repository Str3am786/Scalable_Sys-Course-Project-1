# src/writer.py
import os
import json
import redis

from scalable_system.config import MATCHES_STREAM

OUTFILE    = os.getenv("MATCHES_FILE", "/app/matches/matches.txt")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))
BLOCK_MS   = int(os.getenv("MATCH_WRITER_BLOCK_MS", "5000"))
COUNT      = int(os.getenv("MATCH_WRITER_READ_COUNT", "200"))

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    with open(OUTFILE, "a", buffering=1) as f:
        print(f"[writer] Writing JSONL to {OUTFILE}", flush=True)

        # 1) Backfill ALL existing entries once
        start = "-"
        while True:
            rows = r.xrange(MATCHES_STREAM, min=start, max="+", count=COUNT)
            if not rows:
                break
            last_id = None
            for msg_id, fields in rows:
                f.write(json.dumps(fields, separators=(",", ":")) + "\n")
                last_id = msg_id
            f.flush(); os.fsync(f.fileno())
            # next page starts after last_id
            if last_id is None:
                break
            start = f"({last_id}"  # exclusive

        # 2) Tail new entries forever
        last_id = "$"  # only new items from now on
        while True:
            resp = r.xread({MATCHES_STREAM: last_id}, block=BLOCK_MS, count=COUNT)
            if not resp:
                continue
            _, messages = resp[0]
            for msg_id, fields in messages:
                f.write(json.dumps(fields, separators=(",", ":")) + "\n")
                last_id = msg_id
            f.flush()  # fsync optional; add if you want durability per batch

if __name__ == "__main__":
    import os
    main()
