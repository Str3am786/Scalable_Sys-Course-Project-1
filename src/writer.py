# src/writer.py
import os, json, redis

from scalable_system.config import MATCHES_STREAM

GROUP = os.getenv("MATCH_WRITER_GROUP", "file_writers")
CONSUMER = os.getenv("MATCH_WRITER_NAME", os.getenv("HOSTNAME", "writer-1"))
OUTFILE = os.getenv("MATCHES_FILE", "/app/matches/matches.txt")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))  # <— add this

READ_COUNT = int(os.getenv("MATCH_WRITER_READ_COUNT", "200"))
BLOCK_MS = int(os.getenv("MATCH_WRITER_BLOCK_MS", "5000"))
FSYNC_EVERY = int(os.getenv("MATCH_WRITER_FSYNC_EVERY", "1"))

def ensure_group(r):
    try:
        r.xgroup_create(MATCHES_STREAM, GROUP, id="0-0", mkstream=True)
        print(f"[writer] Created group '{GROUP}' on stream '{MATCHES_STREAM}'", flush=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"[writer] Group '{GROUP}' already exists; continuing.", flush=True)
        else:
            raise

def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    ensure_group(r)

    def drain_history(f):
        # Claim any messages that exist but were never delivered to anyone in this group.
        while True:
            resp = r.xreadgroup(GROUP, CONSUMER, streams={MATCHES_STREAM: "0-0"}, count=READ_COUNT, block=1)
            if not resp:
                break
            total = 0
            ids = []
            for _, messages in resp:
                total += len(messages)
                for msg_id, fields in messages:
                    f.write(json.dumps(fields, separators=(",", ":")) + "\n")
                    ids.append(msg_id)
            if total:
                print(f"[writer] history wrote {total} lines", flush=True)
                f.flush(); os.fsync(f.fileno())
                r.xack(MATCHES_STREAM, GROUP, *ids)

    def tail(f):
        batch_no = 0
        while True:
            resp = r.xreadgroup(GROUP, CONSUMER, streams={MATCHES_STREAM: ">"}, count=READ_COUNT, block=BLOCK_MS)
            if not resp:
                continue
            ids = []
            total = 0
            for _, messages in resp:
                total += len(messages)
                for msg_id, fields in messages:
                    f.write(json.dumps(fields, separators=(",", ":")) + "\n")
                    ids.append(msg_id)
            if total:
                print(f"[writer] tail wrote {total} lines", flush=True)
            batch_no += 1
            f.flush()
            if batch_no % FSYNC_EVERY == 0:
                os.fsync(f.fileno())
            if ids:
                r.xack(MATCHES_STREAM, GROUP, *ids)

    with open(OUTFILE, "a", buffering=1) as f:
        print(f"[writer] Writing JSONL to {OUTFILE}", flush=True)
        drain_history(f)   # <— new
        tail(f)

if __name__ == "__main__":
    main()
