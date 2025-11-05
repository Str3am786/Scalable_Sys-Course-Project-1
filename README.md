# Efficient Pattern Detection over Data Streams (Citi Bike 2017)

> A small, containerized Complex Event Processing (CEP) pipeline over **Redis Streams** that detects “hot paths” in Citi Bike trip data. One command brings up ingestion, per-shard workers, a match writer, and lightweight monitoring. 

---

## Quick Start
### 1) Prerequisites
- Docker & Docker Compose
- ~2 GB free disk space (for the 2017 subset)
- Git (if cloning)

### 2) Data
Use your own subset if you like; keep the folder name or adjust environment variables below. See [Configuration](#configuration)

### 3) Build & Run

```bash
docker compose up --build
```

This launches:
 - Redis —> broker
 - producer —> reads CSVs → sharded Redis Streams
 - consumer —> supervisor + one worker per shard (continuity + 1-hour window)
 - writer —> backfills & tails the match stream → JSONL file
 - monitor —> simple health/latency & shedding triggers 

With the default settings which you can see within [config.py](src/scalable_system/config.py)

## Goal of the project:
For this assignment we were tasked with detecting "Hot Paths", a pattern that highlights stations where bicycles accumulate more quickly than elsewhere; such paths reveal prevailing movement trends and inform operational choices for relocation.

Hotpath Pattern:
```sql
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {7,8,9} AND a[last].bike = b.bike AND a[i+1].start = a[i].end WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)
```

### Datasource
Citi Bike System Data (operated by Lyft, Inc.) — public trip records with timestamps, stations, and bike identifiers. We use the 2017 subset for this project.

Other datasets pertaining to citibike can be found here: [Citibike Datasets](https://citibikenyc.com/system-data)

### Why did we use Redis?

- Per-key order, short windowed state, and a thin, inspectable CEP layer. Easy to deploy in Docker; straightforward to monitor. 
- Processing model. Stable shard function -> one worker per shard -> per-bike ordered chains with a 1-hour window and continuity checks. 
- Shedding. Optional, per-shard flags trigger either drop_k or shrink_window; the matching logic stays unchanged, state/work are bounded under bursts. 

We understand that there are limitations to our implementation and discuss this within the [Report](report/report.pdf)

## Output:
JSON Lines file at ```MATCHES_FILE``` (by default ``/app/matches/matches.txt``, commonly volume-mapped to ``./matches/``).

Example of a line in matches.txt:
```json
{"bike_id":"12345","chain_len":4,"t_first":"2017-06-10T08:02:33Z","t_last":"2017-06-10T08:44:01Z","end_station":"317","shard":6,"emit_ts":1730790000123}
```
Each line: bike id, chain summary (length, timestamps), terminal station, shard, and timing fields for end-to-end latency analysis.
## Configuration
 All settings are environment variables with safe defaults see [config.py](src/scalable_system/config.py)

If you want to modify any of them at run time change the [docker-compose.yaml](docker-compose.yaml)
### I/O & Streams
- `STREAM_PREFIX` — prefix for trip streams (default: `trips:`)
- `MATCHES_STREAM` — stream for detected matches (default: `matches`)
- `MATCHES_FILE` — output path for matches file (default: `/app/matches/matches.txt`)

### Redis
- `REDIS_HOST` (default: `redis`)
- `REDIS_PORT` (default: `6379`)
- `REDIS_DB` (default: `0`)
- `REDIS_URL` — Leave unset unless you want to override (Automatically takes from `REDIS_HOST` and `REDIS_PORT`)

### Sharding & Window
- `N_SHARDS` — number of workers/shards (min 1; default: `10`)
- `ONE_HOUR_MS` — base window length in ms (default: `3600000`)
- `HOT_END_STATIONS` — comma-sep station IDs considered “hot” (default: `7,8,9`)

### State, Dedupe & Writer Read
- `MAX_BIKES` — active bikes cap (default: `200000`)
- `MAX_TRIPS_PER_BIKE` — per-bike chain cap (default: `16`)
- `DEDUPE_TTL_MS` — TTL for match dedupe (default: `86400000`)
- `MATCH_WRITER_BLOCK_MS` — `XREAD` block timeout (default: `5000`)
- `MATCH_WRITER_READ_COUNT` — batch size (default: `200`)

### Load-Shedding (optional)
- `SHEDDING_MECH` — `None` | `drop_k` | `shrink_window` (default: `None`)
- `SHED_DROP_K` — for `drop_k`, remove K oldest from each per-bike chain (default: `3`)
- `SHED_WINDOW_MS` — for `shrink_window`, keep only the most recent window (default: `ONE_HOUR_MS/2`)
- `SHED_KEEP_LAST_N` — for a “keep-last-N” variant when enabled (default: `1`)


## Team
Luca Checchin • Miguel Arroyo Marquez • Nicolas Kivimäki