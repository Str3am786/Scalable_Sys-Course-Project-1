from typing import Dict
import time, json

from ..io.redis_client import get_client, wait_for_redis
from ..config import (
    STREAM_PREFIX, N_SHARDS, MAX_BIKES, CONTROL_STREAM, METRICS_STREAM, LATENCY_SLA_MS,
    INCLUDE_SCORE_METRICS, LOG_SCORE_EVERY_N, SHEDDING_EVENTS_STREAM
)
from ..core.sharding import stream_name
from ..core.lru import LRU
from ..core.models import Chain, Trip
from ..core.matching import process_trip_for_bike
from ..core.score import compute_score, set_hour_weights, compute_score_dbg
from ..core.score_index import ScoreIndex


def _b(fields, key):
    v = fields.get(key.encode());
    return v.decode() if v is not None else None

def run_worker(shard_idx: int, start_id: str = "0-0"):
    r = get_client()
    wait_for_redis(r)
    sname = stream_name(STREAM_PREFIX, shard_idx)
    state: Dict[str, Chain] = {}
    lru = LRU(MAX_BIKES)
    last_id = start_id

    idx = ScoreIndex()
    ctrl_last = "0-0"
    last_metrics = int(time.time() * 1000)
    proc = 0
    sum_latency = 0

    # score telemetry
    score_sum = 0.0
    score_count = 0
    score_min_seen = None
    score_max_seen = None
    score_seen_total = 0  # for sample logs

    # shedding telemetry
    evicted_last = 0
    evicted_total = 0

    print(f"[worker {shard_idx}] reading {sname} from {start_id}")
    while True:
        # Control channel
        ctrl = r.xread({CONTROL_STREAM: ctrl_last}, block=10, count=10)

        def _shed_event(cmd: str, arg: str, victims: int, states_after: int):
            try:
                r.xadd(SHEDDING_EVENTS_STREAM, {
                    b"shard": str(shard_idx).encode(),
                    b"cmd": cmd.encode(),
                    b"arg": arg.encode(),
                    b"dropped": str(victims).encode(),
                    b"states_after": str(states_after).encode(),
                    b"ts_ms": str(int(time.time()*1000)).encode(),
                })
            except Exception:
                pass

        if ctrl:
            _, entries = ctrl[0]
            for msg_id, fields in entries:
                cmd = _b(fields, "cmd") or ""
                arg = _b(fields, "arg") or ""
                if cmd == "DROP_BELOW":
                    thr = float(arg)
                    victims_list = idx.drop_below(thr)
                    victims = len(victims_list)
                    for bid in victims_list:
                        state.pop(bid, None); lru.remove(bid)
                    evicted_last += victims; evicted_total += victims
                    peek = idx.peek_min()
                    print(f"[shed] shard={shard_idx} DROP_BELOW {thr:.3f} "
                        f"dropped={victims} states={len(state)} heap_min={peek[0]:.3f}" if peek else
                        f"[shed] shard={shard_idx} DROP_BELOW {thr:.3f} dropped={victims} states={len(state)}")
                    _shed_event("DROP_BELOW", f"{thr:.3f}", victims, len(state))

                elif cmd == "DROP_K":
                    k = int(arg)
                    victims_list = idx.drop_k_worst(k)
                    victims = len(victims_list)
                    for bid in victims_list:
                        state.pop(bid, None); lru.remove(bid)
                    evicted_last += victims; evicted_total += victims
                    peek = idx.peek_min()
                    print(f"[shed] shard={shard_idx} DROP_K {k} "
                        f"dropped={victims} states={len(state)} heap_min={peek[0]:.3f}" if peek else
                        f"[shed] shard={shard_idx} DROP_K {k} dropped={victims} states={len(state)}")
                    _shed_event("DROP_K", str(k), victims, len(state))
                elif cmd == "SET_HOUR_WEIGHTS":
                    try:
                        set_hour_weights(json.loads(arg))
                        print(f"[worker {shard_idx}] hour weights updated")
                    except Exception as e:
                        print(f"[worker {shard_idx}] bad weights: {e}")
                ctrl_last = msg_id

        # Read events
        resp = r.xread({sname: last_id}, block=1000, count=1000)
        if not resp:
            # Idle flush so metrics still tick even with no events
            now_ms = int(time.time()*1000)
            if now_ms - last_metrics > 1000:
                avg_lat = int(sum_latency / proc) if proc else 0
                r.xadd(METRICS_STREAM, {
                    b"shard": str(shard_idx).encode(),
                    b"states": str(len(state)).encode(),
                    b"eps": str(proc).encode(),
                    b"avg_latency_ms": str(avg_lat).encode(),
                    b"over_sla": b"1" if (avg_lat > LATENCY_SLA_MS and avg_lat > 0) else b"0",
                    b"ts_ms": str(now_ms).encode(),
                })
                proc = 0; sum_latency = 0; last_metrics = now_ms
            continue

        _, entries = resp[0]
        for msg_id, fields in entries:
            # per-message latency accounting
            ig = 0
            ingest_ts = fields.get(b"ingest_ts_ms")
            if ingest_ts:
                try:
                    ig = int(ingest_ts.decode())
                except AttributeError:
                    ig = int(ingest_ts)
                sum_latency += max(0, int(time.time()*1000) - ig)
                proc += 1

            bike_id = _b(fields, "bike_id")
            ss = _b(fields, "start_station_id"); es = _b(fields, "end_station_id")
            st = _b(fields, "started_at_ms");   et = _b(fields, "ended_at_ms")
            if not (bike_id and ss and es and st and et):
                last_id = msg_id; continue

            trip = Trip(
                bike_id=bike_id,
                start_station_id=int(ss),
                end_station_id=int(es),
                started_at_ms=int(st),
                ended_at_ms=int(et),
                ingest_ts_ms=ig, 
            )
            process_trip_for_bike(state, lru, bike_id, trip, r)

            # scoring – keep heap in sync
            ch = state.get(bike_id)
            if ch is not None:
                sc = compute_score(ch, now_ms_val=trip.ended_at_ms)
                idx.upsert(bike_id, sc)

                # per-second stats
                score_sum += sc
                score_count += 1
                score_min_seen = sc if score_min_seen is None else min(score_min_seen, sc)
                score_max_seen = sc if score_max_seen is None else max(score_max_seen, sc)
                score_seen_total += 1

                # rare detailed sample
                if LOG_SCORE_EVERY_N > 0 and (score_seen_total % LOG_SCORE_EVERY_N == 0):
                    sc2, parts = compute_score_dbg(ch, now_ms_val=trip.ended_at_ms)
                    print(f"[score] sample #{score_seen_total} bike={bike_id} "
                        f"score={sc2:.3f} progress={parts['progress']:.3f} "
                        f"time_left={parts['time_left']:.3f} hour={parts['hour']:.3f} "
                        f"len={parts['len']} hour_num={parts['hour_num']}")

            last_id = msg_id  # advance per message

        # Periodic metrics flush
        now_ms = int(time.time()*1000)
        if now_ms - last_metrics > 1000:
            avg_lat = int(sum_latency / proc) if proc else 0
            payload = {
                b"shard": str(shard_idx).encode(),
                b"states": str(len(state)).encode(),
                b"eps": str(proc).encode(),
                b"avg_latency_ms": str(avg_lat).encode(),
                b"over_sla": b"1" if (avg_lat > LATENCY_SLA_MS and avg_lat > 0) else b"0",
                b"ts_ms": str(now_ms).encode(),
                b"evicted_total": str(evicted_total).encode(),
                b"evicted_last": str(evicted_last).encode(),
            }
            if INCLUDE_SCORE_METRICS:
                # avg/min/max from this one-second window
                avg_sc = (score_sum / score_count) if score_count else 0.0
                payload[b"avg_score"] = f"{avg_sc:.4f}".encode()
                payload[b"min_score"] = (f"{score_min_seen:.4f}".encode()
                                        if score_min_seen is not None else b"")
                payload[b"max_score"] = (f"{score_max_seen:.4f}".encode()
                                        if score_max_seen is not None else b"")
                # current heap min across all states
                peek = idx.peek_min()
                if peek:
                    payload[b"heap_min"] = f"{peek[0]:.4f}".encode()

            r.xadd(METRICS_STREAM, payload)
            # reset per-second counters
            proc = 0; sum_latency = 0; last_metrics = now_ms
            score_sum = 0.0; score_count = 0; score_min_seen = None; score_max_seen = None
            evicted_last = 0  # keep evicted_total accumulating


