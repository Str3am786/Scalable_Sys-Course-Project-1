from scalable_system.io.redis_client import get_client_in_docker_net
import time
from datetime import datetime, timezone
import numpy as np
import zoneinfo
from redis.exceptions import ResponseError


    
N_SHARD = 10
N_PREFIX = "metrics:latency"
rate = 5 * 1000

PENDING_TH = 3
LATENCY_TH = 1
RESPONSIVINESS = 3


STATUS = np.zeros(N_SHARD)

def shedding_policy(n_pendings : int, shard_id : int, r, slow: bool = False) -> None:
    print("SHED: ", r.hget(f"fshedding:{shard_id}", "active"))

    prev = STATUS[shard_id]
    if (n_pendings > PENDING_TH) or slow:
        if STATUS[shard_id] < 3:
            STATUS[shard_id] += 1
    else:
        if STATUS[shard_id] > 0:
            STATUS[shard_id] -= 1
            
    if STATUS[shard_id] >= RESPONSIVINESS and prev < RESPONSIVINESS:
        r.hset(f"fshedding:{shard_id}", "active", "True")
        print("SWITCH ONNNNNNNN")
    elif  STATUS[shard_id] < RESPONSIVINESS and prev >= RESPONSIVINESS:
        r.hset(f"fshedding:{shard_id}", "active", "False")
        print("SWITCH OFFFFFFF")
    
    else:
        print("CONTINUEEEE")
        
    print(STATUS)

if __name__ == "__main__":
    

    r_stream = get_client_in_docker_net()
    r_stats = get_client_in_docker_net("redisstats")
    
    print(r_stats.ping())

    latest_i = {
            i : None for i in range(N_SHARD)
        }
        
    while(True):
        
        memory_info = r_stream.info('memory')
        cpu_info = r_stream.info('cpu')
        client_info = r_stream.info('clients')
        keyspace_info = r_stream.info('keyspace')
        command_stats = r_stream.info('commandstats')
            
        print("------------------------------------------ STREAM STATS --------------------------------------------------")

        print("Memory Used:", memory_info.get('used_memory_human'))
        print("Memory RSS:", memory_info.get('used_memory_rss_human', 'N/A'))
        print("CPU Sys:", cpu_info.get('used_cpu_user'))
        print("Connected Clients:", client_info.get('connected_clients'))
        print("Keys in DB0:", keyspace_info.get('db0', {}).get('keys', 'N/A'))
        print("--------------------------------------------- STATS ------------------------------------------------------")
        
        s = [0] * N_SHARD  # last timestamp queried per shard
        
        for i in range(N_SHARD):
            
            group_stats = r_stream.xpending(f"trips:{i}", f"shard:{i}")
            
            n_pendings = group_stats["pending"]
            
            shedding_policy(n_pendings,i,r_stream)
            
            
            # print("SHED: ", r_stream.hget(f"fshedding:{i}", "active")

            key = f"{N_PREFIX}:{i}"
            start_ts = s[i] + 1  
            now_ts = int(time.time() * 1000)
            
            # Query for new buckets only
            latest = None
            try:
                avg_samples = r_stats.ts().range(
                    key, start_ts, now_ts, 
                    aggregation_type="avg", 
                    bucket_size_msec=rate,
                    empty=True
                )
                latest = r_stats.ts().info(key)
            except ResponseError:
                continue
            
            if latest:    
                if latest_i[i] == latest["last_timestamp"]:
                    print("[",dt.now().strftime("%Y-%m-%d %H:%M:%S"),"]",f"Shard {i}: Waiting for Events")
                    continue
                latest_i[i] = latest["last_timestamp"]
                
                
            if avg_samples:
                avg_ms = round(avg_samples[-1][-1],2)
                ts = avg_samples[-1][0]
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                helsinki_tz = zoneinfo.ZoneInfo('Europe/Helsinki')
                local_dt = dt.astimezone(helsinki_tz)
                                
                print("[", local_dt.strftime("%Y-%m-%d %H:%M:%S"), "]",
                    f"Shard {i} Latency Average: {avg_ms} s , Pending: {n_pendings}")
                # Update last timestamp to last returned bucket timestamp
                s[i] = ts
                
                slow = (avg_ms >= LATENCY_TH)
                if slow:
                    print("SLOW")
                shedding_policy(n_pendings, i, r_stream, slow=slow)
        time.sleep(1)