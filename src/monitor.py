from scalable_system.io.redis_client import get_client_in_docker_net
import time
from datetime import datetime, timezone
import numpy as np
import zoneinfo
from redis.exceptions import ResponseError



if __name__ == "__main__":
    # Connect to Redis (adjust host/port if needed)
    r_stream = get_client_in_docker_net()
    r_stats = get_client_in_docker_net("redisstats")
    
    print(r_stats.ping())
    print(r_stats)
    
    N_SHARD = 10
    N_PREFIX = "metrics:latency"
    rate = 5 * 1000
    
    time.sleep(3)
    
    latest_i = {
            i : None for i in range(N_SHARD)
        }
    
    while(True):
    # Or get specific sections
        memory_info = r_stream.info('memory')
        cpu_info = r_stream.info('cpu')
        client_info = r_stream.info('clients')
        keyspace_info = r_stream.info('keyspace')
        command_stats = r_stream.info('commandstats')
    
        # Example outputs
        
        print("------------------------------------------ STREAM STATS --------------------------------------------------")

        print("Memory Used:", memory_info.get('used_memory_human'))
        print("Memory RSS:", memory_info.get('used_memory_rss_human', 'N/A'))
        print("CPU Sys:", cpu_info.get('used_cpu_user'))
        print("Connected Clients:", client_info.get('connected_clients'))
        print("Keys in DB0:", keyspace_info.get('db0', {}).get('keys', 'N/A'))
        
        print("--------------------------------------------- STATS ------------------------------------------------------")
        s = [0] * N_SHARD  # last timestamp queried per shard
        
        for i in range(N_SHARD):
            
            summary = r_stream.xpending(f"trips:{i}", f"shard:{i}")
            print(summary)  # includes count, min ID, max ID, consumers info

            
            key = f"{N_PREFIX}:{i}"
            start_ts = s[i] + 1  
            now_ts = int(time.time() * 1000)
            prev = []
            # Query for new buckets only
            
            
            latest = None
            try:
                avg_samples = r_stats.ts().range(key, start_ts, now_ts, aggregation_type="avg", bucket_size_msec=rate,empty=True)
                latest = r_stats.ts().info(key)
            except ResponseError:
                continue
            
            if latest:
                                
                if latest_i[i] == latest["last_timestamp"]:
                    print("[",dt.now().strftime("%Y-%m-%d %H:%M:%S"),"]",f"Shard {i}: Waiting for Events")
                    continue
                latest_i[i] = latest["last_timestamp"]
                
            if avg_samples:
                dt = datetime.fromtimestamp(avg_samples[-1][0]/ 1000,  tz=timezone.utc)  
                helsinki_tz = zoneinfo.ZoneInfo('Europe/Helsinki')
                local_dt = dt.astimezone(helsinki_tz)
                                
                print("[",dt.strftime("%Y-%m-%d %H:%M:%S"),"]",f"Shard {i} Latency Average: ", round(avg_samples[-1][-1],2))
                # Update last timestamp to last returned bucket timestamp
                s[i] = avg_samples[-1][0]
                
        time.sleep(5)