# import psutil
# import redis
# from redis.commands.timeseries import TimeSeries
# import time
# from datetime import datetime

# def format_bytes(size):
#     power = 1024
#     n = 0
#     labels = ['B', 'KB', 'MB', 'GB', 'TB']
#     while size > power and n < len(labels) - 1:
#         size /= power
#         n += 1
#     return f"{size:.2f} {labels[n]}"

# def get_redis_stream_length(r: redis.Redis, stream_prefix="trips:s", n_shards=4):
#     stream_lengths = {}
#     for i in range(n_shards):
#         stream = f"{stream_prefix}{i}"
#         try:
#             stream_lengths[stream] = r.xlen(stream)
#         except Exception as e:
#             stream_lengths[stream] = f"Error: {e}"
#     return stream_lengths

# def get_timeseries_latency_stats(ts: TimeSeries, metric_prefix="metrics:", n_shards=4):
#     stats = {}
#     for i in range(n_shards):
#         key = f"{metric_prefix}{i}"
#         try:
#             info = ts.info(key)
#             stats[key] = {
#                 "totalSamples": info["totalSamples"],
#                 "lastTimestamp": info["lastTimestamp"],
#                 "lastValue": ts.get(key)
#             }
#         except Exception as e:
#             stats[key] = f"Error: {e}"
#     return stats

# def get_system_usage():
#     return {
#         "cpu_percent": psutil.cpu_percent(interval=1),
#         "memory_percent": psutil.virtual_memory().percent,
#         "memory_used": format_bytes(psutil.virtual_memory().used),
#         "memory_total": format_bytes(psutil.virtual_memory().total),
#         "load_avg": psutil.getloadavg()
#     }

# def monitor(interval=5, redis_host="localhost", redis_port=6379, n_shards=4):
#     r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
#     ts = TimeSeries(r)

#     while True:
#         print("="*40)
#         print(f"📡 Monitor Tick @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

#         print("\n🔢 Redis Stream Lengths:")
#         stream_stats = get_redis_stream_length(r, n_shards=n_shards)
#         for stream, length in stream_stats.items():
#             print(f"{stream}: {length}")

#         print("\n📈 Redis TimeSeries Latency Stats:")
#         latency_stats = get_timeseries_latency_stats(ts, n_shards=n_shards)
#         for key, data in latency_stats.items():
#             print(f"{key}: {data}")

#         print("\n💻 System Resource Usage:")
#         sys_stats = get_system_usage()
#         for k, v in sys_stats.items():
#             print(f"{k}: {v}")

#         time.sleep(interval)

# monitor(redis_host="redisstats", redis_port=6379, n_shards=4)


from scalable_system.io.redis_client import get_client_in_docker_net
import time
# Connect to Redis (adjust host/port if needed)
r = get_client_in_docker_net()
r_stats = get_client_in_docker_net("redsstats")

# Get all INFO
all_info = r.info()


while(True):
# Or get specific sections
    memory_info = r.info('memory')
    cpu_info = r.info('cpu')
    client_info = r.info('clients')
    keyspace_info = r.info('keyspace')
    command_stats = r.info('commandstats')

    # Example outputs
    print("Memory Used:", memory_info.get('used_memory_human'))
    print("Memory RSS:", memory_info.get('used_memory_rss_human', 'N/A'))
    print("CPU Sys:", cpu_info.get('used_cpu_user'))
    print("Connected Clients:", client_info.get('connected_clients'))
    print("Keys in DB0:", keyspace_info.get('db0', {}).get('keys', 'N/A'))

    # Optionally, dump all stats:
    # import pprint; pprint.pprint(all_info)
    time.sleep(3)
