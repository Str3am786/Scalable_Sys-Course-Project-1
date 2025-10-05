import time, redis
from ..config import REDIS_URL

def wait_for_redis(r, timeout=60):
    deadline = time.time() + timeout
    while True:
        try:
            r.ping()
            return
        except (redis.exceptions.BusyLoadingError,
                redis.exceptions.ConnectionError):
            if time.time() > deadline:
                raise
            time.sleep(0.25)

def get_client() -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.from_url(REDIS_URL, decode_responses=False)

def get_default_client() -> redis.Redis:
    return redis.Redis(host='localhost', port=6379, decode_responses=False)

