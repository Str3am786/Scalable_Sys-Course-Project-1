import redis
from ..config import REDIS_URL

def get_client() -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.from_url(REDIS_URL, decode_responses=False)
