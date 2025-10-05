import redis
from ..config import REDIS_URL

def get_client() -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.from_url(REDIS_URL, decode_responses=False)


def get_client_in_docker_net() -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.Redis(host = "redis", port=6379, decode_responses=False)

def get_default_client() -> redis.Redis:
    return redis.Redis(host='localhost', port=6379, decode_responses=False)

