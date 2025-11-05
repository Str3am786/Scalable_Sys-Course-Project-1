import redis

from ..config import REDIS_URL, REDIS_PORT, REDIS_DB

def get_client() -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.from_url(REDIS_URL, decode_responses=False)


def get_client_in_docker_net(host_name = "redis") -> redis.Redis:
    # decode_responses=False to keep bytes and avoid accidental type coercion
    return redis.Redis(host = host_name, port=REDIS_PORT, db=REDIS_DB ,decode_responses=False)


# TODO DEPRECATED
def get_default_client() -> redis.Redis:
    return redis.Redis(host='localhost', port=REDIS_PORT, decode_responses=False)

