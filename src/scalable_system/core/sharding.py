import hashlib
from . import models  # noqa

def shard_for_bike(bike_id: str, n_shards: int) -> int:
    h = int(hashlib.md5(bike_id.encode()).hexdigest(), 16)
    return h % n_shards

def stream_name(prefix: str, shard_idx: int) -> str:
    return f"{prefix}{shard_idx}"
