import multiprocessing as mp
from .worker import run_worker
from ..config import N_SHARDS

def run_all_shards(start_id: str = "0-0", n_shards: int = None):
    nsh = n_shards or N_SHARDS
    procs = []
    for i in range(nsh):
        p = mp.Process(target=run_worker, args=(i, start_id), daemon=True)
        p.start(); procs.append(p)
    for p in procs:
        p.join()
