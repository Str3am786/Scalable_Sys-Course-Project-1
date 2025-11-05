from scalable_system.workers.supervisor import run_all_shards
from scalable_system.config import N_SHARDS

if __name__ == "__main__":
    
    print("Running")
    run_all_shards("$",n_shards=N_SHARDS)

