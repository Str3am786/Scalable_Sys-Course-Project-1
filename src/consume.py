from scalable_system.workers.supervisor import run_all_shards


if __name__ == "__main__":
    
    print("Running")
    run_all_shards("$",n_shards=10)

