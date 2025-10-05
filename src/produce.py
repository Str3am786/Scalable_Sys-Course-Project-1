from scalable_system.ingestion.input_reader import produce_csv


if __name__ == "__main__":
    
    
    print("Start producing")
    produce_csv("/app/data/",n_shards=8)
    print("Finished")
    
    