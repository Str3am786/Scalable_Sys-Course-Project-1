from scalable_system.ingestion.input_reader import produce_csv


if __name__ == "__main__":
    
    print("Start producing")
    produce_csv("/app/data/1_January/d.csv",n_shards=10) #TODO update this
    print("Finished")
    
    