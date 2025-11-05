from scalable_system.ingestion.input_reader import produce_csv


from datetime import datetime

from scalable_system.config import N_SHARDS, INPUT_CSV

if __name__ == "__main__":
    
    start = datetime.now()
    print("Start producing")
    produce_csv(dir_path=INPUT_CSV,n_shards=N_SHARDS)
    print("Finished")
    
    end = datetime.now()
    
    print(f" TIME PRODUCER ------------------------------------------------ {((end - start )/ 60)}----------------------------------------------------------------------------")
    
    