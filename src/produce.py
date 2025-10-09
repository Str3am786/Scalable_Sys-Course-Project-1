from scalable_system.ingestion.input_reader import produce_csv


from datetime import datetime


if __name__ == "__main__":
    
    
    start = datetime.now()
    print("Start producing")
    produce_csv("/app/data/1_January/d.csv",n_shards=10) #TODO update this
    print("Finished")
    
    end = datetime.now()
    
    print(f" TIME PRODUCER ------------------------------------------------ {((end - start )/ 60)}----------------------------------------------------------------------------")
    
    