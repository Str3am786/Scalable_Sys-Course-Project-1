from scalable_system.ingestion.input_reader import produce_csv


from datetime import datetime


if __name__ == "__main__":
    
    start = datetime.now()
    print("Start producing")
    produce_csv("/app/data/1_January/201701-citibike-tripdata.csv_1.csv",n_shards=10, max_rows=50000) #TODO update this
    print("Finished")
    
    end = datetime.now()
    
    print(f" TIME PRODUCER ------------------------------------------------ {((end - start )/ 60)}----------------------------------------------------------------------------")
    
    