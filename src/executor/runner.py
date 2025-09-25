import os.path
import sys
from opencep.CEP import CEP
from opencep.base.Pattern import Pattern
from opencep.stream.FileStream import FileOutputStream
from src.ingestion.citibike_reader import citi_bike_stream
from src.ingestion.CitiBikeDataFormatter import *




class Runner:
    def __init__(self, pattern: Pattern, bursty:bool, limit:int|None=None):
        try:
            self.cep = CEP([pattern])
        except Exception as e:
            print(f"Error while creating CEP - {str(e)}")
            raise e
        try:
            if bursty:
                raise Exception("Not implemented yet")
            else:
                self.events = citi_bike_stream(limit=limit)
        except Exception as e:
            print(f"Error while creating Input Stream - {str(e)}")
            raise e


    def run(self):
        try:
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            output_directory = os.path.join(BASE_DIR, "..", "results", "matches")
            output_directory = os.path.normpath(output_directory)
            os.makedirs(output_directory, exist_ok=True)
            self.cep.run(self.events, FileOutputStream(output_directory, 'output.txt'), CitiBikeDataFormatter())
        except Exception as e:
            raise e

