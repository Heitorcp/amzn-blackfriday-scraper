import glob 
from src.infra.s3 import upload_file
from datetime import datetime 

class IngestionETL:

    INGESTION_LAYER_FOLDER = "00-layer-ingestion"
    
    def __init__(self, files_path:str, full_dump:bool):
        self._output_bkt = "amzn-website-scraper" 
        self._files_path = files_path 
        self.full_dump = full_dump 
        self.today = datetime.now().date().strftime("%Y-%m-%d")

    def __filter_files_by_date(self, min_date=None):
        files = glob.glob(f"{self._files_path}/*.jsonl")  
        filtered = [file for file in files if file.__contains__(min_date)] 
        return filtered

    def _select_files(self):
        if self.full_dump:
            files = glob.glob(f"{self._files_path}/*.jsonl") 
        else: 
            min_date_filter = self.today
            files = self.__filter_files_by_date(min_date_filter) 
        return files

    def upload_to_bucket(self):
        files = self._select_files()
        for file in files:
            file_name = file.split("\\")[-1]  # Get just the filename
            object_name = f"{self.INGESTION_LAYER_FOLDER}/{file_name}"
            upload_file(file, self._output_bkt, object_name) 

    def pipeline(self):
        self.upload_to_bucket()