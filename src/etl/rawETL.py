import pandas as pd 
from src.infra.s3 import S3_SESSION 
import typing

class RawETL: 

    BUCKET_NAME = "amzn-website-scraper"
    INPUT_FOLDER = "00-layer-ingestion"
    OUTPUT_FOLDER = "01-layer-raw"
    BASE_URL = f"s3://{BUCKET_NAME}"

    # Save it as a raw data 
    def __init__(self):
        self.bucket_name = self.BUCKET_NAME 
        self._raw: typing.Dict[str, pd.DataFrame] = {}
        self._dfs: typing.Dict[str, pd.DataFrame] = {}

    def extract(self):      

        response = S3_SESSION.list_objects_v2(
            Bucket=self.BUCKET_NAME,
            Prefix=f"{self.INPUT_FOLDER}/"
        )

        if 'Contents' not in response:
            print("No files found")
            return []

        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.jsonl')]

        for file in files:
            filename = file.split("/")[-1].split(".")[0]
            df = pd.read_json(f"{self.BASE_URL}/{file}", lines=True)
            self._raw[filename] = df

        return self._raw

    def transform(self):
        for key, df_raw in self._raw.items():
            df = df_raw.drop_duplicates(subset=['item_id', 'price'])  
            self._dfs[key] = df

    def load(self):
        #Load to Raw Layer
        for key, df in self._dfs.items():
            df.to_parquet(f"{self.BASE_URL}/{self.OUTPUT_FOLDER}/{key}.parquet", index=False) 

    def pipeline(self):
        self.extract() 
        self.transform() 
        self.load() 

        print("Raw ETL run successfully!")