import pandas as pd 
from src.infra.s3 import S3_SESSION 
import typing

class TrustedETL: 

    BUCKET_NAME = "amzn-website-scraper"
    INPUT_FOLDER = "01-layer-raw"
    OUTPUT_FOLDER = "02-layer-trusted"
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

        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

        print(files)

        for file in files:
            filename = file.split("/")[-1].split(".")[0]
            df = pd.read_parquet(f"{self.BASE_URL}/{file}")
            self._raw[filename] = df

        return self._raw

    def transform(self):
        for key, df_raw in self._raw.items():
            df = df_raw.copy() 

            df['price'] = df['price'].str.extract(r'(\d+,?\d+)', expand=False).str.replace(",", ".").astype(float) 
            df['previous_price'] = df['previous_price'].str.extract(r'(\d+,?\d+)', expand=False).str.replace(",", ".").astype(float) 
            df['discount'] = df['discount'].str.extract(r'(\d+)', expand=False).str.replace(",", ".").astype(float) 
            df['created_at'] = df['created_at'].astype(str) 
            
            self._dfs[key] = df

    def load(self):
        #Load to Raw Layer
        for key, df in self._dfs.items():
            df.to_parquet(f"{self.BASE_URL}/{self.OUTPUT_FOLDER}/{key}.parquet", index=False) 

    def pipeline(self):
        self.extract() 
        self.transform() 
        self.load() 

        print("Trusted ETL run successfully!")

if __name__ == "__main__":
    etl = TrustedETL() 
    print(etl.extract())