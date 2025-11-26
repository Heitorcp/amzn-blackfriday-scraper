from .ingestionETL import IngestionETL  
from .rawETL import RawETL 
from .trustedETL import TrustedETL

def pipeline():
    
    data_source = r"C:\Users\heito\Desktop\projects\personal\amazon-project\src\scraper\data"
    
    ingestion_etl = IngestionETL(data_source, full_dump=True) 
    ingestion_etl.pipeline() 

    raw_etl = RawETL() 
    raw_etl.pipeline() 

    trusted_etl = TrustedETL() 
    trusted_etl.pipeline() 

if __name__ == "__main__":
    pipeline()