import pyspark
from pyspark.sql import SparkSession
from datetime import datetime
from src.utils.logger import log_to_db
import os

spark = SparkSession \
    .builder \
    .appName("Extract CSV") \
    .getOrCreate()

def extract_csv(file_path, table_name):
    try:
        # Read CSV using Spark
        df = spark.read.option("header", "true").csv(file_path)

        # Show extracted data
        df.show()

        # Log success
        log_to_db({
            "step": "Extract",
            "status": "Success",
            "source": "CSV",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        return df
    
    except Exception as e:
        # Log failure
        log_to_db({
            "step": "Extract",
            "status": f"Failed: {e}",
            "source": "CSV",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        print(f"Error extracting {file_path}: {e}")
        return None
        

if __name__ == "__main__":
    extract_csv()