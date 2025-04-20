import os
import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from src.utils.logger import log_to_db

spark = SparkSession.builder.appName("Extract API").getOrCreate()

def extract_api(link_api: str, list_parameter: dict, data_name: str):
    try:
        # Establish connection to API
        resp = requests.get(link_api, params=list_parameter)
        resp.raise_for_status()  # Raise an exception for HTTP errors

        # Parse the response JSON
        raw_response = resp.json()

        # Convert JSON data to pandas DataFrame
        df_api = pd.DataFrame(raw_response)

        if df_api.empty:
            raise ValueError("Empty response from API")

        # Convert pandas DataFrame to PySpark DataFrame
        spark_df = spark.createDataFrame(df_api)

        # Log success
        log_msg = {
            "step": "Extract",
            "status": "Success",
            "source": "API",
            "table_name": data_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        log_to_db(log_msg)

        print(f"Successfully extracted data from API: {data_name}")
        return spark_df

    except requests.exceptions.RequestException as e:
        # Log request failure
        log_msg = {
            "step": "Extract",
            "status": f"Failed: {e}",
            "source": "API",
            "table_name": data_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        log_to_db(log_msg)
        print(f"Request failed: {e}")

    except ValueError as e:
        # Log parsing failure
        log_msg = {
            "step": "Extract",
            "status": f"Failed: {e}",
            "source": "API",
            "table_name": data_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        log_to_db(log_msg)
        print(f"Parsing error: {e}")

    except Exception as e:
        # Catch any other errors
        log_msg = {
            "step": "Extract",
            "status": f"Failed: {e}",
            "source": "API",
            "table_name": data_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        log_to_db(log_msg)
        print(f"An error occurred: {e}")

    return None

if __name__ == "__main__":
    extract_api()


