from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pangres import upsert
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from src.utils.logger import log_to_db

load_dotenv(".env", override=True)

# Setup Connection to Staging
DB_STAGING_NAME = os.getenv("DB_STAGING_NAME")
DB_STAGING_URL = os.getenv("DB_STAGING_URL")
DB_STAGING_USER = os.getenv("DB_STAGING_USER")
DB_STAGING_PASS = os.getenv("DB_STAGING_PASS")

spark = SparkSession \
    .builder \
    .appName("Extract from Staging") \
    .config("spark.jars", "/path/to/postgresql-42.2.5.jar")\
    .getOrCreate()

def extract_from_staging():
    try:
        # Get list of tables from staging
        table_list = spark.read \
            .format("jdbc") \
            .option("url", DB_STAGING_URL) \
            .option("dbtable", "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS tbl") \
            .option("user", DB_STAGING_USER) \
            .option("password", DB_STAGING_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select("table_name") \
            .rdd.flatMap(lambda x: x).collect()

        print(f"Found tables in staging: {table_list}")

        tables = {}
        for table in table_list:
            try:
                # Read each table into a DataFrame
                df = spark.read \
                    .format("jdbc") \
                    .option("url", DB_STAGING_URL) \
                    .option("dbtable", table) \
                    .option("user", DB_STAGING_USER) \
                    .option("password", DB_STAGING_PASS) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                tables[table] = df

                # Log success for each table
                log_to_db({
                    "step": "Extract",
                    "status": "Success",
                    "source": "PostgreSQL (Staging)",
                    "table_name": table,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                print(f"Successfully extracted table: {table}")

            except Exception as e:
                # Log failure for specific table
                log_to_db({
                    "step": "Extract",
                    "status": f"Failed: {e}",
                    "source": "PostgreSQL (Staging)",
                    "table_name": table,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                print(f"Failed to extract table: {table} - Error: {e}")

        return tables
    
    except Exception as e:
        # Log failure for the whole extraction process
        log_to_db({
            "step": "Extract",
            "status": f"Failed: {e}",
            "source": "PostgreSQL (Staging)",
            "table_name": "N/A",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        print(f"Failed to extract tables: {e}")
        return {}

if __name__ == "__main__":
    extract_from_staging()