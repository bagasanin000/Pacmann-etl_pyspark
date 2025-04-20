import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pangres import upsert
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from src.utils.logger import log_to_db
from src.utils.config import staging_engine

load_dotenv(".env", override=True)

# Setup Connection to Staging
DB_STAGING_NAME = os.getenv("DB_STAGING_NAME")
DB_STAGING_URL = os.getenv("DB_STAGING_URL")
DB_STAGING_USER = os.getenv("DB_STAGING_USER")
DB_STAGING_PASS = os.getenv("DB_STAGING_PASS")

# Create Spark session
spark = SparkSession \
    .builder \
    .appName("Load Staging") \
    .config("spark.jars", "/path/to/postgresql-42.2.5.jar")\
    .getOrCreate()

def load_staging2(df, table_name, mode="overwrite", use_upsert=False, idx_name=None, schema=None, source=None):
    try:
        if use_upsert:
            # Convert Spark DataFrame to Pandas DataFrame
            data = df.toPandas()

            # Create connection to PostgreSQL
            conn = staging_engine()

            # Set index for upsert
            if idx_name is None:
                raise ValueError("Index name is required for upsert mode")

            data = data.set_index(idx_name)

            # Upsert
            upsert(
                con=conn,
                df=data,
                table_name=table_name,
                schema=schema,
                if_row_exists="update"
            )
            print(f"Data upserted to table '{table_name}' successfully!")
        else:
            # Load using Spark
            df.write \
                .format("jdbc") \
                .option("url", DB_STAGING_URL) \
                .option("dbtable", table_name) \
                .option("user", DB_STAGING_USER) \
                .option("password", DB_STAGING_PASS) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()

            print(f"Data loaded to table '{table_name}' successfully!")

        # Success log
        log_msg = {
            "step": "Load Staging",
            "status": "Success",
            "source": "Staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        print(f"Error loading data to table '{table_name}': {e}")

        # Failed DataFrame
        failed_data = df.toPandas() if not use_upsert else data
        failed_data['error_message'] = str(e)
        failed_data['etl_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Fail log
        log_msg = {
            "step": "Load Staging",
            "status": "Failed",
            "source": "Staging",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_message": str(e)
        }

        # Save failed data to CSV
        failed_log_path = f'logs/failed_{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        failed_data.to_csv(failed_log_path, index=False)
        print(f"Failed data saved to: {failed_log_path}")

    finally:
        # Delete error_message before save it to log
        if 'error_message' in log_msg:
            del log_msg['error_message']

        # Simpan log ke CSV
        log_to_db(log_msg)

    return df if not use_upsert else data


if __name__ == "__main__":
    load_staging2()
