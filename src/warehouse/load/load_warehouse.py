from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pangres import upsert
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from src.utils.logger import log_to_db
from src.utils.config import warehouse_engine

load_dotenv(".env", override=True)

# Setup Connection to Data Warehouse
DWH_URL_NAME = os.getenv("DWH_URL_NAME")
DWH_URL = os.getenv("DWH_URL")
DWH_USER = os.getenv("DWH_USER")
DWH_PASS = os.getenv("DWH_PASS")

spark = SparkSession \
    .builder \
    .appName("Load DWH") \
    .config("spark.jars", "/path/to/postgresql-42.2.5.jar")\
    .getOrCreate()

def load_to_dwh(df, table_name, mode="overwrite", use_upsert=False, idx_name=None, schema=None, source=None):

    os.makedirs("logs", exist_ok=True)

    try:
        if not DWH_USER or not DWH_PASS:
            raise EnvironmentError("DWH_USER or DWH_PASS is not set")

        if use_upsert:
            data = df.toPandas()
            if idx_name is None:
                raise ValueError("Index name is required for upsert mode")
            data = data.set_index(idx_name)

            conn = warehouse_engine()
            upsert(
                con=conn,
                df=data,
                table_name=table_name,
                schema=schema,
                if_row_exists="update"
            )
            print(f"Data upserted to table '{table_name}' successfully!")
        else:
            df.write \
              .format("jdbc") \
              .option("url", DWH_URL) \
              .option("dbtable", table_name) \
              .option("user", DWH_USER) \
              .option("password", DWH_PASS) \
              .option("driver", "org.postgresql.Driver") \
              .mode(mode) \
              .save()
            print(f"Data loaded to table '{table_name}' successfully!")

        log_msg = {
            "step": "Load to DWH",
            "status": "Success",
            "source": "transformed data",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        print(f"Error loading data to table '{table_name}': {e}")
        failed_data = data if use_upsert else df.toPandas()
        failed_data['error_message'] = str(e)
        failed_data['etl_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        log_msg = {
            "step": "Load to DWH",
            "status": f"Failed - {str(e)}",
            "source": "transformed data",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "error_message": str(e)
        }

        # failed_log_path = f'logs/failed_{table_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        # failed_data.to_csv(failed_log_path, index=False)
        # print(f"Failed data saved to: {failed_log_path}")

    finally:
        log_msg.pop("error_message", None)
        log_to_db(log_msg)

    return df if not use_upsert else data

if __name__ == "__main__":
    load_to_dwh()

