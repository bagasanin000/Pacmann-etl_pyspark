from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv(".env", override=True)

DB_LOGGER_HOST = "host.docker.internal"
DB_LOGGER_PORT = 5432
DB_LOGGER_DBNAME = "pyspark_task_logger"
DB_LOGGER_USER = os.getenv("DB_LOGGER_USER")
DB_LOGGER_PASS = os.getenv("DB_LOGGER_PASS")

DB_SOURCE_DBNAME = "pyspark_task_datasource"
DB_SOURCE_USER = os.getenv("DB_USER")
DB_SOURCE_PASS = os.getenv("DB_PASS")

DB_STAGING_DBNAME = "pyspark_task_staging"
DB_STAGING_USER = os.getenv("DB_STAGING_USER")
DB_STAGING_PASS = os.getenv("DB_STAGING_PASS")

DWH_DBNAME = "pyspark_task_dwh"
DWH_USER = os.getenv("DWH_USER")
DWH_PASS = os.getenv("DWH_PASS")


def log_engine():
    return create_engine(f"postgresql://{DB_LOGGER_USER}:{DB_LOGGER_PASS}@{DB_LOGGER_HOST}:{DB_LOGGER_PORT}/{DB_LOGGER_DBNAME}")

def datasource_engine():
    return create_engine(f"postgresql://{DB_SOURCE_USER}:{DB_SOURCE_PASS}@{DB_LOGGER_HOST}:{DB_LOGGER_PORT}/{DB_SOURCE_DBNAME}")

def staging_engine():
    return create_engine(f"postgresql://{DB_STAGING_USER}:{DB_STAGING_PASS}@{DB_LOGGER_HOST}:{DB_LOGGER_PORT}/{DB_STAGING_DBNAME}")

def warehouse_engine():
    return create_engine(f"postgresql://{DWH_USER}:{DWH_PASS}@{DB_LOGGER_HOST}:{DB_LOGGER_PORT}/{DWH_DBNAME}")

