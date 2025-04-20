import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from src.utils.logger import log_to_db

load_dotenv(".env", override=True)

# Setup Connection to Database (data source)
DB_SOURCE_NAME = os.getenv("DB_SOURCE_NAME")
DB_SOURCE_URL = os.getenv("DB_SOURCE_URL")
DB_SOURCE_USER = os.getenv("DB_SOURCE_USER")
DB_SOURCE_PASS = os.getenv("DB_SOURCE_PASS")

spark = SparkSession \
    .builder \
    .appName("Extract from DB") \
    .config("spark.jars", "/path/to/postgresql-42.2.5.jar")\
    .getOrCreate()


def extract_from_db():
    try:
        # Get list of tables from the database
        table_list = spark.read \
            .format("jdbc") \
            .option("url", DB_SOURCE_URL) \
            .option("dbtable", "information_schema.tables") \
            .option("user", DB_SOURCE_USER) \
            .option("password", DB_SOURCE_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .filter("table_schema = 'public'") \
            .select("table_name") \
            .rdd.flatMap(lambda x: x).collect()

        print(f"Found tables: {table_list}")

        tables = {}
        for table in table_list:
            try:
                # Read each table into a DataFrame
                df = spark.read \
                    .format("jdbc") \
                    .option("url", DB_SOURCE_URL) \
                    .option("dbtable", table) \
                    .option("user", DB_SOURCE_USER) \
                    .option("password", DB_SOURCE_PASS) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                tables[table] = df

                # Log success for each table
                log_to_db({
                    "step": "Extract",
                    "status": "Success",
                    "source": "PostgreSQL",
                    "table_name": table,
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                print(f"Successfully extracted table: {table}")

            except Exception as e:
                # Log failure for specific table
                log_to_db({
                    "step": "Extract",
                    "status": f"Failed: {e}",
                    "source": "PostgreSQL",
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
            "source": "PostgreSQL",
            "table_name": "N/A",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        print(f"Failed to extract tables: {e}")
        return {}


if __name__ == "__main__":
    extract_from_db()