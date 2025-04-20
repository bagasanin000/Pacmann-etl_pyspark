from sqlalchemy import create_engine, Table, MetaData
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
from src.utils.config import log_engine

load_dotenv(".env", override=True)

# Setup Connection to db logger
DB_LOGGER_NAME = os.getenv("DB_LOGGER_NAME")
DB_LOGGER_URL = os.getenv("DB_LOGGER_URL")
DB_LOGGER_USER = os.getenv("DB_LOGGER_USER")
DB_LOGGER_PASS = os.getenv("DB_LOGGER_PASS")

def log_to_db(log_msg: dict):
    try:
        # Setup connection to PostgreSQL
        engine = log_engine()
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # Take table object
        log_table = metadata.tables.get("etl_logs")
        if log_table is None:
            raise Exception("Table 'etl_logs' not found in database")

        # Add timestamp if not exists
        if "etl_date" not in log_msg:
            log_msg["etl_date"] = datetime.now()

        # Use transaction (auto commit)
        with engine.begin() as conn:
            conn.execute(log_table.insert().values(**log_msg))

        print("Log successfully written to database")

    except Exception as e:
        print(f"Error writing log to database: {e}")


def save_invalid_ids(invalid_ids, table_name):
    if not invalid_ids:
        print(f"No invalid IDs to save from table '{table_name}'.")
        return

    try:
        # Normalize column, max 6 columns
        max_cols = 6 
        if len(invalid_ids[0]) > max_cols:
            raise ValueError("Too many columns in invalid_ids input")

        base_columns = [
            "entity_type", "object_id",
            "extra_entity_type_1", "extra_object_id_1",
        ]
        df = pd.DataFrame(invalid_ids, columns=base_columns[:len(invalid_ids[0])])
        df["table_name"] = table_name
        df["logged_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save to DB
        engine = log_engine()
        with engine.begin() as conn:
            df.to_sql("invalid_ids", con=conn, index=False, if_exists="append")
            print(f"{len(df)} invalid IDs from table '{table_name}' saved to logger DB.")

    except Exception as e:
        print(f"Error saving invalid IDs to logger DB: {e}")
