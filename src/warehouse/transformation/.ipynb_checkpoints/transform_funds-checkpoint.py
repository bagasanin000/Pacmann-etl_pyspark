from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from pyspark.sql.types import IntegerType, StringType


def transform_funds(df):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "Staging",
            "table_name": "funds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df = df.na.replace("", None)

        df = df.withColumn("fund_entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("fund_object_id", extract_id(col("object_id")).cast(IntegerType()))
        df = df.withColumn("funding_date", to_date(col("funded_at")))

        df_transformed = df.select(
            col("fund_id"),
            col("fund_entity_type"),
            col("fund_object_id"),
            col("name").alias("fund_name"),
            col("funding_date"),
            col("raised_currency_code").alias("raised_currency"),
            col("raised_amount"),
            col("source_url"),
            col("source_description"),
            col("created_at"),
            col("updated_at")
        )

        df_transformed = df_transformed.fillna({
            "raised_currency": "USD",
            "raised_amount": 0.0,
            "source_url": "Unknown",
            "source_description": "Unknown"
        })

        df_transformed = df_transformed.dropDuplicates(["fund_id"])
        df_transformed = df_transformed.na.drop(subset=["funding_date"])
        df_transformed = df_transformed.withColumn(
            "source_url", when(col("source_url").rlike(r"^(http|https)://.*"), col("source_url")).otherwise("Unknown")
        ).withColumn(
            "source_description", clean_alpha_text("source_description")
        )

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "funds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        print("The data is successfully transformed")
        
        return df_transformed


    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "funds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
