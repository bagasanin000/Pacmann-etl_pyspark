from pyspark.sql import SparkSession
from datetime import datetime
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from pyspark.sql.functions import col, to_date, broadcast, to_timestamp, when, lit
from src.warehouse.transformation.transform_company import transform_company

def transform_ipo(df, transformed_company):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "ipo",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 1: Clean & Format
        df = df.na.replace("", None)
        df = df.withColumn("public_at", to_date(col("public_at")))
        df = df.withColumn("created_at", to_timestamp(col("created_at")))
        df = df.withColumn("updated_at", to_timestamp(col("updated_at")))
        df = df.withColumn("ipo_entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("ipo_object_id", extract_id(col("object_id")).cast("int"))
        df = df.withColumn("stock_market", extract_stock_market(col("stock_symbol")))
        df = df.withColumn("stock_symbol", extract_stock_symbol(col("stock_symbol")))

        # Step 2: Select columns and fill defaults
        df_transformed = df.select(
            "ipo_id", "ipo_entity_type", "ipo_object_id",
            col("valuation_currency_code").alias("valuation_currency"),
            "valuation_amount",
            col("raised_currency_code").alias("raised_currency"),
            "raised_amount", "public_at",
            "stock_market", "stock_symbol",
            "source_url", "source_description",
            "created_at", "updated_at"
        ).fillna({
            "valuation_amount": 0.0,
            "valuation_currency": "USD",
            "raised_amount": 0.0,
            "raised_currency": "USD",
            "stock_market": "N/A",
            "stock_symbol": "N/A",
            "source_url": "Unknown",
            "source_description": "Unknown"
        }).dropDuplicates(["ipo_id"])

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "ipo",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 3: Filter only companies
        df_filtered = df_transformed.filter(col("ipo_entity_type") == "c")

        # Step 4: Join with dim_company to validate foreign key
        df_company_ids = transformed_company.select("company_object_id").distinct()

        df_valid = (
            df_filtered
            .join(broadcast(df_company_ids), on=col("ipo_object_id") == col("company_object_id"), how="inner")
            .drop("company_object_id")
        )

        df_invalid = (
            df_filtered
            .join(broadcast(df_company_ids), on=col("ipo_object_id") == col("company_object_id"), how="left_anti")
        )

        if df_invalid.count() > 0:
            invalid_ids = df_invalid.select("ipo_entity_type", "ipo_object_id").toPandas().values.tolist()
            save_invalid_ids(invalid_ids, table_name="ipo")
            log_to_db({
                "step": "Validation",
                "status": f"{df_invalid.count()} IPO rows missing company match",
                "source": "staging",
                "table_name": "ipo",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        print("The data is successfully transformed")
        return df_valid

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "ipo",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise

