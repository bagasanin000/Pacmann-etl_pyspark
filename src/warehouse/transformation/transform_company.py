from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 

def transform_company(df):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "company",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 1: Replace "" to null
        df = df.na.replace("", None)

        # Step 2: Format data type
        # Extract prefix and ID to temporary columns
        df = df.withColumn("company_entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("company_object_id", extract_id(col("object_id")))
        
        df = df.withColumn("latitude", col("latitude").cast("decimal(9,6)"))
        df = df.withColumn("longitude", col("longitude").cast("decimal(9,6)"))
        
        # Step 3: Encoding
        df = df.withColumn("description", clean_text("description"))
        df = df.withColumn("address1", clean_text("address1"))
        df = df.withColumn("zip_code", clean_text("zip_code"))
        df = df.withColumn("region", clean_text("region"))
        
        log_to_db({
            "step": "Format Data",
            "status": "SUCCESS",
            "source": "staging",
            "table_name": "company",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 4: Mapping to target column
        df_transformed = df.select(
            col("office_id").alias("company_id"),
            col("company_entity_type"),
            col("company_object_id"),
            col("description"),
            col("address1").alias("address"),
            col("region"),
            col("city"),
            col("zip_code"),
            col("state_code"),
            col("country_code"),
            col("latitude"),
            col("longitude"),
            col("created_at"),
            col("updated_at")
        )

        log_to_db({
            "step": "Map Data",
            "status": f"SUCCESS ({df_transformed.count()} rows)",
            "source": "staging",
            "table_name": "company",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 5: Data cleansing 
        df_transformed = df_transformed.fillna({
            "description": "Unknown",
            "address": "Unknown",
            "region": "Unknown",
            "city": "Unknown",
            "zip_code": "Unknown",
            "state_code": "Unknown",
            "country_code": "Unknown"
        })

        # Step 6: Drop duplicate data  and latitude/longitude with value = 0
        df_transformed = df_transformed.dropDuplicates(["company_object_id"])
        df_transformed = df_transformed.filter((col("latitude") != 0) & (col("longitude") != 0))

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "company",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        print("The data is successfully transformed")

        return df_transformed 

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "company",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise