from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, concat_ws, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 

def transform_people(df, enable_company_validation=False): # kalau dibutuhkan validasi ke company, ubah jadi true
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "Staging",
            "table_name": "people",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 1: Replace empty strings with NULL
        df = df.na.replace("", None)

        # Step 2: Create full_name from first_name + last_name
        df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

        # Step 3: Extract prefix and ID
        df = df.withColumn("people_entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("people_object_id", extract_id(col("object_id")))

        log_to_db({
            "step": "Format Data",
            "status": f"SUCCESS ({df.count()} rows)",
            "source": "staging",
            "table_name": "people",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 4: Mapping target column
        df_transformed = df.select(
            col("people_id").alias("people_id"),
            col("people_entity_type"),
            col("people_object_id"),
            col("full_name"),
            col("birthplace"),
            col("affiliation_name")
        )

        # Step 5: Cleaning
        df_transformed = df_transformed.withColumn("people_object_id", clean_integer(col("people_object_id")))
        df_transformed = df_transformed.withColumn("full_name", clean_alpha_text(col("full_name")))
        df_transformed = df_transformed.withColumn("birthplace", fix_encoding(col("birthplace")))
        df_transformed = df_transformed.withColumn("affiliation_name", clean_alpha_text(col("affiliation_name")))

        log_to_db({
            "step": "Map Data",
            "status": f"SUCCESS ({df_transformed.count()} rows)",
            "source": "staging",
            "table_name": "people",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 6: Fillna & dedup
        df_transformed = df_transformed.fillna({
            "full_name": "Unknown",
            "birthplace": "Unknown",
            "affiliation_name": "Unknown"
        })
        df_transformed = df_transformed.dropDuplicates(["people_entity_type", "people_object_id"])
        df_transformed = df_transformed.filter(col("full_name") != "Unknown")

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "people",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 7: OPTIONAL validation against dim_company
        if enable_company_validation and df_company is not None:
            df_company_ids = df_company.select(col("company_object_id"))

            df_valid = df_transformed.join(
                broadcast(df_company_ids),
                on=col("people_object_id") == col("company_object_id"),
                how="inner"
            ).drop("company_object_id")

            df_invalid = df_transformed.join(
                broadcast(df_company_ids),
                on=col("people_object_id") == col("company_object_id"),
                how="left_anti"
            )

            if df_invalid.count() > 0:
                invalid_ids = df_invalid.select("people_entity_type", "people_object_id").toPandas().values.tolist()
                save_invalid_ids(invalid_ids, table_name="people")

                log_to_db({
                    "step": "Validation",
                    "status": f"{df_invalid.count()} rows missing object_id",
                    "source": "staging",
                    "table_name": "people",
                    "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            return df_valid
        else:
            return df_transformed

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "people",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
