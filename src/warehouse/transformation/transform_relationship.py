from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, to_timestamp, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from src.warehouse.transformation.transform_company import transform_company
from src.warehouse.transformation.transform_people import transform_people


def transform_relationship(df, transformed_company, transformed_people):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "relationship",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # === Step 1: Clean and format ===
        df = df.na.replace("", None)
        df = df.withColumn("start_at", to_date(col("start_at")))
        df = df.withColumn("end_at", to_date(col("end_at")))
        df = df.withColumn("created_at", to_timestamp(col("created_at")))
        df = df.withColumn("updated_at", to_timestamp(col("updated_at")))
        df = df.withColumn("people_entity_type", extract_prefix(col("person_object_id")))
        df = df.withColumn("relationship_entity_type", extract_prefix(col("relationship_object_id")))
        df = df.withColumn("people_object_id", extract_id(col("person_object_id")).cast("int"))
        df = df.withColumn("relationship_object_id", extract_id(col("relationship_object_id")).cast("int"))
        df = df.withColumn("title", normalize_text(col("title")))

        # === Step 2: Select & clean ===
        df_transformed = df.select(
            "relationship_id", "people_entity_type", "people_object_id",
            "relationship_entity_type", "relationship_object_id",
            "start_at", "end_at", "title", "created_at", "updated_at"
        ).fillna({"title": "Unknown"}).dropDuplicates(["relationship_id"])

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "relationship",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # === Step 3: Validate FK to people & company ===
        df_people_ids = transformed_people.select("people_object_id").distinct()
        df_company_ids = transformed_company.select("company_object_id").distinct()

        df_valid = (
            df_transformed
            .join(broadcast(df_people_ids), "people_object_id", "inner")
            .join(broadcast(df_company_ids), df_transformed["relationship_object_id"] == df_company_ids["company_object_id"], "inner")
        ).drop("company_object_id")

        df_invalid = df_transformed.join(
            df_valid.select("relationship_id"),
            on="relationship_id",
            how="left_anti"
        )

        if df_invalid.count() > 0:
            invalid_ids = df_invalid.select(
                "people_entity_type", "people_object_id",
                "relationship_entity_type", "relationship_object_id"
            ).toPandas().values.tolist()
            save_invalid_ids(invalid_ids, table_name="relationship")
            log_to_db({
                "step": "Validation",
                "status": f"{df_invalid.count()} invalid rows (missing object_id)",
                "source": "staging",
                "table_name": "relationship",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        print("The data is successfully transformed")
        return df_valid

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "relationship",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
