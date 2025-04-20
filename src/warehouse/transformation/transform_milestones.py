from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from src.warehouse.transformation.transform_company import transform_company


def transform_milestones(df, transformed_company):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "milestones",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 1: Replace "" to null
        df = df.na.replace("", None)
        df = df.na.replace("NaN", None)
        
        # Step 2: Format data type
        df = df.withColumn("milestone_date", to_date(col("milestone_at")))

        # Step 3: Extract prefix and ID dari object_id
        df = df.withColumn("entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("object_id", extract_id(col("object_id")))
        
        log_to_db({
            "step": "Format Data",
            "status": "SUCCESS",
            "source": "staging",
            "table_name": "milestones",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 4: Mapping to target column
        df_transformed = df.select(
            col("milestone_id").alias("milestone_id"),
            col("entity_type").alias("milestone_entity_type"),
            col("object_id").alias("milestone_object_id"),
            col("milestone_date").alias("milestone_date"),
            col("description").alias("description"),
            col("source_url").alias("source_url"),
            col("source_description").alias("source_description"),
            col("created_at").alias("created_at"),
            col("updated_at").alias("updated_at")
        )
            
        # Step 5: Handle strange values
        df_transformed = df_transformed.withColumn("milestone_object_id", clean_integer(col("milestone_object_id")))
        df_transformed = df_transformed.withColumn("description", clean_alpha_text("description"))
        df_transformed = df_transformed.withColumn("source_url", when(col("source_url").rlike(r"^(http|https)://.*"), col("source_url")).otherwise("Unknown"))
        df_transformed = df_transformed.withColumn("source_description", clean_alpha_text("source_description"))

        log_to_db({
            "step": "Map Data",
            "status": f"SUCCESS ({df_transformed.count()} rows)",
            "source": "staging",
            "table_name": "milestones",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 6: Cleaning data
        df_transformed = df_transformed.fillna({
            "source_url": "Unknown",
            "description": "No Description",
            "source_description": "Unknown"
        })
        
        # Step 7: Drop duplicate data 
        df_transformed = df_transformed.dropDuplicates(["milestone_id"])
        df_transformed = df_transformed.dropDuplicates(["milestone_entity_type", "milestone_object_id"])

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "milestones",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Step 8: Validation object_id in company
        # Take object_id from transform_company
        df_company_ids = transformed_company.select(col("company_object_id"))

        # Convert object_id to integer (if needed)
        df_transformed = df_transformed.withColumn("milestone_object_id", col("milestone_object_id").cast("int"))
        
        # Filter NULL object_id explicitly
        df_transformed = df_transformed.filter(col("milestone_object_id").isNotNull())
        
        # Valid data (match with `transformed_company`)
        df_valid = df_transformed.join(
            broadcast(df_company_ids),
            on=col("milestone_object_id") == col("company_object_id"),
            how="inner"
        ).drop("company_object_id")
        
        # Invalid data
        df_invalid = df_transformed.join(
            broadcast(df_company_ids),
            on=col("milestone_object_id") == col("company_object_id"),
            how="left_anti"
        )

        if df_invalid.count() > 0:
            invalid_ids = df_invalid.select("milestone_entity_type", "milestone_object_id").toPandas().values.tolist()
            save_invalid_ids(invalid_ids, table_name="milestones")
        
            log_to_db({
                "step": "Validation",
                "status": f"{df_invalid.count()} rows missing object_id",
                "source": "staging",
                "table_name": "milestones",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        print("The data is successfully transformed")

        return df_valid


    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "milestones",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
