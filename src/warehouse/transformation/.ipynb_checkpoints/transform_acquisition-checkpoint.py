from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from pyspark.sql.types import IntegerType, StringType
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from src.warehouse.transformation.transform_company import transform_company


def transform_acquisition(df, transformed_company):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "acquisition",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Clean & basic transform
        df = df.na.replace("", None)
        df = df.withColumn("price_amount", col("price_amount").cast("decimal(15,2)"))
        df = df.withColumn("acquired_at", to_date(col("acquired_at")))
        df = df.withColumn("acquiring_entity_type", extract_prefix(col("acquiring_object_id")))
        df = df.withColumn("acquired_entity_type", extract_prefix(col("acquired_object_id")))
        df = df.withColumn("acquiring_object_id", extract_id(col("acquiring_object_id")).cast(IntegerType()))
        df = df.withColumn("acquired_object_id", extract_id(col("acquired_object_id")).cast(IntegerType()))

        # Select relevant columns
        df_transformed = df.select(
            "acquisition_id", "acquiring_entity_type", "acquiring_object_id",
            "acquired_entity_type", "acquired_object_id", "price_amount",
            "price_currency_code", "acquired_at", "source_url", "created_at", "updated_at"
        ).fillna({
            "price_amount": 0.0,
            "price_currency_code": "Unknown",
            "source_url": "Unknown"
        }).dropDuplicates(["acquisition_id"]) \
         .filter(col("price_amount") != 0.0) \
         .na.drop(subset=["acquired_at"])

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "acquisition",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # Validate object_id against dim_company
        df_company_ids = transformed_company.select("company_object_id", "company_id")

        df_validated = (
            df_transformed
            .join(broadcast(df_company_ids.withColumnRenamed("company_object_id", "acq_obj_id")
                           .withColumnRenamed("company_id", "acq_id")),
                  col("acquiring_object_id") == col("acq_obj_id"), "left")
            .join(broadcast(df_company_ids.withColumnRenamed("company_object_id", "acqed_obj_id")
                           .withColumnRenamed("company_id", "acqed_id")),
                  col("acquired_object_id") == col("acqed_obj_id"), "left")
        )
        
        df_valid = (
            df_validated
            .filter(col("acq_obj_id").isNotNull() & col("acqed_obj_id").isNotNull())
            .select(
                "acquisition_id",
                "acquiring_entity_type",
                "acquired_entity_type",
                "price_amount",
                "price_currency_code",
                "acquired_at",
                "source_url",
                "created_at",
                "updated_at",
                col("acq_obj_id").alias("acquiring_object_id"),
                col("acqed_obj_id").alias("acquired_object_id")
            )
        )

        # Invalid records
        df_invalid = df_validated.filter(
            col("acq_obj_id").isNull() | col("acqed_obj_id").isNull()
        )


        if df_invalid.count() > 0:
            invalid_ids = df_invalid.select(
                "acquiring_entity_type", "acquiring_object_id",
                "acquired_entity_type", "acquired_object_id"
            ).toPandas().values.tolist()

            save_invalid_ids(invalid_ids, table_name="acquisition")

            log_to_db({
                "step": "Validation",
                "status": f"{df_invalid.count()} rows with missing object_id in dim_company",
                "source": "staging",
                "table_name": "acquisition",
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        print("The data is successfully transformed")
        return df_valid

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "acquisition",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
