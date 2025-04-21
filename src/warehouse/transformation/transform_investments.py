from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from src.warehouse.transformation.transform_company import transform_company
from src.warehouse.transformation.transform_people import transform_people


def transform_investments(df, transformed_company, transformed_people):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "investments",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df = df.na.replace("", None).dropDuplicates(["investment_id"])

        # Extract ID & Prefix
        df = df.withColumn("investor_entity_type", extract_prefix(col("investor_object_id")))
        df = df.withColumn("investor_object_id", extract_id(col("investor_object_id")).cast("int"))
        df = df.withColumn("funded_object_id", extract_id(col("funded_object_id")).cast("int"))
        df = df.withColumn("funding_round_id", col("funding_round_id").cast("int"))

        # === VALIDASI funded_object_id (yang menerima dana)
        funded_company = df.join(
            broadcast(transformed_company.select("company_object_id")),
            df["funded_object_id"] == col("company_object_id"),
            "inner"
        ).withColumn("funded_entity_type", lit("c")) \
         .withColumn("funded_object_id", col("company_object_id")) \
         .drop("company_object_id")  
        
        funded_people = df.join(
            broadcast(transformed_people.select("people_object_id")),
            df["funded_object_id"] == col("people_object_id"),
            "inner"
        ).withColumn("funded_entity_type", lit("p")) \
         .withColumn("funded_object_id", col("people_object_id")) \
         .drop("people_object_id")  

        df_funded = funded_company.unionByName(funded_people)

        # === VALIDASI investor_object_id (yang memberi dana)
        investor_company = df_funded.filter(col("investor_entity_type") == "c") \
            .join(
                broadcast(transformed_company.select("company_object_id")),
                df_funded["investor_object_id"] == col("company_object_id"),
                "inner"
            ).withColumn("investor_object_id", col("company_object_id")) \
             .drop("company_object_id")  
        
        investor_people = df_funded.filter(col("investor_entity_type") == "p") \
            .join(
                broadcast(transformed_people.select("people_object_id")),
                df_funded["investor_object_id"] == col("people_object_id"),
                "inner"
            ).withColumn("investor_object_id", col("people_object_id")) \
             .drop("people_object_id")  

        df_valid = investor_company.unionByName(investor_people).dropDuplicates(["investment_id"])

        # Select relevant cols for fact table
        df_valid = df_valid.select(
            "investment_id",
            "funding_round_id",
            "funded_entity_type",
            "funded_object_id",
            "investor_entity_type",
            "investor_object_id"
        )

        log_to_db({
            "step": "Transform",
            "status": f"SUCCESS ({df_valid.count()} valid rows)",
            "source": "staging",
            "table_name": "investments",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        # === Logging ID yang invalid
        invalid_investors = df_valid.filter(col("investor_object_id").isNull())
        if invalid_investors.count() > 0:
            invalid_ids = invalid_investors.select("investor_entity_type", "investor_object_id").toPandas().values.tolist()
            save_invalid_ids(invalid_ids, "investments")

        return df_valid

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "investments",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
