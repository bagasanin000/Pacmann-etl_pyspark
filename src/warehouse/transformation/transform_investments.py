from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from src.warehouse.transformation.transform_company import transform_company
from src.warehouse.transformation.transform_people import transform_people
from src.warehouse.transformation.transform_funding_rounds import transform_funding_rounds


def transform_investments(df, transformed_company, transformed_people, transformed_funding_rounds):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "investments",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df = df.na.replace("", None).dropDuplicates(["investment_id"])
        df = df.withColumn("investor_entity_type", extract_prefix(col("investor_object_id")))
        df = df.withColumn("investor_object_id", extract_id(col("investor_object_id")).cast("int"))
        df = df.withColumn("funded_object_id", extract_id(col("funded_object_id")).cast("int"))
        df = df.withColumn("funding_round_id", col("funding_round_id").cast("int"))

        # === VALIDASI funded_object_id (yang menerima dana)
        funded_company = df.join(broadcast(transformed_company.select("company_object_id")), df["funded_object_id"] == col("company_object_id"), "inner") \
            .withColumn("funded_entity_type", lit("c")).drop("company_object_id")

        funded_people = df.join(broadcast(transformed_people.select("people_object_id")), df["funded_object_id"] == col("people_object_id"), "inner") \
            .withColumn("funded_entity_type", lit("p")).drop("people_object_id")

        df_funded = funded_company.unionByName(funded_people)

        # === VALIDASI investor_object_id (yang memberi dana)
        investor_company = df_funded.filter(col("investor_entity_type") == "c") \
            .join(broadcast(transformed_company.select("company_object_id")), df_funded["investor_object_id"] == col("company_object_id"), "inner") \
            .drop("company_object_id")

        investor_people = df_funded.filter(col("investor_entity_type") == "p") \
            .join(broadcast(transformed_people.select("people_object_id")), df_funded["investor_object_id"] == col("people_object_id"), "inner") \
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
        
        valid_funding_ids = transformed_funding_rounds.select("funding_round_id").distinct()
        
        df_valid = df_valid.join(
            broadcast(valid_funding_ids),
            on="funding_round_id",
            how="inner"
        )
    

        log_to_db({
            "step": "Transform",
            "status": f"SUCCESS ({df_valid.count()} valid rows)",
            "source": "staging",
            "table_name": "investments",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

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
