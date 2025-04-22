from pyspark.sql import SparkSession
from datetime import datetime
from src.utils.logger import log_to_db, save_invalid_ids
from src.utils.helper import clean_integer, clean_text, normalize_text, clean_alpha_text, fix_encoding, extract_prefix, extract_id, extract_stock_market, extract_stock_symbol 
from pyspark.sql.functions import col, to_date, broadcast, when, lit
from pyspark.sql.types import IntegerType, StringType
from src.warehouse.transformation.transform_company import transform_company
from src.warehouse.transformation.transform_people import transform_people


def transform_funding_rounds(df, transformed_company, transformed_people):
    try:
        log_to_db({
            "step": "Transform",
            "status": "STARTED",
            "source": "staging",
            "table_name": "funding_rounds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df = df.na.replace("", None)
        df = df.withColumn("funding_entity_type", extract_prefix(col("object_id")))
        df = df.withColumn("object_id", extract_id(col("object_id")).cast(IntegerType()))
        df = df.withColumn("funding_date", to_date(col("funded_at")))
        df = df.withColumn("participants", col("participants").cast(IntegerType()))
        df = df.withColumn("funding_entity_type", col("funding_entity_type").cast(StringType()))

        log_to_db({
            "step": "Format Data",
            "status": f"SUCCESS ({df.count()} rows)",
            "source": "staging",
            "table_name": "funding_rounds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df_transformed = df.select(
            col("funding_round_id"),
            col("funding_entity_type"),
            col("object_id").alias("funding_object_id"),
            col("funding_round_type").alias("round_type"),
            col("funding_date"),
            col("raised_currency_code").alias("raised_currency"),
            col("raised_amount"),
            col("raised_amount_usd"),
            col("pre_money_currency_code").alias("pre_money_currency"),
            col("pre_money_valuation"),
            col("pre_money_valuation_usd"),
            col("post_money_currency_code").alias("post_money_currency"),
            col("post_money_valuation"),
            col("post_money_valuation_usd"),
            col("participants"),
            col("source_url"),
            col("source_description"),
            col("created_at"),
            col("updated_at")
        )

        df_transformed = df_transformed.fillna({
            "round_type": "Unknown",
            "raised_currency": "USD",
            "pre_money_currency": "USD",
            "post_money_currency": "USD",
            "raised_amount_usd": 0.0,
            "source_description": "Unknown",
            "source_url": "Unknown"
        }).dropDuplicates(["funding_round_id"]).filter(col("round_type") != "Unknown")

        df_transformed = df_transformed.withColumn("funding_entity_type", when(col("funding_entity_type").isin("c", "p"), col("funding_entity_type")).otherwise("c"))

        df_company = df_transformed.filter(col("funding_entity_type") == "c") \
            .join(broadcast(transformed_company.select("company_object_id")), col("funding_object_id") == col("company_object_id"), "inner") \
            .drop("company_object_id")

        df_people = df_transformed.filter(col("funding_entity_type") == "p") \
            .join(broadcast(transformed_people.select("people_object_id")), col("funding_object_id") == col("people_object_id"), "inner") \
            .drop("people_object_id")

        log_to_db({
            "step": "Clean Data",
            "status": f"SUCCESS ({df_transformed.count()} rows after cleansing)",
            "source": "staging",
            "table_name": "funding_rounds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        df_valid = df_company.unionByName(df_people).dropDuplicates(["funding_round_id"])

        log_to_db({
            "step": "Validation",
            "status": f"SUCCESS ({df_valid.count()} rows after validation)",
            "source": "staging",
            "table_name": "funding_rounds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        return df_valid

    except Exception as e:
        log_to_db({
            "step": "Transform",
            "status": f"FAILED - {str(e)}",
            "source": "staging",
            "table_name": "funding_rounds",
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        raise
