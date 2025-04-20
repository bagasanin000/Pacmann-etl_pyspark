import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.warehouse.extract.extract_staging import extract_from_staging
from src.warehouse.load.load_warehouse import load_to_dwh
from src.warehouse.transformation.transform_company import transform_company
from src.warehouse.transformation.transform_people import transform_people
from src.warehouse.transformation.transform_acquisition import transform_acquisition
from src.warehouse.transformation.transform_investments import transform_investments
from src.warehouse.transformation.transform_milestones import transform_milestones
from src.warehouse.transformation.transform_relationship import transform_relationship
from src.warehouse.transformation.transform_ipo import transform_ipo
from src.warehouse.transformation.transform_funding_rounds import transform_funding_rounds
from src.warehouse.transformation.transform_funds import transform_funds

def pipeline_dwh():

    # Extract All Tables from Staging
    data = extract_from_staging()
    print(f"Extracted tables: {list(data.keys())}")
    
    company = data["company"]
    people = data["people"]
    acquisition = data["acquisition"]
    funding_rounds = data["funding_rounds"]
    funds = data["funds"]
    investments = data["investments"]
    ipo = data["ipo"]
    milestones = data["milestones"]
    relationship = data["relationship"]
    
    # Transformation
    transformed_company = transform_company(company)
    transformed_people = transform_people(people)
    transformed_milestones = transform_milestones(milestones, transformed_company)
    transformed_acquisition = transform_acquisition(acquisition, transformed_company)
    transformed_funding_rounds = transform_funding_rounds(funding_rounds, transformed_company, transformed_people)
    transformed_relationship = transform_relationship(relationship, transformed_company, transformed_people)
    transformed_ipo = transform_ipo(ipo, transformed_company)
    transformed_investments = transform_investments(investments, transformed_company, transformed_people)
    transformed_funds = transform_funds(funds)

    # Load to Data Warehouse
    load_to_dwh(transformed_company, table_name="dim_company", use_upsert=True, idx_name="company_id", schema="public")
    load_to_dwh(transformed_people, table_name="dim_people", use_upsert=True, idx_name="people_id", schema="public")
    load_to_dwh(transformed_milestones, table_name="dim_milestones", use_upsert=True, idx_name="milestone_id", schema="public")
    load_to_dwh(transformed_acquisition, table_name="fact_acquisition", use_upsert=True, idx_name="acquisition_id", schema="public")
    load_to_dwh(transformed_funding_rounds, table_name="dim_funding_rounds", use_upsert=True, idx_name="funding_round_id", schema="public")
    load_to_dwh(transformed_relationship, table_name="fact_relationship", use_upsert=True, idx_name="relationship_id", schema="public")
    load_to_dwh(transformed_ipo, table_name="fact_ipo", use_upsert=True, idx_name="ipo_id", schema="public")
    load_to_dwh(transformed_investments, table_name="fact_investments", use_upsert=True, idx_name="investment_id", schema="public")
    load_to_dwh(transformed_funds, table_name="dim_funds", use_upsert=True, idx_name="fund_id", schema="public")

    print("All process has been sucessfully executed! Congratulations!")
