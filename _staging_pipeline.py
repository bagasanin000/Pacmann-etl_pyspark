import pyspark
from pyspark.sql import SparkSession
from src.staging.extract.extract_db_source import extract_from_db
from src.staging.extract.extract_csv import extract_csv
from src.staging.extract.extract_api import extract_api
from src.staging.load.load_staging import load_staging2
from pyspark.sql.functions import current_timestamp

def pipeline_staging():    
    # Extract data from CSV
    df_people = extract_csv("data/people.csv", "people_data")
    df_relations = extract_csv("data/relationships.csv", "relationships_data")

    # Extract data from database (source)
    tables = extract_from_db()
    print(f"Extracted tables: {list(tables.keys())}")
    
    df_acquisition = tables["acquisition"]
    df_company = tables["company"]
    df_funding_rounds = tables["funding_rounds"]
    df_funds = tables["funds"]
    df_investments = tables["investments"]
    df_ipos = tables["ipos"]

    # Extract data from API
    link_api = "https://api-milestones.vercel.app/api/data"
    list_parameter = {
        "start_date": "2008-01-01",
        "end_date": "2010-12-31"
    }
    
    df_milestones = extract_api(link_api, list_parameter, "milestones")
    
    # Load data to staging 
    load_staging2(df_relations, "relationship")
    load_staging2(df_people, "people", mode="overwrite")

    load_staging2(df_acquisition, "acquisition") 
    load_staging2(df_funding_rounds, "funding_rounds") 
    load_staging2(df_funds, "funds")
    load_staging2(df_investments, "investments")
    load_staging2(df_ipos, "ipo")
    load_staging2(df_company, "company") 

    load_staging2(df_milestones, "milestones")
    