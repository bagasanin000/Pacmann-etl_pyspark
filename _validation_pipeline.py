from src.profiling.profiling import convert_to_serializable, profile_data
from src.warehouse.extract.extract_staging import extract_from_staging

def data_profiling(): 
    data = extract_from_staging()
    print(f"Extracted tables: {list(data.keys())}")
    
    # Read All Data from Staging
    acquisition = data["acquisition"]
    company = data["company"]
    funding_rounds = data["funding_rounds"]
    funds = data["funds"]
    investments = data["investments"]
    ipos = data["ipo"]
    milestones = data["milestones"]
    people = data["people"]
    relationship = data["relationship"]
    
    # Profiling All Data from Staging
    profile_data("Mrs. OP", company, "company_data", "from Staging")
    profile_data("Mr. A", people, "people_data", "from Staging")
    profile_data("Mr. CCC", relationship, "relationship_data", "from Staging")
    profile_data("Mrs. H", acquisition, "acquisition_data", "from Staging")
    profile_data("Mr. CCC", funding_rounds, "funding_rounds_data", "from Staging")
    profile_data("Mr. A", funds, "funds_data", "from Staging")
    profile_data("Mrs. H", investments, "investments_data", "from Staging")
    profile_data("Mr. A", ipos, "ipos_data", "from Staging")
    profile_data("Mrs. OP", milestones, "milestones_data", "from Staging")
