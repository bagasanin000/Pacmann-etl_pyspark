import os
import json
from datetime import datetime, date
from decimal import Decimal
from src.utils.logger import log_to_db

# Helper function to convert values to JSON format
def convert_to_serializable(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

def profile_data(person, df, table_name, format_file):
    try:
        n_rows = df.count()
        n_cols = len(df.columns)
        
        column_info = {}
        for col in df.columns:
            data_type = df.schema[col].dataType.simpleString()
            sample_values = df.select(col).distinct().limit(5).rdd.flatMap(lambda x: x).collect()
            null_count = df.filter(df[col].isNull()).count()
            unique_count = df.select(col).distinct().count()
            
            # Min and max values (if numeric or date type)
            try:
                min_value = df.agg({col: "min"}).collect()[0][0]
                max_value = df.agg({col: "max"}).collect()[0][0]
            except:
                min_value = None
                max_value = None
            
            # Missing value percentage
            percentage_missing = round((null_count / n_rows) * 100, 2) if n_rows > 0 else 0.0
            
            # Take 5 samples
            unique_values = df.select(col).distinct().limit(5).rdd.flatMap(lambda x: x).collect()
            
            # Valid date percentage (only for datetime column)
            percentage_valid_date = None
            if data_type in ['date', 'timestamp']:
                valid_date_count = df.filter(df[col].isNotNull()).count()
                percentage_valid_date = round((valid_date_count / n_rows) * 100, 2) if n_rows > 0 else 0.0

            column_info[col] = {
                "data_type": data_type,
                "sample_values": [convert_to_serializable(v) for v in sample_values] if sample_values else None,
                "unique_count": unique_count,
                "unique_value": [convert_to_serializable(v) for v in unique_values] if unique_values else None,
                "null_count": null_count,
                "percentage_missing_value": percentage_missing,
                "min_value": convert_to_serializable(min_value),
                "max_value": convert_to_serializable(max_value),
                "percentage_valid_date": percentage_valid_date
            }
        
        dict_profiling = {
            "created_at": datetime.now().isoformat(),
            "person_in_charge": person,
            "profiling_result": {
                "table_name": table_name,
                "format_file": format_file,
                "n_rows": n_rows,
                "n_cols": n_cols,
                "report": column_info
            }
        }
        
        # Save profiling result to JSON
        folder_path = "data_profiling"
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"{table_name}_profiling.json")
        with open(file_path, "w") as f:
            json.dump(dict_profiling, f, indent=4, default=convert_to_serializable)

        print(f"Profiling saved to: {file_path}")

        # Create success log message
        log_msg = {
            "step": "Profiling",
            "status": "Success",
            "source": format_file,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        }

    except Exception as e:
        print(f"Error profiling table {table_name}: {e}")

        # Create fail log message
        log_msg = {
            "step": "Profiling",
            "status": f"Failed: {e}",
            "source": format_file,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        }

    finally:
        # Save log to CSV
        log_to_db(log_msg)

    return dict_profiling if 'dict_profiling' in locals() else None
