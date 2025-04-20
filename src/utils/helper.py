from pyspark.sql.functions import col, lit, to_date, when, udf
from pyspark.sql.types import IntegerType, StringType
import re
from unidecode import unidecode
import unicodedata


# Cleaning Values to Make it More Readable
@udf(returnType=IntegerType())
def clean_integer(value):
    if isinstance(value, str):
        match = re.match(r"^[a-zA-Z]:(\d+)", value) 
        if match:
            return int(match.group(1))  # Catch value after ":"
        else:
            return None 
    return value
    
@udf(returnType=StringType())
def clean_text(value):
    if value:
        try:
            # Handle encoding issue 
            value = value.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        # Normalization
        value = unicodedata.normalize("NFKD", value)
        # Handle strange character
        value = re.sub(r'[^\x00-\x7F]+', '', value)
        value = value.strip()
        value = unidecode(value)
    return value

@udf(returnType=StringType())
def normalize_text(value):
    if not isinstance(value, str) or not value.strip():
        return None  
    
    # Make it lowercase
    value = value.lower()
    
    # HDelete strange char
    value = re.sub(r'[^\w\s,&/]', '', value)  # Alphanumeric, space, coma, apersand, slash
    
    value = re.sub(r'[/,&]', ' ', value) 
    
    # Delete exaggerated space
    value = re.sub(r'\s+', ' ', value).strip()  
    return value

@udf(returnType=StringType())
def clean_alpha_text(text):
    if text:
        # Delete all strange char, except alphanumeric and space
        return re.sub(r'[^\w\s]', '', text).strip()
    return None

@udf(returnType=StringType())
def fix_encoding(s):
    if s is not None:
        try:
            return unidecode(s)
        except Exception as e:
            return None
    return s


# For Extracting Prefix and Numeric ID
@udf(returnType=StringType())
def extract_prefix(value):
    if value and ":" in value:
        return value.split(":")[0]
    return None

@udf(returnType=IntegerType())
def extract_id(value):
    if value and ":" in value:
        try:
            return int(value.split(":")[1])
        except ValueError:
            return None
    return None

# For Handle Stock-related Column
@udf(returnType=StringType())
def extract_stock_market(value):
    if value and ":" in value:
        return value.split(":")[0]
    return None

@udf(returnType=StringType())
def extract_stock_symbol(value):
    if value and ":" in value:
        return value.split(":")[1]
    return None