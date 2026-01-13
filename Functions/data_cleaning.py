import json
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import regexp_replace

# UDF to extract 'name' values from a list of dictionaries
@udf(ArrayType(StringType()))
def extract_data(data):
    """Extract 'name' values from a list of dictionaries."""
    if data is None:
        return None
    try:
        # If data is string, parse it
        if isinstance(data, str):
            data = json.loads(data)
        # Extract names from list of dicts
        if isinstance(data, list):
            return [d.get("name") for d in data if isinstance(d, dict) and "name" in d]
    except:
        return None
    return None

# UDF to extract value by key from JSON/dict
@udf(StringType())
def extract_name(data, key='name'):
    """Parse JSON/dict and extract value by key, returning None if invalid."""
    if data is None or data == '':
        return None
    
    try:
        # If data is a string, parse it as JSON
        if isinstance(data, str):
            data = json.loads(data)
        
        # If data is a dict, get the specified key
        if isinstance(data, dict):
            return data.get(key)
    except:
        return None
    
    return None

# UDF to extract director name from credits
@udf(StringType())
def get_director(credits):
    """Return the director's name from credits dict, or None if not found."""
    if credits is None:
        return None
    
    try:
        # Parse if string
        if isinstance(credits, str):
            credits = json.loads(credits)
        
        # Get crew list
        crew = credits.get('crew', []) if isinstance(credits, dict) else []
        
        # Find director
        for person in crew:
            if isinstance(person, dict) and person.get('job') == 'Director':
                return person.get('name')
    except:
        return None
    
    return None

# Function to clean column data
def separate_data(df, column):
    """Clean column by removing brackets/quotes and replacing commas with pipes."""    
    df = df.withColumn(
        column,
        regexp_replace(col(column).cast("string"), r"\[", "")
    )
    df = df.withColumn(
        column,
        regexp_replace(col(column), r"\]", "")
    )
    df = df.withColumn(
        column,
        regexp_replace(col(column), ",", "|")
    )
    df = df.withColumn(
        column,
        regexp_replace(col(column), "'", "")
    )
    
    return df
