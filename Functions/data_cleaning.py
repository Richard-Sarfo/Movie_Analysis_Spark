import json
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import StringType, ArrayType

# UDF to extract value by key from JSON/dict (WITH key parameter)
@udf(StringType())
def extract_name(data, key):
    """Parse JSON/dict and extract value by key, returning None if invalid."""
    if data is None or data == '':
        return None
    
    try:
        # If data is a string, parse it as JSON
        if isinstance(data, str):
            data = json.loads(data)
        
        # If data is a Row-like object, convert to dict
        if hasattr(data, 'asDict'):
            data = data.asDict()

        # If data is a dict, get the specified key
        if isinstance(data, dict):
            # key may be passed as bytes/other; ensure it's a string
            try:
                k = key if isinstance(key, str) else str(key)
            except:
                k = key
            value = data.get(k)
            # Return as string for nested objects/lists
            return json.dumps(value) if value is not None else None
    except:
        return None
    
    return None

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
            names = []
            for d in data:
                # If element is Row-like, convert to dict
                if hasattr(d, 'asDict'):
                    d = d.asDict()
                if isinstance(d, dict) and "name" in d:
                    names.append(d.get("name"))
            return names
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
        
        # If credits is Row-like, convert to dict
        if hasattr(credits, 'asDict'):
            credits = credits.asDict()

        # Get crew list
        crew = credits.get('crew', []) if isinstance(credits, dict) else []

        # Find director
        for person in crew:
            # Convert Row-like person to dict
            if hasattr(person, 'asDict'):
                person = person.asDict()
            if isinstance(person, dict) and person.get('job') == 'Director':
                return person.get('name')
    except:
        return None
    
    return None

@udf(StringType())
def get_crew_names(credits):
    """Return a list of all crew members' names from credits dict, or empty list if not found."""
    if credits is None:
        return []
    
    try:
        # Parse if string
        if isinstance(credits, str):
            credits = json.loads(credits)
        
        # If credits is Row-like, convert to dict
        if hasattr(credits, 'asDict'):
            credits = credits.asDict()

        # Get crew list
        crew = credits.get('crew', []) if isinstance(credits, dict) else []

        # Extract all crew names
        crew_names = []
        for person in crew:
            # Convert Row-like person to dict
            if hasattr(person, 'asDict'):
                person = person.asDict()
            if isinstance(person, dict) and person.get('name'):
                crew_names.append(person.get('name'))
        
        return crew_names
    except:
        return []
    
    return []
@udf(StringType())
def get_cast_names(credits):
    """Return a list of all cast members' names from credits dict, or empty list if not found."""
    if credits is None:
        return []
    
    try:
        # Parse if string
        if isinstance(credits, str):
            credits = json.loads(credits)
        
        # If credits is Row-like, convert to dict
        if hasattr(credits, 'asDict'):
            credits = credits.asDict()

        # Get cast list
        cast = credits.get('cast', []) if isinstance(credits, dict) else []

        # Extract all cast names
        cast_names = []
        for person in cast:
            # Convert Row-like person to dict
            if hasattr(person, 'asDict'):
                person = person.asDict()
            if isinstance(person, dict) and person.get('name'):
                cast_names.append(person.get('name'))
        
        return cast_names
    except:
        return []
    
    return []

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
