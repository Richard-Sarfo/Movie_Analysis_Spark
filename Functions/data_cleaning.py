"""Data cleaning and transformation module for TMDB movie data.

This module provides PySpark UDFs (User Defined Functions) for extracting,
parsing, and cleaning nested JSON data from TMDB API responses. Functions
handle various input formats including raw JSON strings, PySpark Row objects,
and Python dictionaries.

Functions:
    extract_name: Extract a value by key from a JSON/dict structure.
    extract_data: Extract 'name' values from a list of dictionaries.
    get_director: Extract the director's name from credits data.
    get_crew_names: Extract all crew member names from credits data.
    get_cast_names: Extract all cast member names from credits data.
    separate_data: Clean column data by removing brackets and formatting.

Typical usage:
    >>> from Functions.data_cleaning import extract_name, extract_data
    >>> df = df.withColumn('collection', extract_name(col('belongs_to_collection'), lit('name')))
    >>> df = df.withColumn('genre_list', extract_data(col('genres')))
"""

import json
import logging
from pyspark.sql.functions import udf, col, regexp_replace
from pyspark.sql.types import StringType, ArrayType

# Configure logger for data cleaning module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create console handler with formatting
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create file handler for persistent logs
    file_handler = logging.FileHandler('data_cleaning.log')
    file_handler.setLevel(logging.DEBUG)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# UDF to extract value by key from JSON/dict (WITH key parameter)
@udf(StringType())
def extract_name(data, key):
    """Parse JSON/dict and extract a value by key.

    Handles multiple input formats: JSON strings, PySpark Row objects,
    and Python dictionaries. Returns the value as a JSON-encoded string
    for nested objects, or None if the key is not found or parsing fails.

    Args:
        data: The input data — a JSON string, Row object, or dict.
        key: The dictionary key whose value should be extracted.

    Returns:
        str or None: The extracted value as a JSON string, or None
        if the data is invalid, empty, or the key is missing.
    """
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
            except (TypeError, ValueError) as conv_err:
                logger.warning("Could not convert key to string: %s", conv_err)
                k = key
            value = data.get(k)
            # Return as string for nested objects/lists
            return json.dumps(value) if value is not None else None
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse JSON in extract_name: %s (data[:100]=%s)", e, str(data)[:100])
        return None
    except (TypeError, KeyError, AttributeError) as e:
        logger.warning("Data extraction error in extract_name: %s", e)
        return None

    return None

# UDF to extract 'name' values from a list of dictionaries
@udf(ArrayType(StringType()))
def extract_data(data):
    """Extract 'name' values from a list of dictionaries.

    Parses a JSON string or list of dicts/Row objects and collects
    the 'name' field from each element.

    Args:
        data: A JSON string representing a list, a Python list of dicts,
              or a list of PySpark Row objects.

    Returns:
        list[str] or None: A list of name strings, or None if the
        input is None or cannot be parsed.
    """
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
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse JSON in extract_data: %s", e)
        return None
    except (TypeError, KeyError, AttributeError) as e:
        logger.warning("Data extraction error in extract_data: %s", e)
        return None
    return None

# UDF to extract director name from credits
@udf(StringType())
def get_director(credits):
    """Extract the director's name from TMDB credits data.

    Searches the 'crew' list within the credits structure for a person
    whose 'job' field equals 'Director'.

    Args:
        credits: A JSON string, dict, or PySpark Row containing
                 a 'crew' list with crew member details.

    Returns:
        str or None: The director's name, or None if not found
        or the input cannot be parsed.
    """
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
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse JSON in get_director: %s", e)
        return None
    except (TypeError, KeyError, AttributeError) as e:
        logger.warning("Data extraction error in get_director: %s", e)
        return None

    return None

@udf(StringType())
def get_crew_names(credits):
    """Extract all crew member names from TMDB credits data.

    Iterates through the 'crew' list and collects the 'name' field
    from each crew member entry.

    Args:
        credits: A JSON string, dict, or PySpark Row containing
                 a 'crew' list with crew member details.

    Returns:
        list[str]: A list of crew member names, or an empty list
        if the input is None or cannot be parsed.
    """
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
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse JSON in get_crew_names: %s", e)
        return []
    except (TypeError, KeyError, AttributeError) as e:
        logger.warning("Data extraction error in get_crew_names: %s", e)
        return []

    return []
@udf(StringType())
def get_cast_names(credits):
    """Extract all cast member names from TMDB credits data.

    Iterates through the 'cast' list and collects the 'name' field
    from each cast member entry.

    Args:
        credits: A JSON string, dict, or PySpark Row containing
                 a 'cast' list with cast member details.

    Returns:
        list[str]: A list of cast member names, or an empty list
        if the input is None or cannot be parsed.
    """
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
    except json.JSONDecodeError as e:
        logger.warning("Failed to parse JSON in get_cast_names: %s", e)
        return []
    except (TypeError, KeyError, AttributeError) as e:
        logger.warning("Data extraction error in get_cast_names: %s", e)
        return []

    return []

# Function to clean column data
def separate_data(df, column):
    """Clean a DataFrame column by removing list formatting characters.

    Strips square brackets and single quotes, and replaces commas
    with pipe characters ('|') to create a flat, delimited string.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame.
        column (str): The name of the column to clean.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with the cleaned column.

    Raises:
        ValueError: If the specified column does not exist in the DataFrame.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame. Available columns: {df.columns}")

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
