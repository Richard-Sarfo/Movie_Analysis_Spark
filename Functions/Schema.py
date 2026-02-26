"""PySpark schema definitions for TMDB movie API responses.

This module defines strict Spark StructType schemas for the raw JSON data
returned by the TMDB API. Using explicit schemas instead of schema inference
prevents type-mismatch errors (e.g., popularity as IntegerType vs DoubleType)
and ensures consistent downstream processing.

The schema mirrors the TMDB ``/movie/{id}?append_to_response=credits``
endpoint response structure as of API v3.

Functions:
    get_tmdb_raw_schema: Return the full StructType for raw TMDB responses.

Typical usage:
    >>> from Functions.Schema import get_tmdb_raw_schema
    >>> schema = get_tmdb_raw_schema()
    >>> df = spark.read.schema(schema).json('tmdb_movies.json')
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType
)


def get_tmdb_raw_schema() -> StructType:
    """Return the strict Spark schema for raw TMDB movie API responses.

    The schema covers all major fields returned by the TMDB
    ``/movie/{id}?append_to_response=credits`` endpoint, including
    nested structures for genres, production companies, countries,
    spoken languages, and the full cast/crew credits block.

    Using this schema with ``spark.read.schema(...)`` prevents automatic
    schema inference, which can incorrectly type numeric fields when
    values happen to be whole numbers (e.g., ``popularity`` inferred as
    IntegerType instead of DoubleType).

    Returns:
        pyspark.sql.types.StructType: A StructType object defining
        column names and data types for every field in the raw response.

    Sub-schemas:
        - **GENRE_STRUCT**: ``{id: int, name: str}``
        - **COMPANY_STRUCT**: ``{name: str, id: int, logo_path: str, origin_country: str}``
        - **COUNTRY_STRUCT**: ``{iso_3166_1: str, name: str}``
        - **LANGUAGE_STRUCT**: ``{iso_639_1: str, name: str, english_name: str}``
        - **COLLECTION_STRUCT**: ``{id: int, name: str, poster_path: str, backdrop_path: str}``
        - **CAST_STRUCT**: Full cast member info including character and order.
        - **CREW_STRUCT**: Full crew member info including department and job.
        - **CREDITS_STRUCT**: Contains ``cast`` and ``crew`` arrays.
    """

    # --- STRICT SUB-SCHEMAS ---
    GENRE_STRUCT = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    COMPANY_STRUCT = StructType([
        StructField("name", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("origin_country", StringType(), True)
    ])

    COUNTRY_STRUCT = StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])

    LANGUAGE_STRUCT = StructType([
        StructField("iso_639_1", StringType(), True),
        StructField("name", StringType(), True),
        StructField("english_name", StringType(), True)
    ])

    COLLECTION_STRUCT = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True)
    ])

    # --- CREDITS SUB-SCHEMAS ---
    CAST_STRUCT = StructType([
        StructField("adult", BooleanType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("name", StringType(), True),
        StructField("original_name", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("profile_path", StringType(), True),
        StructField("cast_id", IntegerType(), True),
        StructField("character", StringType(), True),
        StructField("credit_id", StringType(), True),
        StructField("order", IntegerType(), True)
    ])

    CREW_STRUCT = StructType([
        StructField("adult", BooleanType(), True),
        StructField("gender", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("name", StringType(), True),
        StructField("original_name", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("profile_path", StringType(), True),
        StructField("credit_id", StringType(), True),
        StructField("department", StringType(), True),
        StructField("job", StringType(), True)
    ])

    CREDITS_STRUCT = StructType([
        StructField("cast", ArrayType(CAST_STRUCT), True),
        StructField("crew", ArrayType(CREW_STRUCT), True)
    ])

    # --- MASTER RAW SCHEMA ---
    RAW_SCHEMA = StructType([
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("status", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("revenue", LongType(), True),
        StructField("budget", LongType(), True),
        StructField("runtime", LongType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", LongType(), True),
        StructField("original_language", StringType(), True),
        StructField("poster_path", StringType(), True),

        # Complex Nested Fields
        StructField("belongs_to_collection", COLLECTION_STRUCT, True),
        StructField("genres", ArrayType(GENRE_STRUCT), True),
        StructField("production_companies", ArrayType(COMPANY_STRUCT), True),
        StructField("production_countries", ArrayType(COUNTRY_STRUCT), True),
        StructField("spoken_languages", ArrayType(LANGUAGE_STRUCT), True),
        # Credits Field
        StructField("credits", ArrayType(CREDITS_STRUCT), True),

        # Fields kept for completeness
        StructField("adult", BooleanType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("original_title", StringType(), True)
    ])

    return RAW_SCHEMA