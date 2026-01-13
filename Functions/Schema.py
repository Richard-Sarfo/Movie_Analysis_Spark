from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType
)

def get_tmdb_raw_schema() -> StructType:
    """
    Returns the strict Spark schema for raw TMDB movie API responses.
    This schema is designed to prevent schema inference issues.
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

        # Fields kept for completeness
        StructField("adult", BooleanType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("original_title", StringType(), True)
    ])

    return RAW_SCHEMA
