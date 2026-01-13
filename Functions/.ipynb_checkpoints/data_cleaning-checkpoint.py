"""
Data Cleaning Module
Handles data cleaning and preprocessing operations
"""
from pyspark.sql.functions import (
    col, when, round as _round, sum as _sum
)
from pyspark.sql.types import IntegerType, FloatType, DateType

class TMDBDataCleaner:
    """Class to handle data cleaning operations"""
    
    def __init__(self, df):
        """Initialize with Spark DataFrame"""
        self.df = df
    
    def convert_datatypes(self):
        """Convert columns to appropriate datatypes"""
        print("\n🔧 Converting datatypes...")
        
        self.df = self.df.withColumn("budget", col("budget").cast(IntegerType())) \
                   .withColumn("revenue", col("revenue").cast(IntegerType())) \
                   .withColumn("runtime", col("runtime").cast(IntegerType())) \
                   .withColumn("vote_count", col("vote_count").cast(IntegerType())) \
                   .withColumn("vote_average", col("vote_average").cast(FloatType())) \
                   .withColumn("popularity", col("popularity").cast(FloatType())) \
                   .withColumn("id", col("id").cast(IntegerType()))
        
        # Convert release_date to date type
        self.df = self.df.withColumn("release_date", 
                           when(col("release_date").isNotNull(), 
                                col("release_date").cast(DateType()))
                           .otherwise(None))
        
        return self
    
    def handle_unrealistic_values(self):
        """Replace unrealistic values with NaN"""
        print("🔧 Handling unrealistic values...")
        
        # Replace 0 values with NaN for budget, revenue, runtime
        self.df = self.df.withColumn("budget", 
                            when(col("budget") == 0, None).otherwise(col("budget"))) \
                   .withColumn("revenue", 
                            when(col("revenue") == 0, None).otherwise(col("revenue"))) \
                   .withColumn("runtime", 
                            when(col("runtime") == 0, None).otherwise(col("runtime")))
        
        return self
    
    def convert_to_millions(self):
        """Convert budget and revenue to millions USD"""
        print("🔧 Converting to millions USD...")
        
        self.df = self.df.withColumn("budget_musd", _round(col("budget") / 1000000, 2)) \
                   .withColumn("revenue_musd", _round(col("revenue") / 1000000, 2))
        
        return self
    
    def handle_vote_data(self):
        """Handle vote_average for movies with vote_count = 0"""
        print("🔧 Handling vote data...")
        
        self.df = self.df.withColumn("vote_average", 
                           when((col("vote_count") == 0) | (col("vote_count").isNull()), None)
                           .otherwise(col("vote_average")))
        
        return self
    
    def handle_text_placeholders(self):
        """Replace placeholders in text fields"""
        print("🔧 Handling text placeholders...")
        
        self.df = self.df.withColumn("overview", 
                           when(col("overview").isin(["No Data", "", "N/A"]), None)
                           .otherwise(col("overview"))) \
                   .withColumn("tagline", 
                           when(col("tagline").isin(["No Data", "", "N/A"]), None)
                           .otherwise(col("tagline")))
        
        return self
    
    def remove_duplicates(self):
        """Remove duplicate rows"""
        print("🔧 Removing duplicates...")
        
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates(["id"])
        final_count = self.df.count()
        
        print(f"   Removed {initial_count - final_count} duplicates")
        
        return self
    
    def filter_valid_rows(self):
        """Filter rows with valid id and title"""
        print("🔧 Filtering valid rows...")
        
        # Drop rows with unknown id or title
        self.df = self.df.filter(col("id").isNotNull() & col("title").isNotNull())
        
        # Keep only rows with at least 10 non-null values
        column_count = len(self.df.columns)
        non_null_count = _sum([when(col(c).isNotNull(), 1).otherwise(0) for c in self.df.columns])
        self.df = self.df.withColumn("non_null_count", non_null_count)
        self.df = self.df.filter(col("non_null_count") >= 10).drop("non_null_count")
        
        return self
    
    def filter_released_movies(self):
        """Filter only released movies"""
        print("🔧 Filtering released movies...")
        
        self.df = self.df.filter(col("status") == "Released").drop("status")
        
        return self
    
    def reorder_columns(self):
        """Reorder columns to final structure"""
        print("🔧 Reordering columns...")
        
        final_columns = ['id', 'title', 'tagline', 'release_date', 'genres', 
                        'belongs_to_collection', 'original_language', 'budget_musd', 
                        'revenue_musd', 'production_companies', 'production_countries', 
                        'vote_count', 'vote_average', 'popularity', 'runtime', 'overview', 
                        'spoken_languages', 'poster_path', 'cast', 'cast_size', 
                        'director', 'crew_size']
        
        self.df = self.df.select(final_columns)
        
        return self
    
    def clean_all(self):
        """Execute all cleaning steps"""
        print("\n" + "="*70)
        print("CLEANING DATA")
        print("="*70)
        
        self.convert_datatypes() \
            .handle_unrealistic_values() \
            .convert_to_millions() \
            .handle_vote_data() \
            .handle_text_placeholders() \
            .remove_duplicates() \
            .filter_valid_rows() \
            .filter_released_movies() \
            .reorder_columns()
        
        print(f"\n✓ Cleaning complete! Final shape: {self.df.count()} rows, {len(self.df.columns)} columns")
        
        return self.df