"""
KPI Analysis Module
Handles KPI calculations and advanced filtering
"""
from pyspark.sql.functions import (
    col, when, desc, asc, round as _round,
    sum as _sum, mean as _mean, count as _count
)
class TMDBKPIAnalyzer:
    """Class to handle KPI analysis"""
    
    def __init__(self, df):
        """Initialize with cleaned DataFrame"""
        self.df = df
        self.prepare_metrics()
    
    def prepare_metrics(self):
        """Calculate profit and ROI"""
        self.df = self.df.withColumn("profit_musd", 
                           _round(col("revenue_musd") - col("budget_musd"), 2))
        self.df = self.df.withColumn("roi", 
                           when(col("budget_musd") > 0, 
                                _round(col("revenue_musd") / col("budget_musd"), 2))
                           .otherwise(None))
    
    def rank_movies(self, column, ascending=False, filter_condition=None, top_n=5):
        """User-Defined Function to rank movies"""
        temp_df = self.df
        
        # Apply filter if provided
        if filter_condition:
            temp_df = temp_df.filter(filter_condition)
        
        # Remove null values from ranking column
        temp_df = temp_df.filter(col(column).isNotNull())
        
        # Sort and select
        if ascending:
            result = temp_df.orderBy(asc(column)).limit(top_n)
        else:
            result = temp_df.orderBy(desc(column)).limit(top_n)
        
        return result.select("title", column)
    
    def analyze_best_worst_movies(self):
        """Analyze best and worst performing movies"""
        print("\n" + "="*70)
        print("KPI ANALYSIS: BEST/WORST PERFORMING MOVIES")
        print("="*70)
        
        results = {}
        
        # Highest Revenue
        print("Top 5 Highest Revenue Movies:")
        results['highest_revenue'] = self.rank_movies("revenue_musd", ascending=False)
        results['highest_revenue'].show()
        
        # Highest Budget
        print("Top 5 Highest Budget Movies:")
        results['highest_budget'] = self.rank_movies("budget_musd", ascending=False)
        results['highest_budget'].show()
        
        # Highest Profit
        print("Top 5 Highest Profit Movies:")
        results['highest_profit'] = self.rank_movies("profit_musd", ascending=False)
        results['highest_profit'].show()
        
        # Lowest Profit
        print("\n📉 Top 5 Lowest Profit Movies:")
        results['lowest_profit'] = self.rank_movies("profit_musd", ascending=True)
        results['lowest_profit'].show()
        
        # Highest ROI (Budget >= 10M)
        print("\n🎯 Top 5 Highest ROI Movies (Budget ≥ $10M):")
        results['highest_roi'] = self.rank_movies("roi", ascending=False, 
                    filter_condition=(col("budget_musd") >= 10))
        results['highest_roi'].show()
        
        # Lowest ROI (Budget >= 10M)
        print("\n⚠️  Top 5 Lowest ROI Movies (Budget ≥ $10M):")
        results['lowest_roi'] = self.rank_movies("roi", ascending=True, 
                    filter_condition=(col("budget_musd") >= 10))
        results['lowest_roi'].show()
        
        # Most Voted Movies
        print("\n🗳️  Top 5 Most Voted Movies:")
        results['most_voted'] = self.rank_movies("vote_count", ascending=False)
        results['most_voted'].show()
        
        # Highest Rated Movies (≥10 votes)
        print("\n⭐ Top 5 Highest Rated Movies (≥10 votes):")
        results['highest_rated'] = self.rank_movies("vote_average", ascending=False, 
                    filter_condition=(col("vote_count") >= 10))
        results['highest_rated'].show()
        
        # Lowest Rated Movies (≥10 votes)
        print("\n⭐ Top 5 Lowest Rated Movies (≥10 votes):")
        results['lowest_rated'] = self.rank_movies("vote_average", ascending=True, 
                    filter_condition=(col("vote_count") >= 10))
        results['lowest_rated'].show()
        
        # Most Popular Movies
        print("\n🔥 Top 5 Most Popular Movies:")
        results['most_popular'] = self.rank_movies("popularity", ascending=False)
        results['most_popular'].show()
        
        return results
    
    def advanced_search_queries(self):
        """Execute advanced search queries"""
        print("\n" + "="*70)
        print("ADVANCED FILTERING & SEARCH QUERIES")
        print("="*70)
        
        results = {}
        
        # Search 1: Best-rated Science Fiction Action movies starring Bruce Willis
        print("\n🔍 Search 1: Best-rated Sci-Fi Action movies with Bruce Willis:")
        results['search1'] = self.df.filter(
            (col("genres").contains("Science Fiction")) &
            (col("genres").contains("Action")) &
            (col("cast").contains("Bruce Willis"))
        ).orderBy(desc("vote_average"))
        
        results['search1'].select("title", "vote_average", "genres").show(truncate=False)
        
        # Search 2: Movies starring Uma Thurman, directed by Quentin Tarantino
        print("\n🔍 Search 2: Uma Thurman movies directed by Quentin Tarantino:")
        results['search2'] = self.df.filter(
            (col("cast").contains("Uma Thurman")) &
            (col("director").contains("Quentin Tarantino"))
        ).orderBy(asc("runtime"))
        
        results['search2'].select("title", "director", "runtime").show(truncate=False)
        
        return results
    
    def franchise_vs_standalone(self):
        """Compare franchise vs standalone movies"""
        print("\n" + "="*70)
        print("FRANCHISE VS STANDALONE PERFORMANCE")
        print("="*70)
        
        # Create franchise indicator
        df_analysis = self.df.withColumn("is_franchise", 
                                 when(col("belongs_to_collection").isNotNull(), "Franchise")
                                 .otherwise("Standalone"))
        
        # Calculate metrics
        franchise_stats = df_analysis.groupBy("is_franchise").agg(
            _mean("revenue_musd").alias("mean_revenue"),
            _mean("roi").alias("mean_roi"),
            _mean("budget_musd").alias("mean_budget"),
            _mean("popularity").alias("mean_popularity"),
            _mean("vote_average").alias("mean_rating"),
            _count("*").alias("count")
        )
        
        print("\n📊 Franchise vs Standalone Comparison:")
        franchise_stats.show()
        
        return franchise_stats
    
    def analyze_franchises(self):
        """Analyze most successful franchises"""
        print("\n" + "="*70)
        print("MOST SUCCESSFUL FRANCHISES")
        print("="*70)
        
        franchise_analysis = self.df.filter(col("belongs_to_collection").isNotNull()) \
            .groupBy("belongs_to_collection") \
            .agg(
                _count("*").alias("total_movies"),
                _sum("budget_musd").alias("total_budget"),
                _mean("budget_musd").alias("mean_budget"),
                _sum("revenue_musd").alias("total_revenue"),
                _mean("revenue_musd").alias("mean_revenue"),
                _mean("vote_average").alias("mean_rating")
            ) \
            .orderBy(desc("total_revenue"))
        
        print("\n🏆 Most Successful Franchises:")
        franchise_analysis.show(10, truncate=False)
        
        return franchise_analysis
    
    def analyze_directors(self):
        """Analyze most successful directors"""
        print("\n" + "="*70)
        print("MOST SUCCESSFUL DIRECTORS")
        print("="*70)
        
        director_analysis = self.df.filter(col("director").isNotNull()) \
            .groupBy("director") \
            .agg(
                _count("*").alias("total_movies"),
                _sum("revenue_musd").alias("total_revenue"),
                _mean("vote_average").alias("mean_rating")
            ) \
            .orderBy(desc("total_revenue"))
        
        print("\n🎬 Most Successful Directors:")
        director_analysis.show(10, truncate=False)
        
        return director_analysis
    
    def run_all_analysis(self):
        """Run all KPI analyses"""
        results = {
            'best_worst': self.analyze_best_worst_movies(),
            'search_queries': self.advanced_search_queries(),
            'franchise_comparison': self.franchise_vs_standalone(),
            'franchise_analysis': self.analyze_franchises(),
            'director_analysis': self.analyze_directors()
        }
        
        return results
