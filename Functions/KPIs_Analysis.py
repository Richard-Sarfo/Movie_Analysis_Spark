from pyspark.sql import functions as F

def rank_movies(df, column, ascending=False, n=None, min_budget=None, min_votes=None):
    """
    Rank and filter movies using PySpark operations.
    """
    temp = df
    
    if min_budget:
        temp = temp.filter(F.col('budget_musd') >= min_budget)
    
    if min_votes:
        temp = temp.filter(F.col('vote_count') >= min_votes)
    
    # Sort
    temp = temp.orderBy(F.col(column).asc() if ascending else F.col(column).desc())
    
    # Limit results
    if n:
        temp = temp.limit(n)
    
    return temp

