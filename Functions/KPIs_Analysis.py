"""KPI analysis module for TMDB movie data.

This module provides functions for ranking and filtering movies based on
key performance indicators (KPIs) such as revenue, budget, ROI, and
audience ratings using PySpark operations.

Functions:
    rank_movies: Rank and filter movies by a specified column with
        optional budget and vote-count thresholds.

Typical usage:
    >>> from Functions.KPIs_Analysis import rank_movies
    >>> top_5_revenue = rank_movies(df, column='revenue_musd', n=5)
    >>> best_roi = rank_movies(df, column='roi', n=1, min_budget=10)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def rank_movies(df, column, ascending=False, n=None, min_budget=None, min_votes=None):
    """Rank and filter movies using PySpark operations.

    Applies optional filters for minimum budget and vote count, then sorts
    the resulting DataFrame by the specified column.

    Args:
        df (pyspark.sql.DataFrame): The input movie DataFrame. Must contain
            the column specified, and optionally 'budget_musd' and 'vote_count'.
        column (str): The name of the column to sort/rank by.
        ascending (bool, optional): Sort in ascending order if True,
            descending otherwise. Default: False (highest first).
        n (int, optional): Limit the output to the top *n* rows.
            If None, all rows are returned.
        min_budget (float, optional): Minimum budget (in millions USD) to
            include. Requires a 'budget_musd' column. Default: None.
        min_votes (int, optional): Minimum vote count to include. Requires
            a 'vote_count' column. Default: None.

    Returns:
        pyspark.sql.DataFrame: A filtered and sorted DataFrame.

    Raises:
        TypeError: If df is not a PySpark DataFrame.
        ValueError: If the specified column does not exist in the DataFrame.

    Example:
        >>> top_5 = rank_movies(df, column='revenue_musd', n=5)
        >>> top_5.show()
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    if column not in df.columns:
        raise ValueError(
            f"Column '{column}' not found in DataFrame. "
            f"Available columns: {df.columns}"
        )

    temp = df

    if min_budget is not None:
        if 'budget_musd' not in temp.columns:
            raise ValueError("Cannot filter by min_budget: 'budget_musd' column is missing.")
        temp = temp.filter(F.col('budget_musd') >= min_budget)

    if min_votes is not None:
        if 'vote_count' not in temp.columns:
            raise ValueError("Cannot filter by min_votes: 'vote_count' column is missing.")
        temp = temp.filter(F.col('vote_count') >= min_votes)

    # Sort
    temp = temp.orderBy(F.col(column).asc() if ascending else F.col(column).desc())

    # Limit results
    if n is not None:
        temp = temp.limit(n)

    return temp
