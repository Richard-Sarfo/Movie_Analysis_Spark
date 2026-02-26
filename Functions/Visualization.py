"""Visualization module for TMDB movie analysis.

This module generates publication-quality plots for exploring and presenting
key metrics from the TMDB movie dataset. All functions accept PySpark
DataFrames and convert to Pandas internally for plotting.

Functions:
    plot_revenue_vs_budget: Scatter plot of revenue against budget.
    plot_roi_by_genre: Box plot showing ROI distribution by primary genre.
    plot_popularity_vs_rating: Scatter plot of popularity vs. audience rating.
    plot_yearly_trends: Line plot of average revenue and budget by year.
    plot_franchise_vs_standalone_revenue: Bar chart comparing franchise
        and standalone movie performance.

Typical usage:
    >>> from Functions.Visualization import plot_revenue_vs_budget
    >>> plot_revenue_vs_budget(df)
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def plot_revenue_vs_budget(df):
    """Create a scatter plot of revenue versus budget.

    Args:
        df (pyspark.sql.DataFrame): Must contain 'budget_musd' and
            'revenue_musd' columns (values in millions USD).

    Raises:
        TypeError: If df is not a PySpark DataFrame.
        ValueError: If required columns are missing.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    for col_name in ('budget_musd', 'revenue_musd'):
        if col_name not in df.columns:
            raise ValueError(f"Required column '{col_name}' not found in DataFrame.")

    # Convert Spark DataFrame to Pandas for plotting
    pdf = df.select("budget_musd", "revenue_musd").toPandas()

    plt.figure(figsize=(8, 4))
    plt.scatter(
        pdf['budget_musd'],
        pdf['revenue_musd'],
        alpha=0.5,
        color='blue',
        label='Movies'
    )
    plt.xlabel('Budget (Million USD)')
    plt.ylabel('Revenue (Million USD)')
    plt.title('Revenue vs Budget')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_roi_by_genre(df):
    """Create a horizontal box plot of ROI distribution by primary genre.

    Genres with fewer than 5 movies are excluded. Genres are sorted by
    median ROI in descending order. Sample sizes are shown on the y-axis.

    Args:
        df (pyspark.sql.DataFrame): Must contain 'genres' (pipe-delimited
            string) and 'roi' columns.

    Raises:
        TypeError: If df is not a PySpark DataFrame.
        ValueError: If required columns are missing.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    for col_name in ('genres', 'roi'):
        if col_name not in df.columns:
            raise ValueError(f"Required column '{col_name}' not found in DataFrame.")

    # Convert Spark DataFrame to Pandas
    pdf = df.toPandas()

    # Split compound genres and take only the primary (first) genre
    pdf['primary_genre'] = pdf['genres'].str.split('|').str[0].str.strip()

    # Filter out genres with too few samples (less than 5 movies)
    genre_counts = pdf['primary_genre'].value_counts()
    valid_genres = genre_counts[genre_counts >= 5].index
    pdf_filtered = pdf[pdf['primary_genre'].isin(valid_genres)]

    # Calculate median ROI for sorting
    genre_median = pdf_filtered.groupby('primary_genre')['roi'].median().sort_values(ascending=False)

    # Create horizontal box plot
    plt.figure(figsize=(12, 8))
    ax = sns.boxplot(
        y='primary_genre',
        x='roi',
        data=pdf_filtered,
        order=genre_median.index,
        palette='Set2',
        linewidth=1.5,
        fliersize=5
    )

    plt.title("ROI Distribution by Genre", fontsize=18, fontweight='bold', pad=20)
    plt.xlabel("ROI", fontsize=14, fontweight='bold')
    plt.ylabel("Genre", fontsize=14, fontweight='bold')

    # Add sample sizes to y-axis labels
    labels = [f"{genre} (n={genre_counts[genre]})" for genre in genre_median.index]
    ax.set_yticklabels(labels)

    plt.tight_layout()
    plt.show()


def plot_popularity_vs_rating(df):
    """Create a scatter plot of popularity versus audience rating.

    Args:
        df (pyspark.sql.DataFrame): Must contain 'popularity' and
            'vote_average' columns.

    Raises:
        TypeError: If df is not a PySpark DataFrame.
        ValueError: If required columns are missing.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    for col_name in ('popularity', 'vote_average'):
        if col_name not in df.columns:
            raise ValueError(f"Required column '{col_name}' not found in DataFrame.")

    pdf = df.select("popularity", "vote_average").toPandas()

    plt.figure(figsize=(8, 4))
    plt.scatter(
        pdf['popularity'],
        pdf['vote_average'],
        alpha=0.6,
        color='green',
        label='Movies'
    )
    plt.xlabel('Popularity')
    plt.ylabel('Rating')
    plt.title('Popularity vs Rating')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_yearly_trends(df):
    """Plot yearly average revenue and budget trends over time.

    Extracts the year from the 'release_date' column and computes
    the mean revenue and budget per year.

    Args:
        df (pyspark.sql.DataFrame): Must contain 'release_date',
            'revenue_musd', and 'budget_musd' columns.

    Raises:
        TypeError: If df is not a PySpark DataFrame.
        ValueError: If required columns are missing.
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame.")
    for col_name in ('release_date', 'revenue_musd', 'budget_musd'):
        if col_name not in df.columns:
            raise ValueError(f"Required column '{col_name}' not found in DataFrame.")

    # Extract year from release_date
    df = df.withColumn("release_year", F.year("release_date"))

    # Aggregate with Spark
    yearly = df.groupBy("release_year").agg(
        F.mean("revenue_musd").alias("mean_revenue"),
        F.mean("budget_musd").alias("mean_budget")
    ).orderBy("release_year")

    pdf = yearly.toPandas()

    plt.figure(figsize=(10, 5))
    plt.plot(
        pdf['release_year'],
        pdf['mean_revenue'],
        label='Mean Revenue',
        color='orange',
        marker='o'
    )
    plt.plot(
        pdf['release_year'],
        pdf['mean_budget'],
        label='Mean Budget',
        color='blue',
        marker='o'
    )
    plt.xlabel('Year')
    plt.ylabel('Million USD')
    plt.title('Yearly Average Revenue & Budget')
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_franchise_vs_standalone_revenue(franchise_movies, standalone_movies):
    """Bar chart comparing mean revenue of franchise vs. standalone movies.

    Args:
        franchise_movies (pyspark.sql.DataFrame): DataFrame of movies
            belonging to a franchise. Must contain 'revenue_musd'.
        standalone_movies (pyspark.sql.DataFrame): DataFrame of standalone
            movies. Must contain 'revenue_musd'.

    Raises:
        TypeError: If either argument is not a PySpark DataFrame.
        ValueError: If 'revenue_musd' column is missing from either DataFrame.
    """
    for label, df in [('franchise_movies', franchise_movies), ('standalone_movies', standalone_movies)]:
        if not isinstance(df, DataFrame):
            raise TypeError(f"{label} must be a PySpark DataFrame.")
        if 'revenue_musd' not in df.columns:
            raise ValueError(f"Required column 'revenue_musd' not found in {label}.")

    # Compute mean revenue in Spark
    franchise_mean_rev = franchise_movies.agg(F.mean("revenue_musd")).collect()[0][0]
    standalone_mean_rev = standalone_movies.agg(F.mean("revenue_musd")).collect()[0][0]

    plt.figure(figsize=(6, 4))
    plt.bar(
        ['Franchise', 'Standalone'],
        [franchise_mean_rev, standalone_mean_rev],
        color=['orange', 'blue'],
    )
    plt.ylabel('Million USD')
    plt.title('Franchise vs Standalone: Mean Revenue')
    plt.tight_layout()
    plt.show()
