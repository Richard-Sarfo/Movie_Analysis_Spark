import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

def plot_revenue_vs_budget(df):
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
