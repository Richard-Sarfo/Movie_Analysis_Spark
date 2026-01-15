"""
Visualization Module
Handles all data visualizations
"""
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


def setup_style():
    """Setup matplotlib style"""
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (15, 10)


def plot_revenue_vs_budget(df_pandas, ax):
    """Plot Revenue vs Budget"""
    df_plot = df_pandas.dropna(subset=['budget_musd', 'revenue_musd'])
    ax.scatter(df_plot['budget_musd'], df_plot['revenue_musd'], alpha=0.6, c='blue')
    ax.plot([0, df_plot['budget_musd'].max()], [0, df_plot['budget_musd'].max()],
            'r--', label='Break-even line')
    ax.set_xlabel('Budget (Million USD)')
    ax.set_ylabel('Revenue (Million USD)')
    ax.set_title('Revenue vs Budget')
    ax.legend()
    ax.grid(True, alpha=0.3)


def plot_roi_by_genre(df_pandas, ax):
    """Plot ROI Distribution by Genre"""
    genre_roi = []
    for _, row in df_pandas.iterrows():
        if pd.notna(row['genres']) and pd.notna(row['roi']):
            genres = row['genres'].split('|')
            for genre in genres[:1]:
                genre_roi.append({'genre': genre, 'roi': row['roi']})

    genre_roi_df = pd.DataFrame(genre_roi)
    if not genre_roi_df.empty:
        genre_roi_summary = genre_roi_df.groupby('genre')['roi'].mean().sort_values(ascending=False).head(10)
        genre_roi_summary.plot(kind='barh', ax=ax, color='green')
        ax.set_xlabel('Average ROI')
        ax.set_title('Top 10 Genres by Average ROI')
        ax.grid(True, alpha=0.3, axis='x')


def plot_popularity_vs_rating(df_pandas, ax):
    """Plot Popularity vs Rating"""
    df_plot = df_pandas.dropna(subset=['popularity', 'vote_average'])
    scatter = ax.scatter(df_plot['vote_average'], df_plot['popularity'],
                         alpha=0.6, c=df_plot['revenue_musd'], cmap='viridis')
    ax.set_xlabel('Vote Average (Rating)')
    ax.set_ylabel('Popularity')
    ax.set_title('Popularity vs Rating (colored by Revenue)')
    plt.colorbar(scatter, ax=ax, label='Revenue (M USD)')
    ax.grid(True, alpha=0.3)


def plot_yearly_trends(df_pandas, ax):
    """Plot Yearly Box Office Trends"""
    df_pandas['year'] = pd.to_datetime(df_pandas['release_date']).dt.year
    yearly_revenue = df_pandas.groupby('year')['revenue_musd'].sum().dropna()
    yearly_revenue.plot(kind='line', ax=ax, marker='o', color='purple', linewidth=2)
    ax.set_xlabel('Year')
    ax.set_ylabel('Total Revenue (Million USD)')
    ax.set_title('Yearly Box Office Trends')
    ax.grid(True, alpha=0.3)


def plot_franchise_comparison(df_pandas, ax):
    """Plot Franchise vs Standalone"""
    franchise_comparison = df_pandas.copy()
    franchise_comparison['type'] = franchise_comparison['belongs_to_collection'].apply(
        lambda x: 'Franchise' if pd.notna(x) else 'Standalone'
    )
    comparison_stats = franchise_comparison.groupby('type')[['revenue_musd', 'budget_musd']].mean()
    comparison_stats.plot(kind='bar', ax=ax, color=['#FF6B6B', '#4ECDC4'])
    ax.set_xlabel('Movie Type')
    ax.set_ylabel('Average (Million USD)')
    ax.set_title('Franchise vs Standalone: Budget & Revenue')
    ax.legend(['Revenue', 'Budget'])
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
    ax.grid(True, alpha=0.3, axis='y')


def plot_top_directors(df_pandas, ax):
    """Plot Top Directors by Revenue"""
    director_revenue = df_pandas.groupby('director')['revenue_musd'].sum().sort_values(ascending=False).head(10)
    director_revenue.plot(kind='barh', ax=ax, color='orange')
    ax.set_xlabel('Total Revenue (Million USD)')
    ax.set_title('Top 10 Directors by Total Revenue')
    ax.grid(True, alpha=0.3, axis='x')


def create_dashboard(df_spark, save_path='tmdb_analysis_dashboard.png'):
    """Create complete visualization dashboard"""
    print("\n" + "="*70)
    print("CREATING VISUALIZATIONS")
    print("="*70)

    df_pandas = df_spark.toPandas()
    setup_style()

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('TMDB Movie Data Analysis Dashboard', fontsize=16, fontweight='bold')

    # Create all plots
    plot_revenue_vs_budget(df_pandas, axes[0, 0])
    plot_roi_by_genre(df_pandas, axes[0, 1])
    plot_popularity_vs_rating(df_pandas, axes[0, 2])
    plot_yearly_trends(df_pandas, axes[1, 0])
    plot_franchise_comparison(df_pandas, axes[1, 1])
    plot_top_directors(df_pandas, axes[1, 2])

    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"\n✓ Dashboard saved as '{save_path}'")
    plt.show()

    return fig
