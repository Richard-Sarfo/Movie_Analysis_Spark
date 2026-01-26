A comprehensive data analysis project that extracts, transforms, and analyzes movie data from The Movie Database (TMDB) API using Apache Spark and PySpark.
📋 Table of Contents

Overview
Features
Project Structure
Prerequisites
Installation
Configuration
Usage
Data Pipeline
Visualizations
Key Insights

🎯 Overview
This project demonstrates a complete big data pipeline using Apache Spark to analyze movie data from TMDB. It includes data extraction from the TMDB API, data transformation using PySpark, comprehensive analysis, and visualization of key metrics.
✨ Features

Data Extraction: Automated fetching of movie data from TMDB API
Data Transformation: Complex data cleaning and transformation using PySpark
KPI Analysis:

Revenue and budget analysis
ROI (Return on Investment) calculations
Movie ratings and popularity metrics
Genre-based analysis
Franchise vs standalone movie comparisons


Visualizations:

Revenue vs Budget scatter plots
ROI distribution by genre
Yearly revenue trends
Popularity vs Rating analysis
Franchise performance comparisons

📁 Project Structure
├── main.ipynb                    # Main Jupyter notebook
├── Functions/
│   ├── data_extraction.py        # TMDB API data fetching
│   ├── data_cleaning.py          # Data transformation functions
│   ├── KPIs_Analysis.py          # KPI calculation functions
│   ├── Visualization.py          # Plotting and visualization
│   └── Schema.py                 # PySpark schema definitions
├── tmdb_movies.json              # Raw movie data (generated)
└── README.md
🔧 Prerequisites

Python 3.8+
Apache Spark 3.5.0
Java 8 or 11 (required for Spark)
TMDB API Key

📦 Installation

Clone the repository

bashgit clone <https://github.com/Richard-Sarfo/Movie_Analysis_Spark>
cd tmdb-movie-analysis

Install required packages

bashpip install pyspark==3.5.0
pip install requests
pip install matplotlib
pip install seaborn

Set up Apache Spark (if not already installed)

bash# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz

# Set environment variables
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
⚙️ Configuration

Get TMDB API Key

Sign up at TMDB
Get your API key from account settings

Set API Key

python# In your environment or notebook
import os
os.environ["key"] = "your_tmdb_api_key_here"
🚀 Usage

Start Jupyter Notebook

bashjupyter notebook main.ipynb

Run the Analysis

Execute cells sequentially to:

Extract data from TMDB API
Transform and clean data
Calculate KPIs
Generate visualizations


Key Movie IDs Used (customizable in notebook)

pythonmovie_ids = [299534, 19995, 140607, 299536, 597, 135397, 
             420818, 24428, 168259, 99861, 284054, 12445, 
             181808, 330457, 351286, 109445, 321612, 260513]
🔄 Data Pipeline
1. Data Extraction
pythonfrom Functions.data_extraction import movie

movies = []
for movie_id in movie_ids:
    movies.append(movie(movie_id, key))
2. Data Transformation

Genre extraction
Collection name extraction
Cast and crew processing
Budget/Revenue conversion to millions
ROI calculation

3. Data Analysis
Key metrics calculated:

Top movies by revenue
Highest/lowest budget
Profit analysis
ROI rankings
Rating distributions

📊 Visualizations
The project generates several insightful visualizations:

Revenue vs Budget: Scatter plot showing relationship between budget and revenue
ROI by Genre: Box plot comparing ROI across different genres
Yearly Trends: Line plot showing revenue and budget trends over time
Popularity vs Rating: Scatter plot analyzing correlation
Franchise Comparison: Bar chart comparing franchise vs standalone movie performance

🎯 Key Insights
Sample insights from the analysis:

Top Revenue Movie: Avatar - $2,923.71M
Highest Budget: Avengers: Endgame - $356.0M
Best ROI: Avatar - 1,233.63%
Genre Analysis: Action/Adventure films show high ROI
Franchise Impact: Franchise movies generally outperform standalone films

📈 Sample KPIs
python# Top 5 movies by revenue
highest_revenue = rank_movies(df, column='revenue_musd', n=5)

# Highest ROI (minimum budget $10M)
highest_roi = rank_movies(df, column='roi', n=1, min_budget=10)

# Best rated movies (minimum 10 votes)
highest_rate = rank_movies(df, column='vote_average', n=1, min_votes=10)
🛠️ Technologies Used

Apache Spark: Distributed data processing
PySpark: Python API for Spark
Pandas: Data manipulation
Matplotlib/Seaborn: Data visualization
TMDB API: Movie data source

📝 Notes

The project uses a subset of movies for demonstration
API rate limits may apply for large-scale extractions
Spark configuration can be adjusted based on available resources
Data is stored as JSON for portability

🤝 Contributing
Contributions are welcome! Please feel free to submit a Pull Request.
📄 License
This project is open source and available under the MIT License.
🙏 Acknowledgments

The Movie Database (TMDB) for providing the API
Apache Spark community for the excellent documentation
