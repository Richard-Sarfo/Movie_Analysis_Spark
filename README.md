# 🎬 TMDB Movie Analysis with Apache Spark

A comprehensive data analysis project that extracts, transforms, and analyzes movie data from [The Movie Database (TMDB)](https://www.themoviedb.org/) API using **Apache Spark** and **PySpark**.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Configuration](#%EF%B8%8F-configuration)
- [Usage](#-usage)
- [Data Pipeline](#-data-pipeline)
- [Data Validation](#-data-validation)
- [Visualizations](#-visualizations)
- [Key Insights](#-key-insights)
- [Technologies Used](#%EF%B8%8F-technologies-used)
- [Contributing](#-contributing)
- [License](#-license)
- [Acknowledgments](#-acknowledgments)

---

## 🎯 Overview

This project demonstrates a complete **big data pipeline** using Apache Spark to analyze movie data from TMDB. It includes:

1. **Data Extraction** — Automated fetching of movie data from the TMDB API with retry logic and error handling.
2. **Data Transformation** — Complex data cleaning and transformation using PySpark UDFs.
3. **Data Validation** — Schema conformance, null checks, value-range constraints, and duplicate detection.
4. **KPI Analysis** — Revenue, budget, ROI, rating, and genre-based analytics.
5. **Visualization** — Publication-quality plots generated with Matplotlib and Seaborn.

---

## ✨ Features

### Data Extraction
- Configurable API base URL via `TMDB_BASE_URL` environment variable
- Automatic retry with exponential backoff (3 retries)
- Per-request timeouts (configurable via `TMDB_CONNECT_TIMEOUT` / `TMDB_READ_TIMEOUT`)
- Detailed logging to both console and file

### Data Transformation
- Genre, collection, cast, and crew extraction from nested JSON
- Budget / Revenue conversion to millions USD
- ROI (Return on Investment) calculation
- Pipe-delimited formatting for multi-value fields

### Data Validation
- Schema validation against a strict PySpark StructType
- Null / missing-value checks on critical columns (`id`, `title`)
- Value-range constraints (e.g., `budget ≥ 0`, `0 ≤ vote_average ≤ 10`)
- Duplicate-row detection on the `id` column
- Consolidated pass / fail report with per-check details

### KPI Analysis
- Revenue and budget ranking
- ROI calculations with minimum-budget thresholds
- Rating analysis with minimum-vote filters
- Genre-based aggregations

### Visualizations
- Revenue vs Budget scatter plot
- ROI distribution by genre (box plot)
- Popularity vs Rating scatter plot
- Yearly average revenue & budget trends
- Franchise vs Standalone bar chart comparison

---

## 📁 Project Structure

```text
Spark_Analysis/
├── Dockerfile                    # Docker image definition
├── docker-compose.yml            # Docker Compose orchestration
├── requirements.txt              # Python dependencies
├── README.md                     # This file
├── .env                          # Environment variables (API key, config)
│
├── Functions/                    # Reusable Python modules
│   ├── data_extraction.py        # TMDB API data fetching (configurable URL)
│   ├── data_cleaning.py          # Data transformation UDFs
│   ├── data_validation.py        # Schema & data quality checks
│   ├── KPIs_Analysis.py          # KPI calculation functions
│   ├── Visualization.py          # Plotting and visualization
│   └── Schema.py                 # PySpark schema definitions
│
├── etl/                          # ETL entry points
│   ├── main.ipynb                # Main Jupyter notebook
│   └── tmdb_movies.json          # Raw movie data (generated)
│
└── data/                         # Additional data files (optional)
```

---

## 🔧 Prerequisites

| Requirement       | Version  |
|--------------------|----------|
| Python             | 3.8+     |
| Apache Spark       | 3.5.0    |
| Java (JDK)         | 8 or 11  |
| TMDB API Key       | v3       |
| Docker *(optional)* | 20.10+  |

---

## 📦 Installation

### 1. Clone the repository

```bash
git clone https://github.com/Richard-Sarfo/Movie_Analysis_Spark.git
cd Movie_Analysis_Spark
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install pyspark==3.5.0 requests matplotlib seaborn pandas
```

### 3. Install Apache Spark (if not already installed)

```bash
# Download Spark
wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz

# Set environment variables
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

### 4. Docker (alternative)

```bash
docker compose up --build
```

This starts a Jupyter PySpark notebook accessible at <http://localhost:8888>.

---

## ⚙️ Configuration

### TMDB API Key

1. Sign up at [TMDB](https://www.themoviedb.org/signup).
2. Navigate to **Settings → API** and copy your API key.
3. Create a `.env` file in the project root:

```dotenv
# .env
TMDB_API_KEY=your_tmdb_api_key_here

# Optional overrides (defaults shown)
TMDB_BASE_URL=https://api.themoviedb.org/3
TMDB_CONNECT_TIMEOUT=5
TMDB_READ_TIMEOUT=10
```

### Using the API key in code

```python
import os

key = os.environ["TMDB_API_KEY"]
```

> **Note:** Never commit your `.env` file to version control. It is already listed in `.gitignore`.

---

## 🚀 Usage

### Start Jupyter Notebook

```bash
jupyter notebook etl/main.ipynb
```

### Run the analysis

Execute the cells sequentially to:

1. **Extract** data from TMDB API
2. **Validate** the raw data against the strict schema
3. **Transform** and clean the data
4. **Calculate** KPIs
5. **Generate** visualizations

### Configurable Movie IDs

Movie IDs are configurable — pass any list of valid TMDB IDs:

```python
from Functions.data_extraction import fetch_movies

movie_ids = [299534, 19995, 140607, 299536, 597, 135397,
             420818, 24428, 168259, 99861, 284054, 12445,
             181808, 330457, 351286, 109445, 321612, 260513]

results = fetch_movies(movie_ids, key)
```

---

## 🔄 Data Pipeline

### 1. Data Extraction

```python
from Functions.data_extraction import movie, fetch_movies

# Single movie
data = movie(movie_id=299534, key=key)

# Batch fetch with summary logging
results = fetch_movies(movie_ids, key)
```

### 2. Data Validation

```python
from Functions.data_validation import run_all_validations
from Functions.Schema import get_tmdb_raw_schema

schema = get_tmdb_raw_schema()
df = spark.read.schema(schema).json("etl/tmdb_movies.json")

report = run_all_validations(df, expected_schema=schema)
print(f"Validation: {'PASSED' if report['passed'] else 'FAILED'}")
print(f"  Checks: {report['passed_checks']}/{report['total_checks']} passed")

if not report['passed']:
    for failure in report['failures']:
        print(f"  ✗ {failure['check']}: {failure}")
```

### 3. Data Transformation

- Genre extraction (nested JSON → flat list)
- Collection name extraction
- Cast and crew processing
- Budget / Revenue conversion to millions USD
- ROI calculation: `(revenue - budget) / budget × 100`

### 4. KPI Analysis

```python
from Functions.KPIs_Analysis import rank_movies

# Top 5 movies by revenue
top_revenue = rank_movies(df, column='revenue_musd', n=5)

# Highest ROI (minimum budget $10M)
best_roi = rank_movies(df, column='roi', n=1, min_budget=10)

# Best rated movies (minimum 10 votes)
best_rated = rank_movies(df, column='vote_average', n=1, min_votes=10)
```

---

## ✅ Data Validation

The `data_validation` module provides four independent checks that can be run individually or as a suite:

| Check              | What it validates                                      |
|--------------------|-------------------------------------------------------|
| `validate_schema`  | Column names, data types match expected schema         |
| `validate_not_null`| Critical columns (`id`, `title`) contain no nulls      |
| `validate_value_ranges` | Numeric columns within expected bounds (e.g., `budget ≥ 0`) |
| `validate_no_duplicates` | No duplicate rows on the `id` column              |

Run all checks at once:

```python
from Functions.data_validation import run_all_validations

report = run_all_validations(df, expected_schema=schema)
# report['passed']        → True / False
# report['total_checks']  → Number of checks run
# report['failures']      → List of failed check dicts
```

---

## 📊 Visualizations

The project generates several insightful visualizations:

| Visualization                  | Description                                        |
|--------------------------------|----------------------------------------------------|
| **Revenue vs Budget**          | Scatter plot showing the budget–revenue relationship |
| **ROI by Genre**               | Box plot comparing ROI across primary genres        |
| **Popularity vs Rating**       | Scatter plot analyzing the popularity–rating correlation |
| **Yearly Trends**              | Line plot of average revenue & budget over time     |
| **Franchise vs Standalone**    | Bar chart comparing mean revenue                   |

```python
from Functions.Visualization import (
    plot_revenue_vs_budget,
    plot_roi_by_genre,
    plot_popularity_vs_rating,
    plot_yearly_trends,
    plot_franchise_vs_standalone_revenue
)

plot_revenue_vs_budget(df)
plot_roi_by_genre(df)
```

---

## 🎯 Key Insights

Sample insights from the analysis:

| Metric              | Result                          |
|---------------------|---------------------------------|
| Top Revenue Movie   | Avatar — $2,923.71M             |
| Highest Budget      | Avengers: Endgame — $356.0M     |
| Best ROI            | Avatar — 1,233.63%              |
| Genre Insights      | Action/Adventure shows high ROI |
| Franchise Impact    | Franchise movies outperform standalone films |

---

## 🛠️ Technologies Used

| Technology            | Purpose                        |
|-----------------------|--------------------------------|
| Apache Spark          | Distributed data processing    |
| PySpark               | Python API for Spark           |
| Pandas                | Data manipulation (plotting)   |
| Matplotlib / Seaborn  | Data visualization             |
| TMDB API              | Movie data source              |
| Docker                | Containerized environment      |

---

## 📝 Notes

- The project uses a configurable list of movie IDs — not hardcoded.
- API rate limits may apply for large-scale extractions; retry logic handles transient errors.
- Spark configuration can be adjusted based on available resources.
- Data is stored as JSON for portability.
- All modules include comprehensive logging (console + file).
- Data validation should be run after extraction and before analysis to catch data quality issues early.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🙏 Acknowledgments

- [The Movie Database (TMDB)](https://www.themoviedb.org/) for providing the API
- [Apache Spark community](https://spark.apache.org/) for excellent documentation
