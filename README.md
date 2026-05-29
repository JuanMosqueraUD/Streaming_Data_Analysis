# Streaming & Entertainment Public Sentiment - Data Analysis Project

## Project Overview

This project analyzes public sentiment toward movies and entertainment content by collecting and processing review/rating data from different public sources.

Current pipeline focus:
- TMDB (trending, now playing, and movie reviews)
- Rotten Tomatoes (in theaters, at home, and reviews)

The project follows a medallion architecture:
- Bronze: raw JSON ingestion
- Silver: normalized Parquet datasets
- Gold: curated outputs for analysis

## Team Members

| Name | Student ID |
|------|-----------|
| Juan David Amaya Patino | 20221020057 |
| Juan Pablo Mosquera | 20221020026 |
| Edward Julian Garcia Gaitan | - |

## Data Sources

| Source | Description | URL |
|--------|-------------|-----|
| TMDB | Movie metadata, now playing and trending titles, plus movie reviews | https://www.themoviedb.org |
| Rotten Tomatoes | In-theaters and at-home titles, plus critic and audience reviews | https://www.rottentomatoes.com |

## Repository Structure

```text
Streaming_Data_Analysis/
|- airflow/
|  |- config/
|  |- dags/
|  |  |- bronze_ingestion_dag.py
|  |  |- silver_processing_dag.py
|  |- logs/
|  |- plugins/
|  |- utils/
|- datalake_bronze/
|- datalake_silver/
|- datalake_gold/
|- notebooks/
|- dashboard/
|- workshop_1/
|- workshop_2/
|- docker-compose.yaml
|- dockerfile
|- requirements.txt
|- README.md
```

## Data Layers

| Layer | Directory | Main format | Purpose |
|-------|-----------|-------------|---------|
| Bronze | datalake_bronze/ | JSON | Raw API and scraping outputs |
| Silver | datalake_silver/ | Parquet | Cleaned and standardized records |
| Gold | datalake_gold/ | Parquet / CSV | Aggregated datasets for analysis |

## Airflow DAGs

- **bronze_ingestion**: Collects TMDB and Rotten Tomatoes raw data into Bronze (datalake_bronze/).
- **silver_processing**: Reads Bronze JSON files, normalizes and cleans data, writes Silver Parquet datasets (datalake_silver/). Automatically triggered by bronze_ingestion.
- **gold_processing**: Reads all Silver Parquet files, computes governance KPIs and storytelling aggregations using PySpark, writes two consolidated datasets (datalake_gold/). Scheduled weekly or triggered after silver_processing completes.

## Getting Started

### 1) Prerequisites

- Docker and Docker Compose installed
- A .env file at the project root with at least:
  - TMDB_ACCESS_TOKEN
  - TMDB_API_KEY

Optional variables (have defaults in docker-compose):
- TMDB_NOW_PLAYING_PAGES
- TMDB_REVIEW_MOVIES
- TMDB_REVIEWS_PER_MOVIE
- RT_MAX_MOVIES
- RT_MAX_REVIEWS_PER_MOVIE

### 2) Build and initialize Airflow

```bash
docker compose build
docker compose up airflow-init
```

### 3) Start services

```bash
docker compose up -d
```

Airflow UI will be available at http://localhost:8080

Default credentials (if not overridden):
- user: airflow
- password: airflow

### 4) Run the complete three-layer pipeline

In Airflow UI (http://localhost:8080):

1. **Enable and trigger bronze_ingestion DAG**
   - This DAG collects raw data from TMDB and Rotten Tomatoes APIs
   - Output: JSON files in `datalake_bronze/`
   - Automatically triggers silver_processing upon completion

2. **silver_processing runs automatically**
   - Reads Bronze JSON files and normalizes them
   - Output: Parquet files in `datalake_silver/` (silver_titles_*, silver_rt_reviews_*, silver_tmdb_reviews_*)
   - Automatically triggers gold_processing upon completion

3. **gold_processing runs automatically or on schedule (@weekly)**
   - Consolidates all Silver Parquet files using PySpark
   - Computes governance KPIs and storytelling aggregations
   - Output: Two consolidated Parquet files in `datalake_gold/`:
     - `gold_*_YYYYMMDD_HHMMSS.parquet` (consolidated datasets with metadata)

### 5) Explore Gold layer outputs

Use the Jupyter notebook to inspect and validate Gold outputs:

```bash
# Option 1: Run locally in VSCode
# Open: notebooks/gold_consolidation.ipynb
# Execute cells sequentially to read Silver Parquet and consolidate into Gold

# Option 2: Run inside Airflow container
docker-compose exec airflow-webserver jupyter notebook --ip=0.0.0.0 --no-browser
# Access at http://localhost:8888
```

### 6) Data flow summary

```
datalake_bronze/                    (raw JSON)
       ↓
bronze_ingestion DAG ──────────────→ Silver normalized
       ↓
datalake_silver/                    (Parquet: titles, reviews)
       ↓
silver_processing DAG ─────────────→ Silver cleanup, dedup
       ↓
datalake_gold/                      (consolidated Parquet)
       ↓
gold_processing DAG ───────────────→ KPIs + aggregations
       ↓
dashboard/                          (Workshop 4: Plotly Dash)
```

### Available outputs

- **datalake_bronze/**: Raw JSON files collected from APIs (managed by Airflow)
- **datalake_silver/**: Normalized Parquet datasets (titles, reviews per source)
- **datalake_gold/**: Consolidated analytical datasets ready for dashboards:
  - `gold_silver_titles_*.parquet`: Consolidated movie title data with KPIs
  - `gold_silver_rt_reviews_*.parquet`: Consolidated Rotten Tomatoes reviews
  - `gold_silver_tmdb_reviews_*.parquet`: Consolidated TMDB reviews

## Workshop 3: Gold Layer Implementation

The gold_processing DAG computes:

### Governance KPIs
- **Null rate per field**: Data quality percentage
- **Volume metrics**: Total records ingested per source and period
- **Duplicate rate**: Percentage of duplicates pre/post-deduplication
- **Schema compliance**: Records conforming to expected Silver schema
- **Text statistics**: Min, max, mean length of review text per source

### Storytelling Aggregations
- **Sentiment distribution**: Positive/negative/neutral percentages
- **Sentiment trends**: Temporal opinion shifts (by day/week)
- **Top keywords**: Most frequent terms from review corpus
- **Source comparison**: Sentiment and volume breakdown by API vs scraping
- **Reviewer rankings**: Top contributors by review count
- **Title rankings**: Most-reviewed titles

All aggregations are persisted as Parquet and ready for consumption by Workshop 4 dashboards.

## Local Python Usage (optional)

If you want to inspect notebooks or run scripts locally:

```bash
pip install -r requirements.txt
```

## Notes

- This repository currently contains pipeline code and data outputs.
- The dashboard folder exists but does not currently include an executable app.py entry point.

## License

Academic project for the Streaming Data Analysis course.
