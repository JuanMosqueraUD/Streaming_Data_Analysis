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

- bronze_ingestion: collects TMDB and Rotten Tomatoes raw data into Bronze.
- silver_processing: reads Bronze JSON files and writes Silver Parquet datasets.

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

### 4) Run the pipeline

In Airflow UI:
1. Enable DAG bronze_ingestion
2. Trigger DAG bronze_ingestion
3. DAG silver_processing is triggered automatically by bronze_ingestion

Output directories:
- Bronze files in datalake_bronze/
- Silver files in datalake_silver/

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
