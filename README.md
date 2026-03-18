# Streaming & Entertainment Public Sentiment — Data Analysis Project

## Project Overview

This project analyzes **public sentiment toward streaming content and the entertainment industry** by collecting, processing, and visualizing review and rating data from major entertainment aggregators. The goal is to uncover trends in audience perception, identify highly-rated vs. poorly-received titles, and track how sentiment evolves over time across genres and platforms.

---

## Team Members

| Name | Student ID |
|------|-----------|
| Juan David Amaya Patiño | 20221020057 |
| Juan Pablo Mosquera | 20221020026 |
| Edward Julian Garcia Gaitan | — |

---

## Data Sources

| Source | Description | URL |
|--------|-------------|-----|
| **IMDb** | Internet Movie Database — user ratings, reviews, and metadata for movies and TV shows | https://www.imdb.com |
| **Rotten Tomatoes** | Critic and audience scores, consensus summaries, and review text for films and series | https://www.rottentomatoes.com |

---

## Repository Structure

```
Streaming_Data_Analysis/
│
├── datalake_bronze/        # Raw ingestion outputs (JSON files from APIs / scrapers)
├── datalake_silver/        # Cleaned and transformed data (Parquet files)
├── datalake_gold/          # Aggregated and summarized datasets ready for analysis
│
├── airflow/
│   └── dags/               # Apache Airflow DAG definitions for pipeline orchestration
│
├── dashboard/              # Interactive Plotly Dash web application(s)
├── notebooks/              # Exploratory data analysis (Jupyter Notebooks)
├── workshop_1/             # Workshop 1 deliverables and data samples
│
└── README.md               # Project documentation (this file)
```

### Layer descriptions

| Layer | Directory | Format | Purpose |
|-------|-----------|--------|---------|
| **Bronze** | `datalake_bronze/` | JSON | Raw data as received from sources — no transformations applied |
| **Silver** | `datalake_silver/` | Parquet | Cleaned, deduplicated, and schema-normalized records |
| **Gold** | `datalake_gold/` | Parquet / CSV | Aggregated metrics and KPIs ready for dashboarding and reporting |

---

## Pipeline Architecture

```
IMDb / Rotten Tomatoes
        │
        ▼
  [Ingestion scripts]
        │
        ▼
  datalake_bronze/   (raw JSON)
        │
        ▼
  [Transformation]
        │
        ▼
  datalake_silver/   (cleaned Parquet)
        │
        ▼
  [Aggregation]
        │
        ▼
  datalake_gold/     (summary Parquet / CSV)
        │
        ▼
  dashboard/         (Plotly Dash visualizations)
```

All pipeline steps are orchestrated via **Apache Airflow** DAGs located in `airflow/dags/`.

---

## Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/JuanMosqueraUD/Streaming_Data_Analysis.git
   cd Streaming_Data_Analysis
   ```

2. **Install dependencies** (requirements file will be added per component)
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Airflow** to trigger data pipelines
   ```bash
   airflow db init
   airflow scheduler &
   airflow webserver
   ```

4. **Launch the dashboard**
   ```bash
   python dashboard/app.py
   ```

---

## License

This project is developed for academic purposes as part of the Streaming Data Analysis course.
