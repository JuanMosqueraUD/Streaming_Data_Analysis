from airflow.decorators import dag, task
from datetime import datetime
import json
import os
import sys

sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/dags")
from utils.tmdb_client import fetch_top_rated_movies, fetch_top_rated_tv
from utils.rt_scraper import scrape_movies_in_theaters

BRONZE_PATH = "/opt/airflow/datalake_bronze"

@dag(
    dag_id="bronze_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
    doc_md="""
    ## Bronze Ingestion DAG
    Extrae datos de TMDB API y Rotten Tomatoes scraping.
    Guarda los resultados como JSON en datalake_bronze/.
    Nomenclatura: source_YYYYMMDDTHHMMSS.json
    """
)
def bronze_ingestion():

    @task()
    def extract_tmdb_movies(**context):
        ts = context["ts_nodash"]
        data = fetch_top_rated_movies()

        # Agregar metadata de ingestion
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_top_rated_movies"

        filename = f"{BRONZE_PATH}/tmdb_movies_{ts}.json"
        os.makedirs(BRONZE_PATH, exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Saved {len(data['results'])} records to {filename}")
        return filename

    @task()
    def extract_tmdb_tv(**context):
        ts = context["ts_nodash"]
        data = fetch_top_rated_tv()

        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_top_rated_tv"

        filename = f"{BRONZE_PATH}/tmdb_tv_{ts}.json"
        os.makedirs(BRONZE_PATH, exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Saved {len(data['results'])} records to {filename}")
        return filename

    @task()
    def extract_rotten_tomatoes(**context):
        ts = context["ts_nodash"]
        movies = scrape_movies_in_theaters()

        data = {
            "results": movies,
            "total": len(movies),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "rotten_tomatoes_theaters"
        }

        filename = f"{BRONZE_PATH}/rottentomatoes_{ts}.json"
        os.makedirs(BRONZE_PATH, exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        print(f"Saved {len(movies)} records to {filename}")
        return filename

    @task()
    def validate_bronze_files(tmdb_movies_file, tmdb_tv_file, rt_file):
        """Valida que los archivos se crearon y tienen contenido"""
        for filepath in [tmdb_movies_file, tmdb_tv_file, rt_file]:
            assert os.path.exists(filepath), f"File not found: {filepath}"
            size = os.path.getsize(filepath)
            assert size > 0, f"File is empty: {filepath}"
            print(f"OK: {filepath} ({size} bytes)")

    tmdb_movies = extract_tmdb_movies()
    tmdb_tv     = extract_tmdb_tv()
    rt          = extract_rotten_tomatoes()

    validate_bronze_files(tmdb_movies, tmdb_tv, rt)

bronze_ingestion()