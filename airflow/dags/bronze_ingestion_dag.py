from airflow.decorators import dag, task
from datetime import datetime
import json
import os
import sys
sys.path.insert(0, "/opt/airflow/utils")
sys.path.insert(0, "/opt/airflow/dags")
from utils.tmdb_client import (
    fetch_trending_today,
    fetch_popular_movies,
    fetch_top_rated_movies,
    fetch_top_rated_tv,
    fetch_multiple_pages
)
from utils.rt_scraper import (
    scrape_movies_in_theaters,
    scrape_reviews_for_movies
)

BRONZE_PATH = "/opt/airflow/datalake_bronze"
TMDB_PAGES = int(os.getenv("TMDB_PAGES", "3"))
RT_MAX_MOVIES = int(os.getenv("RT_MAX_MOVIES", "5"))
RT_MAX_REVIEWS_PER_MOVIE = int(os.getenv("RT_MAX_REVIEWS_PER_MOVIE", "10"))


def get_timestamp():
    """Retorna timestamp en formato YYYYMMDD_HHMMSS para nombres de archivo"""
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def save_json(data, filename):
    os.makedirs(BRONZE_PATH, exist_ok=True)
    filepath = f"{BRONZE_PATH}/{filename}"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"Saved: {filepath} ({os.path.getsize(filepath)} bytes)")
    return filepath


@dag(
    dag_id="bronze_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"]
)
def bronze_ingestion():

    @task()
    def extract_tmdb_trending():
        ts = get_timestamp()
        data = fetch_trending_today()
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_trending_day"
        # Nomenclatura: source_YYYYMMDD_HHMMSS.json
        return save_json(data, f"tmdb_trending_{ts}.json")

    @task()
    def extract_tmdb_popular():
        ts = get_timestamp()
        data = fetch_multiple_pages(fetch_popular_movies, pages=TMDB_PAGES)
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_popular_movies"
        return save_json(data, f"tmdb_popular_{ts}.json")

    @task()
    def extract_tmdb_top_rated():
        ts = get_timestamp()
        data = fetch_multiple_pages(fetch_top_rated_movies, pages=TMDB_PAGES)
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_top_rated"
        return save_json(data, f"tmdb_top_rated_{ts}.json")

    @task()
    def extract_tmdb_tv():
        ts = get_timestamp()
        data = fetch_multiple_pages(fetch_top_rated_tv, pages=TMDB_PAGES)
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_top_rated_tv"
        return save_json(data, f"tmdb_tv_{ts}.json")

    @task()
    def extract_rt_theaters():
        ts = get_timestamp()
        movies = scrape_movies_in_theaters()
        data = {
            "results": movies,
            "total": len(movies),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "rottentomatoes_theaters"
        }
        return save_json(data, f"rottentomatoes_theaters_{ts}.json")

    @task()
    def extract_rt_reviews(rt_theaters_file):
        """
        Lee el archivo de teatros que acaba de generar la tarea anterior
        y scrapea las reseñas de las primeras 5 películas.
        Depende de extract_rt_theaters para tener las URLs.
        """
        ts = get_timestamp()

        # Lee el archivo Bronze que generó la tarea anterior
        with open(rt_theaters_file, "r", encoding="utf-8") as f:
            theaters_data = json.load(f)

        movies = theaters_data.get("results", [])
        reviews_data = scrape_reviews_for_movies(
            movies,
            max_movies=RT_MAX_MOVIES,
            max_reviews_per_movie=RT_MAX_REVIEWS_PER_MOVIE
        )

        total_critic_reviews = sum(item.get("total_critic_reviews", 0) for item in reviews_data)
        total_audience_reviews = sum(item.get("total_audience_reviews", 0) for item in reviews_data)
        print(
            "RT review summary -> "
            f"movies={len(reviews_data)}, "
            f"critic_reviews={total_critic_reviews}, "
            f"audience_reviews={total_audience_reviews}"
        )

        data = {
            "results": reviews_data,
            "total_movies": len(reviews_data),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "rottentomatoes_reviews"
        }
        return save_json(data, f"rottentomatoes_reviews_{ts}.json")

    @task()
    def validate_files(*files):
        for filepath in files:
            assert os.path.exists(filepath), f"Missing: {filepath}"
            assert os.path.getsize(filepath) > 0, f"Empty: {filepath}"
            print(f"OK: {filepath}")

    trending   = extract_tmdb_trending()
    popular    = extract_tmdb_popular()
    top_rated  = extract_tmdb_top_rated()
    tv         = extract_tmdb_tv()
    theaters   = extract_rt_theaters()
    reviews    = extract_rt_reviews(theaters)  # depende de theaters

    validate_files(trending, popular, top_rated, tv, theaters, reviews)

bronze_ingestion()