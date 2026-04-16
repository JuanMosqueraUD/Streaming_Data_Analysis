from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import json
import os
import sys
sys.path.insert(0, "/opt/airflow/utils")
sys.path.insert(0, "/opt/airflow/dags")
from utils.tmdb_client import (
    fetch_trending_movies_day,
    fetch_now_playing_movies,
    fetch_multiple_pages,
    fetch_reviews_for_movies
)
from utils.rt_scraper import (
    scrape_movies_in_theaters,
    scrape_movies_at_home,
    scrape_reviews_for_movies
)

BRONZE_PATH = "/opt/airflow/datalake_bronze"
TMDB_NOW_PLAYING_PAGES = int(os.getenv("TMDB_NOW_PLAYING_PAGES", os.getenv("TMDB_PAGES", "3")))
TMDB_REVIEW_MOVIES = int(os.getenv("TMDB_REVIEW_MOVIES", "10"))
TMDB_REVIEWS_PER_MOVIE = int(os.getenv("TMDB_REVIEWS_PER_MOVIE", "5"))
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
        data = fetch_trending_movies_day()
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_trending_movie_day"
        return save_json(data, f"tmdb_trending_movie_day_{ts}.json")

    @task()
    def extract_tmdb_now_playing():
        ts = get_timestamp()
        data = fetch_multiple_pages(fetch_now_playing_movies, pages=TMDB_NOW_PLAYING_PAGES)
        data["ingested_at"] = datetime.utcnow().isoformat()
        data["source"] = "tmdb_now_playing"
        return save_json(data, f"tmdb_now_playing_{ts}.json")

    @task()
    def extract_tmdb_reviews(trending_file, now_playing_file):
        ts = get_timestamp()
        with open(trending_file, "r", encoding="utf-8") as f:
            trending_data = json.load(f)

        with open(now_playing_file, "r", encoding="utf-8") as f:
            now_playing_data = json.load(f)

        combined_movies = trending_data.get("results", []) + now_playing_data.get("results", [])
        seen_ids = set()
        unique_movies = []
        for movie in combined_movies:
            movie_id = movie.get("id")
            if movie_id is None or movie_id in seen_ids:
                continue
            seen_ids.add(movie_id)
            unique_movies.append(movie)

        reviews_data = fetch_reviews_for_movies(
            unique_movies,
            max_movies=TMDB_REVIEW_MOVIES,
            max_reviews_per_movie=TMDB_REVIEWS_PER_MOVIE,
        )

        total_reviews = sum(item.get("total_reviews", 0) for item in reviews_data)
        print(
            "TMDB review summary -> "
            f"movies={len(reviews_data)}, "
            f"reviews={total_reviews}"
        )

        data = {
            "results": reviews_data,
            "total_movies": len(reviews_data),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "tmdb_movie_reviews"
        }
        return save_json(data, f"tmdb_movie_reviews_{ts}.json")

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
    def extract_rt_at_home():
        ts = get_timestamp()
        movies = scrape_movies_at_home()
        data = {
            "results": movies,
            "total": len(movies),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "rottentomatoes_at_home"
        }
        return save_json(data, f"rottentomatoes_at_home_{ts}.json")

    @task()
    def extract_rt_reviews():
        ts = get_timestamp()
        movies = scrape_movies_in_theaters()
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
    def extract_rt_at_home_reviews():
        ts = get_timestamp()
        movies = scrape_movies_at_home()
        reviews_data = scrape_reviews_for_movies(
            movies,
            max_movies=RT_MAX_MOVIES,
            max_reviews_per_movie=RT_MAX_REVIEWS_PER_MOVIE
        )

        total_critic_reviews = sum(item.get("total_critic_reviews", 0) for item in reviews_data)
        total_audience_reviews = sum(item.get("total_audience_reviews", 0) for item in reviews_data)
        print(
            "RT at-home review summary -> "
            f"movies={len(reviews_data)}, "
            f"critic_reviews={total_critic_reviews}, "
            f"audience_reviews={total_audience_reviews}"
        )

        data = {
            "results": reviews_data,
            "total_movies": len(reviews_data),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "rottentomatoes_at_home_reviews"
        }
        return save_json(data, f"rottentomatoes_at_home_reviews_{ts}.json")

    @task()
    def validate_files(*files):
        for filepath in files:
            assert os.path.exists(filepath), f"Missing: {filepath}"
            assert os.path.getsize(filepath) > 0, f"Empty: {filepath}"
            print(f"OK: {filepath}")

    trending   = extract_tmdb_trending()
    now_playing = extract_tmdb_now_playing()
    tmdb_reviews = extract_tmdb_reviews(trending, now_playing)
    theaters   = extract_rt_theaters()
    at_home    = extract_rt_at_home()
    reviews    = extract_rt_reviews()
    at_home_reviews = extract_rt_at_home_reviews()

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_processing",
        trigger_dag_id="silver_processing",
        execution_date="{{ execution_date + macros.timedelta(minutes=3) }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    validate_files(trending, now_playing, tmdb_reviews, theaters, at_home, reviews, at_home_reviews) >> trigger_silver

bronze_ingestion()