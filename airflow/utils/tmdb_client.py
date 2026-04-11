import requests
import os
from datetime import datetime

TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_headers():
    token = os.environ.get("TMDB_ACCESS_TOKEN")
    if not token:
        raise ValueError("TMDB_ACCESS_TOKEN no definido")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def _request_json(path, params=None):
    response = requests.get(
        f"{TMDB_BASE_URL}{path}",
        headers=get_headers(),
        params=params or {},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _normalize_movie(movie):
    return {
        "id": movie.get("id"),
        "title": movie.get("title"),
        "popularity": movie.get("popularity"),
        "vote_average": movie.get("vote_average"),
        "vote_count": movie.get("vote_count"),
        "genre_ids": movie.get("genre_ids", []),
        "release_date": movie.get("release_date"),
        "overview": movie.get("overview"),
    }


def _normalize_review(review):
    author_details = review.get("author_details") or {}
    return {
        "author": review.get("author"),
        "content": review.get("content"),
        "rating": author_details.get("rating"),
        "created_at": review.get("created_at"),
        "review_id": review.get("id"),
        "url": review.get("url"),
    }


def fetch_trending_movies_day():
    """Trending diario de películas."""
    data = _request_json("/trending/movie/day")
    data["results"] = [_normalize_movie(movie) for movie in data.get("results", [])]
    return data

def fetch_now_playing_movies(page=1):
    """Películas actualmente en cines."""
    data = _request_json("/movie/now_playing", params={"page": page})
    data["results"] = [_normalize_movie(movie) for movie in data.get("results", [])]
    return data


def fetch_movie_reviews(movie_id, page=1):
    """Reseñas asociadas a una película específica de TMDB."""
    data = _request_json(f"/movie/{movie_id}/reviews", params={"page": page})
    data["results"] = [_normalize_review(review) for review in data.get("results", [])]
    return data


def fetch_reviews_for_movies(movies, max_movies=10, max_reviews_per_movie=5):
    """
    Trae reviews de TMDB para las películas recibidas desde trending/now playing.
    Devuelve un objeto por película con sus reviews.
    """
    seen_ids = set()
    unique_movies = []
    for movie in movies:
        movie_id = movie.get("id")
        if movie_id is None or movie_id in seen_ids:
            continue
        seen_ids.add(movie_id)
        unique_movies.append(movie)

    results = []
    for movie in unique_movies[:max_movies]:
        movie_id = movie.get("id")
        if movie_id is None:
            continue

        collected_reviews = []
        page = 1

        while len(collected_reviews) < max_reviews_per_movie:
            payload = fetch_movie_reviews(movie_id, page=page)
            page_reviews = payload.get("results", [])
            if not page_reviews:
                break

            collected_reviews.extend(page_reviews)

            total_pages = payload.get("total_pages") or page
            if page >= total_pages:
                break

            page += 1

        results.append({
            "movie": movie,
            "reviews": collected_reviews[:max_reviews_per_movie],
            "total_reviews": len(collected_reviews[:max_reviews_per_movie]),
            "scraped_at": datetime.utcnow().isoformat(),
            "source": "tmdb_movie_reviews",
        })

    return results

def fetch_multiple_pages(fetch_func, pages=3):
    """
    Llama cualquier función de fetch varias veces para obtener más registros.
    TMDB trae 20 resultados por página — con 3 páginas tienes 60 registros.
    """
    all_results = []
    metadata = {}
    
    for page in range(1, pages + 1):
        data = fetch_func(page=page)
        all_results.extend(data["results"])
        if page == 1:
            metadata = {k: v for k, v in data.items() if k != "results"}
    
    metadata["results"] = all_results
    return metadata


# Backwards-compatible alias kept for any external code that still imports the old name.
fetch_trending_today = fetch_trending_movies_day