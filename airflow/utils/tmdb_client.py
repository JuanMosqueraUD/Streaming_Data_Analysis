import requests
import os

TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_headers():
    token = os.environ.get("TMDB_ACCESS_TOKEN")
    if not token:
        raise ValueError("TMDB_ACCESS_TOKEN no definido")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def fetch_top_rated_movies(page=1):
    """Top rated histórico de películas"""
    response = requests.get(
        f"{TMDB_BASE_URL}/movie/top_rated",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()

def fetch_top_rated_tv(page=1):
    """Top rated histórico de series"""
    response = requests.get(
        f"{TMDB_BASE_URL}/tv/top_rated",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()

def fetch_trending_today():
    """Lo más trending hoy — cambia cada día, ideal para análisis temporal"""
    response = requests.get(
        f"{TMDB_BASE_URL}/trending/all/day",
        headers=get_headers()
    )
    response.raise_for_status()
    return response.json()

def fetch_now_playing_movies(page=1):
    """Películas actualmente en cines — complementa el scraping de RT"""
    response = requests.get(
        f"{TMDB_BASE_URL}/movie/now_playing",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()

def fetch_popular_movies(page=1):
    """Populares ahora — métrica dinámica que cambia diario"""
    response = requests.get(
        f"{TMDB_BASE_URL}/movie/popular",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()

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