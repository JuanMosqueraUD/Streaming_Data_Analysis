import requests
import json
import os
from datetime import datetime

TMDB_BASE_URL = "https://api.themoviedb.org/3"

def get_headers():
    token = os.environ.get("TMDB_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def fetch_top_rated_movies(page=1):
    response = requests.get(
        f"{TMDB_BASE_URL}/movie/top_rated",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()

def fetch_top_rated_tv(page=1):
    response = requests.get(
        f"{TMDB_BASE_URL}/tv/top_rated",
        headers=get_headers(),
        params={"page": page}
    )
    response.raise_for_status()
    return response.json()