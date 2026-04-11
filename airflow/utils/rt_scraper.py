import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RT_BASE_URL = "https://www.rottentomatoes.com"


def _get_soup(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def _extract_media_props(movie_url):
    """
    Obtiene metadatos embebidos en la página de reviews para construir
    el endpoint JSON real de Rotten Tomatoes (napi/rtcf).
    """
    soup = _get_soup(f"{movie_url}/reviews")
    props_tag = soup.select_one('page-media-reviews-manager script[data-json="props"]')

    if not props_tag or not props_tag.string:
        return None

    try:
        props = json.loads(props_tag.string)
    except json.JSONDecodeError:
        return None

    media = props.get("vanity") or {}
    media_type = media.get("mediaType")
    ems_id = media.get("emsId")

    if not media_type or not ems_id:
        return None

    base_by_type = {
        "Movie": f"{RT_BASE_URL}/napi/rtcf/v1/movies/{ems_id}/reviews",
        "TvEpisode": f"{RT_BASE_URL}/napi/rtcf/v1/tv/episodes/{ems_id}/reviews",
        "TvSeason": f"{RT_BASE_URL}/napi/rtcf/v1/tv/seasons/{ems_id}/reviews",
    }

    api_url = base_by_type.get(media_type)
    if not api_url:
        return None

    return {
        "api_url": api_url,
        "ems_id": ems_id,
        "media_type": media_type,
    }


def _fetch_reviews_from_api(api_url, review_type, max_reviews=20, top_only=False, verified=False):
    """
    Consulta la API JSON usada por Rotten Tomatoes y pagina usando cursor.
    """
    if max_reviews <= 0:
        return []

    page_count = min(max_reviews, 20)
    params = {
        "type": review_type,
        "pageCount": page_count,
    }

    if top_only:
        params["topOnly"] = "true"
    if verified:
        params["verified"] = "true"

    collected = []
    next_cursor = ""

    while len(collected) < max_reviews:
        request_params = dict(params)
        request_params["after"] = next_cursor

        response = requests.get(api_url, headers=HEADERS, params=request_params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        page_reviews = payload.get("reviews", [])
        page_info = payload.get("pageInfo", {})

        if not page_reviews:
            break

        collected.extend(page_reviews)

        has_next = bool(page_info.get("hasNextPage"))
        next_cursor = page_info.get("endCursor", "")

        if not has_next or not next_cursor:
            break

    return collected[:max_reviews]


def scrape_movies_in_theaters():
    """
    Scrapea películas actualmente en cartelera.
    Trae: título, fecha, critics score, audience score, poster, url.
    """
    url = f"{RT_BASE_URL}/browse/movies_in_theaters/sort:popular"
    soup = _get_soup(url)

    movies = []
    items = soup.find_all("div", {"data-qa": "discovery-media-list-item"})

    for item in items:
        title_tag    = item.find("span", {"data-qa": "discovery-media-list-item-title"})
        date_tag     = item.find("span", {"data-qa": "discovery-media-list-item-start-date"})
        critics_tag  = item.find("rt-text", {"slot": "criticsScore"})
        audience_tag = item.find("rt-text", {"slot": "audienceScore"})
        link_tag     = item.find("a", {"data-qa": "discovery-media-list-item-caption"})
        img_tag      = item.find("rt-img", {"class": "posterImage"})

        movies.append({
            "title":          title_tag.get_text(strip=True) if title_tag else None,
            "release_date":   date_tag.get_text(strip=True) if date_tag else None,
            "critics_score":  critics_tag.get_text(strip=True) if critics_tag else None,
            "audience_score": audience_tag.get_text(strip=True) if audience_tag else None,
            "url":            RT_BASE_URL + link_tag["href"] if link_tag else None,
            "poster_url":     img_tag["src"] if img_tag and img_tag.get("src") else None,
            "scraped_at":     datetime.utcnow().isoformat()
        })

    return movies


def scrape_critic_reviews(movie_url, max_reviews=20):
    """
    Dado el URL de una película en RT, scrapea las reseñas de críticos.
    Ejemplo de URL: https://www.rottentomatoes.com/m/inception
    Trae: nombre del crítico, publicación, texto de la reseña, score (fresh/rotten), fecha.
    Esto es lo que NO trae TMDB y es clave para sentiment analysis.
    """
    source_url = f"{movie_url}/reviews"
    media_props = _extract_media_props(movie_url)
    if not media_props:
        return []

    raw_reviews = _fetch_reviews_from_api(
        api_url=media_props["api_url"],
        review_type="critic",
        max_reviews=max_reviews,
    )

    reviews = []
    for item in raw_reviews:
        review_text = item.get("reviewQuote")
        if not review_text:
            continue

        critic = item.get("critic") or {}
        publication = item.get("publication") or {}

        reviews.append({
            "critic": critic.get("displayName"),
            "publication": publication.get("name"),
            "review_text": review_text,
            "sentiment": item.get("scoreSentiment"),
            "original_score": item.get("originalScore"),
            "is_top_critic": bool(critic.get("isTopCritic")),
            "date": item.get("createDate"),
            "source_url": source_url,
            "publication_review_url": item.get("publicationReviewUrl"),
            "scraped_at": datetime.utcnow().isoformat(),
        })

    return reviews


def scrape_audience_reviews(movie_url, max_reviews=20):
    """
    Scrapea reseñas de audiencia (usuarios comunes, no críticos).
    Complementa las reseñas de críticos con opinión del público general.
    Trae: usuario, rating (1-5 estrellas), texto de la reseña, fecha.
    """
    source_url = f"{movie_url}/reviews"
    media_props = _extract_media_props(movie_url)
    if not media_props:
        return []

    raw_reviews = _fetch_reviews_from_api(
        api_url=media_props["api_url"],
        review_type="audience",
        max_reviews=max_reviews,
    )

    reviews = []
    for item in raw_reviews:
        review_text = item.get("review")
        if not review_text:
            continue

        user = item.get("user") or {}

        reviews.append({
            "user": item.get("displayName") or user.get("displayName") or "Anonymous",
            "rating": item.get("rating"),
            "is_verified": bool(item.get("isVerified")),
            "review_text": review_text,
            "date": item.get("createDate"),
            "source_url": source_url,
            "scraped_at": datetime.utcnow().isoformat(),
        })

    return reviews


def scrape_reviews_for_movies(movies, max_movies=5, max_reviews_per_movie=10):
    """
    Función de alto nivel que combina todo:
    dado una lista de películas (resultado de scrape_movies_in_theaters),
    scrapea reseñas de críticos y audiencia para las primeras N películas.

    max_movies=5 por defecto para no sobrecargar RT con demasiados requests.
    """
    results = []

    for movie in movies[:max_movies]:
        movie_url = movie.get("url")
        title = movie.get("title")

        if not movie_url:
            continue

        print(f"Scraping reviews for: {title}")

        critic_reviews   = scrape_critic_reviews(movie_url, max_reviews=max_reviews_per_movie)
        audience_reviews = scrape_audience_reviews(movie_url, max_reviews=max_reviews_per_movie)

        results.append({
            "title":            title,
            "url":              movie_url,
            "critics_score":    movie.get("critics_score"),
            "audience_score":   movie.get("audience_score"),
            "critic_reviews":   critic_reviews,
            "audience_reviews": audience_reviews,
            "total_critic_reviews":   len(critic_reviews),
            "total_audience_reviews": len(audience_reviews),
            "scraped_at":       datetime.utcnow().isoformat()
        })

        # Pausa entre requests para no ser bloqueado por RT
        time.sleep(2)

    return results