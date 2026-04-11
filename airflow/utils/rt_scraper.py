import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RT_BASE_URL = "https://www.rottentomatoes.com"


def scrape_movies_in_theaters():
    """
    Scrapea películas actualmente en cartelera.
    Trae: título, fecha, critics score, audience score, poster, url.
    """
    url = f"{RT_BASE_URL}/browse/movies_in_theaters/sort:popular"
    page = requests.get(url, headers=HEADERS)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")

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
    reviews_url = f"{movie_url}/reviews"
    page = requests.get(reviews_url, headers=HEADERS)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")

    reviews = []
    review_items = soup.find_all("div", {"class": "review-row"})[:max_reviews]

    for item in review_items:
        # Texto de la reseña
        quote_tag = item.find("p", {"class": "review-text"})
        if not quote_tag:
            quote_tag = item.find("div", {"data-qa": "review-quote"})

        # Nombre del crítico
        critic_tag = item.find("a", {"data-qa": "review-critic-link"})
        if not critic_tag:
            critic_tag = item.find("span", {"class": "critic-name"})

        # Publicación / medio
        pub_tag = item.find("span", {"data-qa": "review-publication"})
        if not pub_tag:
            pub_tag = item.find("em", {"class": "critic-publication"})

        # Score fresh o rotten
        score_tag = item.find("score-icon-critic-deprecated")
        if not score_tag:
            score_tag = item.find("span", {"class": "review-sentiment"})

        # Fecha de la reseña
        date_tag = item.find("span", {"data-qa": "review-date"})

        review_text = quote_tag.get_text(strip=True) if quote_tag else None

        # Solo agrega si tiene texto — sin texto no sirve para NLP
        if review_text:
            reviews.append({
                "critic":       critic_tag.get_text(strip=True) if critic_tag else None,
                "publication":  pub_tag.get_text(strip=True) if pub_tag else None,
                "review_text":  review_text,
                "sentiment":    score_tag.get("sentiment") if score_tag else None,
                "date":         date_tag.get_text(strip=True) if date_tag else None,
                "source_url":   reviews_url,
                "scraped_at":   datetime.utcnow().isoformat()
            })

    return reviews


def scrape_audience_reviews(movie_url, max_reviews=20):
    """
    Scrapea reseñas de audiencia (usuarios comunes, no críticos).
    Complementa las reseñas de críticos con opinión del público general.
    Trae: usuario, rating (1-5 estrellas), texto de la reseña, fecha.
    """
    audience_url = f"{movie_url}/reviews?type=user"
    page = requests.get(audience_url, headers=HEADERS)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")

    reviews = []
    review_items = soup.find_all("div", {"data-qa": "audience-review-item"})[:max_reviews]

    for item in review_items:
        # Texto de la reseña
        text_tag = item.find("p", {"data-qa": "audience-review-body"})

        # Usuario
        user_tag = item.find("span", {"data-qa": "audience-reviewer-name"})

        # Rating en estrellas
        rating_tag = item.find("span", {"data-qa": "audience-reviewer-score"})
        if not rating_tag:
            rating_tag = item.find("score-icon-audience")

        # Fecha
        date_tag = item.find("span", {"data-qa": "audience-review-date"})

        review_text = text_tag.get_text(strip=True) if text_tag else None

        if review_text:
            reviews.append({
                "user":         user_tag.get_text(strip=True) if user_tag else "Anonymous",
                "rating":       rating_tag.get("value") if rating_tag else None,
                "review_text":  review_text,
                "date":         date_tag.get_text(strip=True) if date_tag else None,
                "source_url":   audience_url,
                "scraped_at":   datetime.utcnow().isoformat()
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