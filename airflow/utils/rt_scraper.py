import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import time
import os
import re

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

RT_BASE_URL = "https://www.rottentomatoes.com"


def _clean_title(raw_text):
    """
    Limpia un título extrayendo solo el nombre de película.
    Remueve porcentajes, números iniciales, 'Link to', 'Streaming', fechas, etc.
    """
    if not raw_text:
        return None
    
    # Remover "Link to" y similares
    text = re.sub(r"^\s*Link to\s+", "", raw_text, flags=re.I)
    
    # Remover porcentajes y números iniciales (críticos/audiencia scores)
    # Ejemplo: "94%87%The Title Streaming..." -> "The Title Streaming..."
    text = re.sub(r"^\s*[\d%\s]+", "", text)
    
    # Remover "Streaming" y todo lo que siga (con o sin espacios antes)
    # Puede estar pegado: "MaryStreaming" o con espacios: "Mary Streaming"
    text = re.sub(r"[Ss]treaming.*$", "", text)
    text = re.sub(r"[Oo]pened.*$", "", text)
    text = re.sub(r"[Ll]ink\s+to\s+.*$", "", text)
    
    # Remover patrones de fecha al final (Month DD, YYYY, etc.)
    # Ejemplo: "The Title May 12, 2026" -> "The Title"
    text = re.sub(r"\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]?\s+\d{1,2},?\s+\d{4}\s*$", "", text, flags=re.I)
    
    # Normalizar espacios internos
    text = re.sub(r"\s+", " ", text).strip()
    
    # Si queda vacío, devolver None
    return text if text else None


def _get_soup(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
    except Exception as exc:
        logger.exception("Request failed for %s: %s", url, exc)
        raise
    return BeautifulSoup(response.text, "html.parser")


def _extract_media_props(movie_url):
    """
    Obtiene metadatos embebidos en la página de reviews para construir
    el endpoint JSON real de Rotten Tomatoes (napi/rtcf).
    """
    soup = _get_soup(f"{movie_url}/reviews")
    props_tag = soup.select_one('page-media-reviews-manager script[data-json="props"]')

    if not props_tag or not props_tag.string:
        logger.warning("No props_tag found for %s - page snippet: %s", movie_url, (soup.text[:1000] if soup else ""))
        return None

    try:
        props = json.loads(props_tag.string)
    except json.JSONDecodeError as exc:
        logger.exception("Failed to parse props JSON for %s: %s", movie_url, exc)
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

        try:
            response = requests.get(api_url, headers=HEADERS, params=request_params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.exception("Failed API request to %s with params %s: %s", api_url, request_params, exc)
            break

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


def _scrape_browse_movies(browse_path):
    """
    Scrapea una página de browse de Rotten Tomatoes que usa tarjetas
    de discovery-media-list-item.
    """
    soup = _get_soup(f"{RT_BASE_URL}{browse_path}")

    movies = []
    items = soup.find_all("div", {"data-qa": "discovery-media-list-item"})

    if not items:
        # Fallback: the page structure may have changed. Try to extract
        # movie links (hrefs like /m/<slug>) and nearby titles.
        logger.warning("No discovery-media-list-item elements found for %s. Trying fallback parser.", browse_path)

        # Save a snippet of HTML for offline debugging if possible
        try:
            logs_dir = "/opt/airflow/logs/rt_scraper_failures"
            os.makedirs(logs_dir, exist_ok=True)
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            snippet_path = os.path.join(logs_dir, f"browse_failed_{timestamp}.html")
            with open(snippet_path, "w", encoding="utf-8") as fh:
                fh.write(soup.prettify()[:20000])
            logger.info("Saved RT browse HTML snippet to %s", snippet_path)
        except Exception:
            logger.exception("Failed saving HTML snippet for %s", browse_path)

        anchors = soup.find_all("a", href=re.compile(r"^/m/"))
        seen = set()
        for a in anchors:
            href = a.get("href")
            if not href:
                continue
            if href in seen:
                continue
            seen.add(href)

            title = None
            release_date = None
            critics_score = None
            audience_score = None

            # 1) Intentar extraer título desde nodos específicos dentro del anchor
            title_elem = a.find("span", {"data-qa": "discovery-media-list-item-title"})
            if not title_elem:
                title_elem = a.find("rt-text", {"slot": "title"})
            if title_elem:
                title = _clean_title(title_elem.get_text(strip=True))

            # 2) Intentar extraer scores desde elementos rt-text con slot
            critics_tag = a.find("rt-text", {"slot": "criticsScore"})
            audience_tag = a.find("rt-text", {"slot": "audienceScore"})
            if critics_tag:
                critics_score = critics_tag.get_text(strip=True)
            if audience_tag:
                audience_score = audience_tag.get_text(strip=True)

            # 3) Si no hay scores, intentar detectar porcentajes en el texto del anchor
            full_text = a.get_text(" ", strip=True)
            if (not critics_score or not audience_score) and full_text:
                percents = re.findall(r"(\d{1,3}%)", full_text)
                if percents:
                    if not critics_score and len(percents) >= 1:
                        critics_score = percents[0]
                    if not audience_score and len(percents) >= 2:
                        audience_score = percents[1]

            # 4) Extraer fecha si aparece en texto
            date_match = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]?\s+\d{1,2},?\s+\d{4}", full_text, re.I)
            if date_match:
                release_date = date_match.group(0)

            # 5) Si aún no hay scores, consultar la página de detalle y leer media-scorecard
            if (not critics_score or not audience_score):
                try:
                    detail_soup = _get_soup(RT_BASE_URL + href)
                    media_scorecard = detail_soup.find("media-scorecard")
                    if media_scorecard:
                        # Preferir selectores por slot
                        c_tag = media_scorecard.find("rt-text", {"slot": "criticsScore"})
                        a_tag = media_scorecard.find("rt-text", {"slot": "audienceScore"})
                        if c_tag:
                            cs = c_tag.get_text(strip=True)
                            if cs:
                                critics_score = cs
                        if a_tag:
                            au = a_tag.get_text(strip=True)
                            if au:
                                audience_score = au

                        # Fallback: recorrer rt-texts y coger los porcentajes encontrados
                        if (not critics_score or not audience_score):
                            rt_texts = media_scorecard.find_all("rt-text")
                            for rt in rt_texts:
                                t = rt.get_text(strip=True)
                                if re.match(r"^\d+%$", t):
                                    if not critics_score:
                                        critics_score = t
                                    elif not audience_score:
                                        audience_score = t
                                if critics_score and audience_score:
                                    break
                except Exception:
                    logger.exception("Failed fetching detail page for %s", href)

            # 6) Si no hay título aún, intentar con spans directos y otros heurísticos
            if not title:
                spans = a.find_all("span", recursive=False)
                if spans:
                    for span in spans:
                        span_text = span.get_text(strip=True)
                        if not span_text:
                            continue
                        if re.match(r"^\d+%$", span_text):
                            if not critics_score:
                                critics_score = span_text
                            elif not audience_score:
                                audience_score = span_text
                            continue
                        if re.search(r"[Ss]treaming|[Oo]pened|[Ll]ink\s+to", span_text):
                            continue
                        if re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)", span_text):
                            if not release_date:
                                release_date = span_text
                            continue
                        title = _clean_title(span_text)
                        if title:
                            break

            # 7) Último recurso: limpiar todo el texto del anchor
            if not title:
                title = _clean_title(full_text)

            img = a.find("img") if a else None
            poster = img.get("src") if img and img.get("src") else None

            movies.append({
                "title": title,
                "release_date": release_date,
                "critics_score": critics_score,
                "audience_score": audience_score,
                "url": RT_BASE_URL + href,
                "poster_url": poster,
                "scraped_at": datetime.utcnow().isoformat(),
            })

        logger.info("Fallback parser found %s candidate anchors for %s", len(movies), browse_path)
        return movies

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


def scrape_movies_in_theaters():
    """
    Scrapea películas actualmente en cartelera.
    Trae: título, fecha, critics score, audience score, poster, url.
    """
    return _scrape_browse_movies("/browse/movies_in_theaters/sort:popular")


def scrape_movies_at_home():
    """
    Scrapea películas en streaming / at home ordenadas por popularidad.
    Complementa la cartelera con contenido disponible fuera de salas.
    """
    return _scrape_browse_movies("/browse/movies_at_home/sort:popular")


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