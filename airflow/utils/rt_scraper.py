import requests
from bs4 import BeautifulSoup
from datetime import datetime

RT_URL = "https://www.rottentomatoes.com/browse/movies_in_theaters/sort:popular"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def scrape_movies_in_theaters():
    page = requests.get(RT_URL, headers=HEADERS)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")

    movies = []
    items = soup.find_all("div", {"data-qa": "discovery-media-list-item"})

    for item in items:
        title_tag   = item.find("span", {"data-qa": "discovery-media-list-item-title"})
        date_tag    = item.find("span", {"data-qa": "discovery-media-list-item-start-date"})
        critics_tag = item.find("rt-text", {"slot": "criticsScore"})
        audience_tag= item.find("rt-text", {"slot": "audienceScore"})
        link_tag    = item.find("a", {"data-qa": "discovery-media-list-item-caption"})
        img_tag     = item.find("rt-img", {"class": "posterImage"})

        movies.append({
            "title":          title_tag.get_text(strip=True) if title_tag else None,
            "release_date":   date_tag.get_text(strip=True) if date_tag else None,
            "critics_score":  critics_tag.get_text(strip=True) if critics_tag else None,
            "audience_score": audience_tag.get_text(strip=True) if audience_tag else None,
            "url":            "https://www.rottentomatoes.com" + link_tag["href"] if link_tag else None,
            "poster_url":     img_tag["src"] if img_tag else None,
            "scraped_at":     datetime.utcnow().isoformat()
        })

    return movies