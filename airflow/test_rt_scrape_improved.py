import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}
RT_BASE = "https://www.rottentomatoes.com"
BROWSE_PATH = "/browse/movies_at_home/sort:popular"

def fetch_page(path):
    url = RT_BASE + path
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

def parse_browse(html):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.find_all("div", {"data-qa": "discovery-media-list-item"})
    results = []
    if items:
        for item in items:
            title_tag = item.find("span", {"data-qa": "discovery-media-list-item-title"})
            date_tag = item.find("span", {"data-qa": "discovery-media-list-item-start-date"})
            critics_tag = item.find("rt-text", {"slot": "criticsScore"}) or item.select_one("[data-qa='critics-score']")
            audience_tag = item.find("rt-text", {"slot": "audienceScore"}) or item.select_one("[data-qa='audience-score']")
            link_tag = item.find("a", {"data-qa": "discovery-media-list-item-caption"}) or item.find("a", href=re.compile(r"^/m/"))
            img = item.find("rt-img") or item.find("img")
            title = title_tag.get_text(strip=True) if title_tag else None
            critics = (critics_tag.get_text(strip=True) if critics_tag else None)
            audience = (audience_tag.get_text(strip=True) if audience_tag else None)
            url = (RT_BASE + link_tag["href"]) if link_tag and link_tag.get("href") else None
            poster = img.get("src") if img and img.get("src") else None
            results.append({
                "title": title,
                "release_date": date_tag.get_text(strip=True) if date_tag else None,
                "critics_score": critics,
                "audience_score": audience,
                "url": url,
                "poster": poster,
            })
        return results

    # Fallback to anchors /m/
    anchors = soup.find_all("a", href=re.compile(r"^/m/"))
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href or href in seen:
            continue
        seen.add(href)
        # Try to extract a clean title from child spans or text
        title = None
        title_tag = a.find("span")
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            text = a.get_text(" ", strip=True)
            # Remove words like 'Link to' and percentage patterns
            text = re.sub(r"Link to", "", text, flags=re.I).strip()
            text = re.sub(r"\\d+%","", text).strip()
            title = text or None
        img = a.find("img")
        poster = img.get("src") if img and img.get("src") else None
        results.append({
            "title": title,
            "release_date": None,
            "critics_score": None,
            "audience_score": None,
            "url": RT_BASE + href,
            "poster": poster,
        })
    return results

def main():
    try:
        html = fetch_page(BROWSE_PATH)
    except Exception as e:
        print("Request failed:", e)
        return

    items = parse_browse(html)
    print("Found:", len(items))
    for i, it in enumerate(items[:50], 1):
        print(f"{i:2}. title={it['title']!s} | critics={it['critics_score']!s} | audience={it['audience_score']!s} | date={it['release_date']!s} | url={it['url']!s}")

    if not items:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        fname = f"rt_browse_failed_{ts}.html"
        with open(fname, "w", encoding="utf-8") as fh:
            fh.write(html[:20000])
        print("Saved HTML snippet to", fname)

if __name__ == "__main__":
    main()