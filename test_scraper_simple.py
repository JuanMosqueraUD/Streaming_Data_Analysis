#!/usr/bin/env python3
import sys
from pathlib import Path
import json
from datetime import datetime

sys.path.insert(0, str(Path('.') / 'airflow' / 'utils'))

from rt_scraper import scrape_movies_at_home

print("=" * 80)
print("SCRAPING MOVIES AT HOME")
print("=" * 80)

movies = scrape_movies_at_home()
print(f"\nTotal movies: {len(movies)}\n")

print("FIRST 15 MOVIES:")
print("-" * 80)
for i, m in enumerate(movies[:15], 1):
    title = m.get('title', '')
    critics = m.get('critics_score')
    audience = m.get('audience_score')
    date = m.get('release_date')
    
    print(f"{i:2}. Title: {title!r}")
    print(f"    Critics: {critics} | Audience: {audience} | Date: {date}")
    
    # Check for problems
    problems = []
    if 'Streaming' in title:
        problems.append("HAS 'Streaming'")
    if 'Link to' in title:
        problems.append("HAS 'Link to'")
    if critics is None:
        problems.append("critics=None")
    if audience is None:
        problems.append("audience=None")
    
    if problems:
        print(f"    ISSUES: {', '.join(problems)}")
    print()

# Statistics
print("=" * 80)
print("STATISTICS")
print("=" * 80)
has_streaming = sum(1 for m in movies if 'Streaming' in m.get('title', ''))
has_link_to = sum(1 for m in movies if 'Link to' in m.get('title', ''))
has_critics = sum(1 for m in movies if m.get('critics_score'))
has_audience = sum(1 for m in movies if m.get('audience_score'))
has_date = sum(1 for m in movies if m.get('release_date'))

print(f"Movies with 'Streaming' in title: {has_streaming}")
print(f"Movies with 'Link to' in title:   {has_link_to}")
print(f"Movies with critics_score:       {has_critics}")
print(f"Movies with audience_score:      {has_audience}")
print(f"Movies with release_date:        {has_date}")

# Save JSON
output = {
    "results": movies,
    "total": len(movies),
    "ingested_at": datetime.utcnow().isoformat(),
    "source": "rottentomatoes_at_home"
}

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
filename = f"datalake_bronze/rottentomatoes_at_home_NEWTEST_{timestamp}.json"

with open(filename, 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"\nSaved to: {filename}")
