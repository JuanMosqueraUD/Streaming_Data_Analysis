import os
from glob import glob
from typing import Dict, List, Optional

# Filename prefix -> normalized bronze source type
BRONZE_SOURCE_TYPE_BY_PREFIX = {
    "tmdb_trending_movie_day_": "tmdb_catalog_trending_day",
    "tmdb_now_playing_": "tmdb_catalog_now_playing",
    "tmdb_movie_reviews_": "tmdb_reviews",
    "rottentomatoes_theaters_": "rt_catalog_theaters",
    "rottentomatoes_at_home_": "rt_catalog_at_home",
    "rottentomatoes_reviews_": "rt_reviews_theaters",
    "rottentomatoes_at_home_reviews_": "rt_reviews_at_home",
}

# Source type -> silver output family
SILVER_FAMILY_BY_SOURCE_TYPE = {
    "tmdb_catalog_trending_day": "silver_titles",
    "tmdb_catalog_now_playing": "silver_titles",
    "rt_catalog_theaters": "silver_titles",
    "rt_catalog_at_home": "silver_titles",
    "rt_reviews_theaters": "silver_rt_reviews",
    "rt_reviews_at_home": "silver_rt_reviews",
    "tmdb_reviews": "silver_tmdb_reviews",
}


def list_bronze_json_files(bronze_dir: str) -> List[str]:
    pattern = os.path.join(bronze_dir, "*.json")
    return sorted(glob(pattern))


def detect_bronze_source_type(file_path: str) -> Optional[str]:
    file_name = os.path.basename(file_path)
    for prefix, source_type in BRONZE_SOURCE_TYPE_BY_PREFIX.items():
        if file_name.startswith(prefix):
            return source_type
    return None


def group_files_by_silver_family(file_paths: List[str]) -> Dict[str, List[str]]:
    grouped = {
        "silver_titles": [],
        "silver_rt_reviews": [],
        "silver_tmdb_reviews": [],
        "ignored": [],
    }

    for path in file_paths:
        source_type = detect_bronze_source_type(path)
        if not source_type:
            grouped["ignored"].append(path)
            continue

        family = SILVER_FAMILY_BY_SOURCE_TYPE.get(source_type)
        if not family:
            grouped["ignored"].append(path)
            continue

        grouped[family].append(path)

    return grouped
