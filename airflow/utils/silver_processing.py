import json
import logging
import os
import re
from datetime import datetime
from html import unescape
from typing import Dict, List, Optional

import pandas as pd

from utils.bronze_file_utils import detect_bronze_source_type

logger = logging.getLogger(__name__)


SILVER_TITLES_COLUMNS = [
    "canonical_title",
    "source_title",
    "source",
    "source_type",
    "source_id",
    "url",
    "poster_url",
    "release_date",
    "release_year",
    "popularity",
    "vote_average",
    "vote_count",
    "critics_score",
    "audience_score",
    "overview",
    "genre_ids",
    "scraped_at",
    "bronze_file_name",
    "bronze_source_type",
    "processed_at",
]

SILVER_RT_REVIEWS_COLUMNS = [
    "canonical_title",
    "source",
    "review_type",
    "reviewer_name",
    "publication",
    "review_text",
    "review_text_clean",
    "sentiment_label",
    "original_score",
    "rating",
    "is_verified",
    "is_top_critic",
    "review_date",
    "source_url",
    "publication_review_url",
    "title_url",
    "critics_score",
    "audience_score",
    "scraped_at",
    "bronze_file_name",
    "bronze_source_type",
    "processed_at",
]

SILVER_TMDB_REVIEWS_COLUMNS = [
    "movie_id",
    "canonical_title",
    "source",
    "reviewer_name",
    "review_text",
    "review_text_clean",
    "rating",
    "review_date",
    "review_id",
    "review_url",
    "popularity",
    "vote_average",
    "vote_count",
    "release_date",
    "release_year",
    "overview",
    "genre_ids",
    "total_reviews",
    "scraped_at",
    "bronze_file_name",
    "bronze_source_type",
    "processed_at",
]


def _load_json(file_path: str) -> Dict:
    with open(file_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _to_null(value):
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def _normalize_text_whitespace(value: Optional[str]) -> Optional[str]:
    value = _to_null(value)
    if value is None:
        return None
    return re.sub(r"\s+", " ", value).strip()


def _canonical_title(value: Optional[str]) -> Optional[str]:
    text = _normalize_text_whitespace(value)
    return text.lower() if text else None


def _parse_iso_datetime(value: Optional[str]) -> Optional[str]:
    text = _to_null(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def _parse_date(value: Optional[str]) -> Optional[str]:
    text = _to_null(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _parse_rt_release_date(value: Optional[str]) -> Optional[str]:
    text = _to_null(value)
    if not text:
        return None

    prefixes = ["Opened", "Streaming"]
    cleaned = text
    for prefix in prefixes:
        if cleaned.lower().startswith(prefix.lower()):
            cleaned = cleaned[len(prefix) :].strip()
            break

    parsed = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _extract_year(iso_date: Optional[str]) -> Optional[int]:
    if not iso_date:
        return None
    try:
        return int(iso_date[:4])
    except (TypeError, ValueError):
        return None


def _parse_percent(value: Optional[str]) -> Optional[float]:
    text = _to_null(value)
    if not text:
        return None
    text = text.replace("%", "").strip()
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_bool(value) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None


def _serialize_genre_ids(genre_ids) -> Optional[str]:
    if genre_ids is None:
        return None
    if isinstance(genre_ids, list):
        try:
            normalized = [int(x) for x in genre_ids]
        except (TypeError, ValueError):
            normalized = genre_ids
        return json.dumps(normalized, ensure_ascii=False)
    return None


def _clean_review_text(value: Optional[str]) -> Optional[str]:
    text = _to_null(value)
    if not text:
        return None
    text = unescape(text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\.{2,}", ".", text)
    return text


def _ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]


def _stringify_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def _drop_duplicates(df: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return df

    dedup_df = df.copy()
    for col in key_columns:
        if col not in dedup_df.columns:
            dedup_df[col] = None

    dedup_df["__dedup_key"] = dedup_df[key_columns].apply(
        lambda row: "||".join(_stringify_value(v) for v in row),
        axis=1,
    )
    dedup_df = dedup_df.drop_duplicates(subset=["__dedup_key"], keep="first").drop(columns=["__dedup_key"])
    return dedup_df


def _merge_group_values(values):
    normalized_values = []
    seen = set()

    for value in values:
        if pd.isna(value):
            continue
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                continue

        if isinstance(value, (list, dict)):
            key = json.dumps(value, sort_keys=True, ensure_ascii=False)
        else:
            key = str(value)

        if key in seen:
            continue
        seen.add(key)
        normalized_values.append(value)

    if not normalized_values:
        return None
    if len(normalized_values) == 1:
        return normalized_values[0]
    return ", ".join(str(v) for v in normalized_values)


def _merge_duplicate_titles(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    merged_records: List[Dict] = []
    for canonical_title, group in df.groupby("canonical_title", sort=False):
        record: Dict = {"canonical_title": canonical_title}
        for col in df.columns:
            if col == "canonical_title":
                continue
            record[col] = _merge_group_values(group[col].tolist())
        merged_records.append(record)

    return pd.DataFrame(merged_records, columns=df.columns)


def transform_titles(files: List[str], processed_at: str) -> pd.DataFrame:
    records: List[Dict] = []

    for file_path in files:
        file_name = os.path.basename(file_path)
        source_type = detect_bronze_source_type(file_path)
        if not source_type:
            logger.warning("Skipping unrecognized Bronze file: %s", file_name)
            continue

        payload = _load_json(file_path)
        items = payload.get("results", [])

        if source_type.startswith("tmdb_catalog"):
            for item in items:
                source_title = _normalize_text_whitespace(item.get("title"))
                release_date = _parse_date(item.get("release_date"))
                records.append(
                    {
                        "canonical_title": _canonical_title(source_title),
                        "source_title": source_title,
                        "source": "tmdb",
                        "source_type": "tmdb_catalog",
                        "source_id": item.get("id"),
                        "url": None,
                        "poster_url": None,
                        "release_date": release_date,
                        "release_year": _extract_year(release_date),
                        "popularity": item.get("popularity"),
                        "vote_average": item.get("vote_average"),
                        "vote_count": item.get("vote_count"),
                        "critics_score": None,
                        "audience_score": None,
                        "overview": _normalize_text_whitespace(item.get("overview")),
                        "genre_ids": _serialize_genre_ids(item.get("genre_ids")),
                        "scraped_at": _parse_iso_datetime(payload.get("ingested_at")),
                        "bronze_file_name": file_name,
                        "bronze_source_type": source_type,
                        "processed_at": processed_at,
                    }
                )

        elif source_type.startswith("rt_catalog"):
            for item in items:
                source_title = _normalize_text_whitespace(item.get("title"))
                release_date = _parse_rt_release_date(item.get("release_date"))
                records.append(
                    {
                        "canonical_title": _canonical_title(source_title),
                        "source_title": source_title,
                        "source": "rottentomatoes",
                        "source_type": source_type,
                        "source_id": None,
                        "url": _to_null(item.get("url")),
                        "poster_url": _to_null(item.get("poster_url")),
                        "release_date": release_date,
                        "release_year": _extract_year(release_date),
                        "popularity": None,
                        "vote_average": None,
                        "vote_count": None,
                        "critics_score": _parse_percent(item.get("critics_score")),
                        "audience_score": _parse_percent(item.get("audience_score")),
                        "overview": None,
                        "genre_ids": None,
                        "scraped_at": _parse_iso_datetime(item.get("scraped_at")),
                        "bronze_file_name": file_name,
                        "bronze_source_type": source_type,
                        "processed_at": processed_at,
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        return _ensure_columns(df, SILVER_TITLES_COLUMNS)

    # Required key fields for title-level entity quality.
    df = df[df["canonical_title"].notna()]
    df = _drop_duplicates(df, ["source", "source_title", "url", "source_id"])
    df = _merge_duplicate_titles(df)
    return _ensure_columns(df, SILVER_TITLES_COLUMNS)


def transform_rt_reviews(files: List[str], processed_at: str) -> pd.DataFrame:
    records: List[Dict] = []

    for file_path in files:
        file_name = os.path.basename(file_path)
        source_type = detect_bronze_source_type(file_path)
        if not source_type:
            logger.warning("Skipping unrecognized Bronze file: %s", file_name)
            continue

        payload = _load_json(file_path)
        titles = payload.get("results", [])

        for title_obj in titles:
            source_title = _normalize_text_whitespace(title_obj.get("title"))
            canonical_title = _canonical_title(source_title)
            title_url = _to_null(title_obj.get("url"))
            critics_score = _parse_percent(title_obj.get("critics_score"))
            audience_score = _parse_percent(title_obj.get("audience_score"))

            for critic in title_obj.get("critic_reviews", []) or []:
                raw_text = _to_null(critic.get("review_text"))
                records.append(
                    {
                        "canonical_title": canonical_title,
                        "source": "rottentomatoes",
                        "review_type": "critic",
                        "reviewer_name": _normalize_text_whitespace(critic.get("critic")),
                        "publication": _normalize_text_whitespace(critic.get("publication")),
                        "review_text": raw_text,
                        "review_text_clean": _clean_review_text(raw_text),
                        "sentiment_label": _normalize_text_whitespace(critic.get("sentiment")),
                        "original_score": _normalize_text_whitespace(critic.get("original_score")),
                        "rating": None,
                        "is_verified": None,
                        "is_top_critic": _normalize_bool(critic.get("is_top_critic")),
                        "review_date": _parse_iso_datetime(critic.get("date")),
                        "source_url": _to_null(critic.get("source_url")),
                        "publication_review_url": _to_null(critic.get("publication_review_url")),
                        "title_url": title_url,
                        "critics_score": critics_score,
                        "audience_score": audience_score,
                        "scraped_at": _parse_iso_datetime(critic.get("scraped_at")),
                        "bronze_file_name": file_name,
                        "bronze_source_type": source_type,
                        "processed_at": processed_at,
                    }
                )

            for audience in title_obj.get("audience_reviews", []) or []:
                raw_text = _to_null(audience.get("review_text"))
                records.append(
                    {
                        "canonical_title": canonical_title,
                        "source": "rottentomatoes",
                        "review_type": "audience",
                        "reviewer_name": _normalize_text_whitespace(audience.get("user")),
                        "publication": None,
                        "review_text": raw_text,
                        "review_text_clean": _clean_review_text(raw_text),
                        "sentiment_label": None,
                        "original_score": None,
                        "rating": _to_null(audience.get("rating")),
                        "is_verified": _normalize_bool(audience.get("is_verified")),
                        "is_top_critic": None,
                        "review_date": _parse_iso_datetime(audience.get("date")),
                        "source_url": _to_null(audience.get("source_url")),
                        "publication_review_url": None,
                        "title_url": title_url,
                        "critics_score": critics_score,
                        "audience_score": audience_score,
                        "scraped_at": _parse_iso_datetime(audience.get("scraped_at")),
                        "bronze_file_name": file_name,
                        "bronze_source_type": source_type,
                        "processed_at": processed_at,
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        return _ensure_columns(df, SILVER_RT_REVIEWS_COLUMNS)

    # Keep only records with minimum review identity.
    df = df[df["canonical_title"].notna() & df["review_text"].notna()]
    df = _drop_duplicates(
        df,
        ["canonical_title", "review_type", "reviewer_name", "review_date", "review_text"],
    )
    return _ensure_columns(df, SILVER_RT_REVIEWS_COLUMNS)


def transform_tmdb_reviews(files: List[str], processed_at: str) -> pd.DataFrame:
    records: List[Dict] = []

    for file_path in files:
        file_name = os.path.basename(file_path)
        source_type = detect_bronze_source_type(file_path)
        if source_type != "tmdb_reviews":
            logger.warning("Skipping non-TMDb-review file in TMDb reviews route: %s", file_name)
            continue

        payload = _load_json(file_path)
        items = payload.get("results", [])

        for entry in items:
            movie = entry.get("movie") or {}
            movie_id = movie.get("id")
            source_title = _normalize_text_whitespace(movie.get("title"))
            release_date = _parse_date(movie.get("release_date"))
            total_reviews = entry.get("total_reviews")
            scraped_at = _parse_iso_datetime(entry.get("scraped_at"))

            for review in entry.get("reviews", []) or []:
                raw_text = _to_null(review.get("content"))
                records.append(
                    {
                        "movie_id": movie_id,
                        "canonical_title": _canonical_title(source_title),
                        "source": "tmdb",
                        "reviewer_name": _normalize_text_whitespace(review.get("author")),
                        "review_text": raw_text,
                        "review_text_clean": _clean_review_text(raw_text),
                        "rating": review.get("rating"),
                        "review_date": _parse_iso_datetime(review.get("created_at")),
                        "review_id": _to_null(review.get("review_id")),
                        "review_url": _to_null(review.get("url")),
                        "popularity": movie.get("popularity"),
                        "vote_average": movie.get("vote_average"),
                        "vote_count": movie.get("vote_count"),
                        "release_date": release_date,
                        "release_year": _extract_year(release_date),
                        "overview": _normalize_text_whitespace(movie.get("overview")),
                        "genre_ids": _serialize_genre_ids(movie.get("genre_ids")),
                        "total_reviews": total_reviews,
                        "scraped_at": scraped_at,
                        "bronze_file_name": file_name,
                        "bronze_source_type": source_type,
                        "processed_at": processed_at,
                    }
                )

    df = pd.DataFrame(records)
    if df.empty:
        return _ensure_columns(df, SILVER_TMDB_REVIEWS_COLUMNS)

    # For TMDb reviews we only keep records that can be tracked by movie and review id.
    df = df[df["movie_id"].notna() & df["review_id"].notna()]
    df = _drop_duplicates(df, ["movie_id", "review_id"])
    return _ensure_columns(df, SILVER_TMDB_REVIEWS_COLUMNS)


def _build_output_file_name(prefix: str, run_stamp: str) -> str:
    return f"{prefix}_{run_stamp}.parquet"


def write_silver_parquet(df: pd.DataFrame, output_dir: str, prefix: str, run_stamp: str) -> Optional[str]:
    os.makedirs(output_dir, exist_ok=True)
    output_name = _build_output_file_name(prefix, run_stamp)
    output_path = os.path.join(output_dir, output_name)

    if df.empty:
        logger.info("No rows to write for %s. Skipping file generation.", prefix)
        return None

    df_to_write = df.copy()
    for col in df_to_write.select_dtypes(include=["object"]).columns:
        df_to_write[col] = df_to_write[col].astype("string")

    # Write to a temporary file and atomically replace to improve idempotency in retries.
    tmp_path = f"{output_path}.tmp"
    df_to_write.to_parquet(tmp_path, index=False, engine="pyarrow")
    os.replace(tmp_path, output_path)
    logger.info("Wrote %s rows to %s", len(df), output_path)
    return output_path


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
