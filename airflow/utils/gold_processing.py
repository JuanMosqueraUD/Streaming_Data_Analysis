import logging
import os
import re
from collections import Counter
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from textblob import TextBlob
from pyspark.sql.functions import (
    col, date_format,
    avg, length, count
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────

def create_spark_session(app_name: str = "GoldProcessing") -> SparkSession:
    """Create a SparkSession configured for local execution within Airflow/Docker."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Silver readers — one per family
# ─────────────────────────────────────────────

def _read_family(spark: SparkSession, silver_dir: str, prefix: str) -> Optional[DataFrame]:
    """
    Read all parquet files for a Silver family, normalizing all columns to STRING
    to handle schema drift across runs (e.g. release_year as DOUBLE in old files
    vs STRING in new files). Numeric casts happen explicitly in each aggregation.
    """
    import glob
    pattern = os.path.join(silver_dir, f"{prefix}_*.parquet")
    paths = glob.glob(pattern)
    if not paths:
        logger.warning("No Silver files found for prefix '%s' in %s", prefix, silver_dir)
        return None

    # Read each file via pandas (avoids Spark schema conflict), cast all to str, then union.
    frames = []
    for path in sorted(paths):
        try:
            pdf = pd.read_parquet(path).astype(str).replace("nan", None)
            frames.append(pdf)
        except Exception as e:
            logger.warning("Skipping file %s — could not read: %s", path, e)

    if not frames:
        logger.warning("All files skipped for prefix '%s'", prefix)
        return None

    combined = pd.concat(frames, ignore_index=True)
    df = spark.createDataFrame(combined)
    logger.info("Read %s records from %s files (prefix=%s)", len(combined), len(frames), prefix)
    return df


# ─────────────────────────────────────────────
# Governance KPIs
# ─────────────────────────────────────────────

def _governance_for_family(df: DataFrame, family: str, run_stamp: str) -> list:
    """Compute governance KPIs for a single Silver family DataFrame."""
    records = []
    now = datetime.utcnow().isoformat()
    pdf = df.toPandas().copy()
    total = len(pdf)

    for col_name in pdf.columns:
        null_count = int(pdf[col_name].isna().sum())
        null_rate = round(null_count / total * 100, 2) if total else 0.0
        records.append({
            "kpi_type": "null_rate",
            "metric_name": f"null_rate_{family}_{col_name}",
            "family": family,
            "column_name": col_name,
            "value": null_rate,
            "unit": "percentage",
            "computed_at": now,
            "run_stamp": run_stamp,
        })

    records.append({
        "kpi_type": "volume",
        "metric_name": f"total_records_{family}",
        "family": family,
        "column_name": None,
        "value": float(total),
        "unit": "count",
        "computed_at": now,
        "run_stamp": run_stamp,
    })

    if "canonical_title" in pdf.columns:
        unique_titles = pdf["canonical_title"].dropna().nunique()
        dup_rate = round((total - unique_titles) / total * 100, 2) if total else 0.0
        records.append({
            "kpi_type": "duplicates",
            "metric_name": f"duplicate_rate_{family}",
            "family": family,
            "column_name": "canonical_title",
            "value": dup_rate,
            "unit": "percentage",
            "computed_at": now,
            "run_stamp": run_stamp,
        })

    if "source" in pdf.columns:
        for source_name, count in pdf["source"].fillna("UNKNOWN").value_counts().items():
            records.append({
                "kpi_type": "volume_by_source",
                "metric_name": f"volume_{family}_{source_name}",
                "family": family,
                "column_name": "source",
                "value": float(count),
                "unit": "count",
                "computed_at": now,
                "run_stamp": run_stamp,
            })

    for numeric_col in [c for c in pdf.columns if pd.api.types.is_numeric_dtype(pdf[c])]:
        numeric = pd.to_numeric(pdf[numeric_col], errors="coerce")
        valid = numeric.dropna()
        if valid.empty:
            continue
        mean = float(valid.mean())
        std = float(valid.std(ddof=0))
        threshold = 2 * std if std else 0.0
        outlier_rate = round(float(((valid < mean - threshold) | (valid > mean + threshold)).mean() * 100), 2)
        records.append({
            "kpi_type": "outlier_rate",
            "metric_name": f"outlier_rate_{family}_{numeric_col}",
            "family": family,
            "column_name": numeric_col,
            "value": outlier_rate,
            "unit": "percentage",
            "computed_at": now,
            "run_stamp": run_stamp,
        })

    for text_col in [c for c in pdf.columns if "text" in c.lower() and pd.api.types.is_object_dtype(pdf[c])]:
        text_series = pdf[text_col].fillna("")
        if text_series.empty:
            continue
        lengths = text_series.astype(str).str.len()
        records.append({
            "kpi_type": "text_statistics",
            "metric_name": f"min_text_length_{family}_{text_col}",
            "family": family,
            "column_name": text_col,
            "value": float(lengths.min()),
            "unit": "characters",
            "computed_at": now,
            "run_stamp": run_stamp,
        })
        records.append({
            "kpi_type": "text_statistics",
            "metric_name": f"max_text_length_{family}_{text_col}",
            "family": family,
            "column_name": text_col,
            "value": float(lengths.max()),
            "unit": "characters",
            "computed_at": now,
            "run_stamp": run_stamp,
        })

    schema_required = {
        "rt_reviews": ["canonical_title", "review_text", "review_date"],
        "titles": ["canonical_title", "source"],
        "tmdb_reviews": ["canonical_title", "review_text", "review_date"],
    }.get(family, ["canonical_title"])
    available = [c for c in schema_required if c in pdf.columns]
    if available:
        compliance = float(pdf[available].notna().all(axis=1).mean() * 100)
        records.append({
            "kpi_type": "schema_compliance",
            "metric_name": f"schema_compliance_rate_{family}",
            "family": family,
            "column_name": ",".join(available),
            "value": round(compliance, 2),
            "unit": "percentage",
            "computed_at": now,
            "run_stamp": run_stamp,
        })

    date_col = next((c for c in ("review_date", "scraped_at", "processed_at") if c in pdf.columns), None)
    if date_col:
        parsed = pd.to_datetime(pdf[date_col], errors="coerce")
        valid_dates = parsed.dropna()
        if not valid_dates.empty:
            days_span = (valid_dates.max() - valid_dates.min()).days + 1
            frequency_rate = round((valid_dates.nunique() / max(days_span, 1)) * 100, 2)
            records.append({
                "kpi_type": "ingestion_frequency",
                "metric_name": f"ingestion_frequency_compliance_{family}",
                "family": family,
                "column_name": date_col,
                "value": frequency_rate,
                "unit": "percentage",
                "computed_at": now,
                "run_stamp": run_stamp,
            })

    records.extend([
        {"kpi_type": "definition", "metric_name": "governance_definition_3_1_overview", "family": family, "column_name": "3.1 Overview", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_1_null_rate", "family": family, "column_name": "3.2.1 Null Rate per Field", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_2_volume_metrics", "family": family, "column_name": "3.2.2 Volume Metrics", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_3_duplicate_rate", "family": family, "column_name": "3.2.3 Duplicate Rate", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_4_schema_compliance", "family": family, "column_name": "3.2.4 Schema Compliance Rate", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_5_outlier_rate", "family": family, "column_name": "3.2.5 Outlier Rate per Numeric Field", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_6_text_length", "family": family, "column_name": "3.2.6 Text Length Statistics", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"kpi_type": "definition", "metric_name": "governance_definition_3_2_7_ingestion_frequency", "family": family, "column_name": "3.2.7 Ingestion Frequency Compliance", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
    ])

    return records


def compute_governance_kpis(spark: SparkSession, run_stamp: str, silver_dir: str) -> DataFrame:
    """Compute governance KPIs reading each Silver family separately."""
    all_records = []

    families = {
        "rt_reviews": _read_family(spark, silver_dir, "silver_rt_reviews"),
        "titles":     _read_family(spark, silver_dir, "silver_titles"),
        "tmdb_reviews": _read_family(spark, silver_dir, "silver_tmdb_reviews"),
    }

    for family, df in families.items():
        if df is None:
            logger.warning("Skipping governance for family '%s' — no files found", family)
            continue
        records = _governance_for_family(df, family, run_stamp)
        all_records.extend(records)
        logger.info("Governance: %s KPIs computed for family '%s'", len(records), family)

    if not all_records:
        logger.warning("No governance KPIs computed across all families.")
        schema = ("kpi_type STRING, metric_name STRING, family STRING, column_name STRING, "
                  "value DOUBLE, unit STRING, computed_at STRING, run_stamp STRING")
        return spark.createDataFrame([], schema)

    return spark.createDataFrame(pd.DataFrame(all_records))


# ─────────────────────────────────────────────
# Storytelling aggregations
# ─────────────────────────────────────────────

def _text_tokens(text: str) -> list:
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return [token for token in text.split() if len(token) > 2]


def _nlp_sentiment_label(text: str) -> str:
    polarity = TextBlob(text).sentiment.polarity
    if polarity >= 0.1:
        return "POSITIVE"
    if polarity <= -0.1:
        return "NEGATIVE"
    return "NEUTRAL"


def _ensure_sentiment_label(pdf: pd.DataFrame, text_col: str = "review_text_clean") -> pd.DataFrame:
    """Populate sentiment_label from NLP when the original field is missing or empty."""
    if "sentiment_label" not in pdf.columns:
        pdf = pdf.copy()
        pdf["sentiment_label"] = None

    text_source = text_col if text_col in pdf.columns else "review_text" if "review_text" in pdf.columns else None
    if text_source is None:
        return pdf

    has_real_sentiment = pdf["sentiment_label"].notna().astype(bool).any()
    if not has_real_sentiment:
        pdf["sentiment_label"] = pdf[text_source].fillna("").astype(str).apply(_nlp_sentiment_label)
    return pdf


def _safe_float(value) -> float:
    try:
        return float(value) if pd.notna(value) else 0.0
    except Exception:
        return 0.0


def compute_storytelling_aggregations(spark: SparkSession, run_stamp: str, silver_dir: str) -> DataFrame:
    """Compute storytelling aggregations reading each Silver family separately."""
    records = []
    now = datetime.utcnow().isoformat()

    # ── rt_reviews ──────────────────────────────
    rt = _read_family(spark, silver_dir, "silver_rt_reviews")
    if rt is not None:
        rt_pdf = rt.toPandas().copy()
        rt_pdf = _ensure_sentiment_label(rt_pdf, "review_text_clean")
        for col_name in ("critics_score", "audience_score", "rating"):
            if col_name in rt_pdf.columns:
                rt_pdf[col_name] = pd.to_numeric(rt_pdf[col_name], errors="coerce")
        sentiment_counts = rt_pdf["sentiment_label"].fillna("UNKNOWN").value_counts().to_dict()
        if sentiment_counts:
            for label, count in sentiment_counts.items():
                records.append({
                    "aggregation_type": "sentiment_distribution",
                    "metric_name": f"rt_sentiment_{label}",
                    "dimension": "sentiment_label",
                    "dimension_value": str(label),
                    "family": "rt_reviews",
                    "value": float(count),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if {"review_date", "sentiment_label"}.issubset(rt_pdf.columns):
            trend = (rt_pdf.assign(day=pd.to_datetime(rt_pdf["review_date"], errors="coerce").dt.normalize())
                     .groupby(["day", "sentiment_label"], dropna=False).size().reset_index(name="count"))
            for _, row in trend.iterrows():
                records.append({
                    "aggregation_type": "sentiment_trend",
                    "metric_name": f"rt_sentiment_trend_{row['day'].date()}_{row['sentiment_label']}",
                    "dimension": "day",
                    "dimension_value": str(row["day"].date()),
                    "family": "rt_reviews",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if {"critics_score", "audience_score", "canonical_title"}.issubset(rt_pdf.columns):
            score_summary = (rt_pdf.groupby("canonical_title", dropna=False)[["critics_score", "audience_score"]]
                             .mean().reset_index())
            for _, row in score_summary.head(10).iterrows():
                records.append({
                    "aggregation_type": "critic_vs_audience",
                    "metric_name": f"rt_critic_vs_audience_{row['canonical_title']}",
                    "dimension": "canonical_title",
                    "dimension_value": str(row["canonical_title"]),
                    "family": "rt_reviews",
                    "value": round(_safe_float(row["critics_score"]) - _safe_float(row["audience_score"]), 2),
                    "unit": "score_delta",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if "review_text_clean" in rt_pdf.columns:
            tokens = Counter()
            for text in rt_pdf["review_text_clean"].dropna().astype(str):
                tokens.update(_text_tokens(text))
            for token, count in tokens.most_common(10):
                records.append({
                    "aggregation_type": "top_keywords",
                    "metric_name": f"rt_top_keyword_{token}",
                    "dimension": "keyword",
                    "dimension_value": token,
                    "family": "rt_reviews",
                    "value": float(count),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if "source" in rt_pdf.columns:
            by_source = rt_pdf.groupby("source", dropna=False).size().reset_index(name="count")
            for _, row in by_source.iterrows():
                records.append({
                    "aggregation_type": "source_volume",
                    "metric_name": f"rt_source_volume_{row['source']}",
                    "dimension": "source",
                    "dimension_value": str(row["source"]),
                    "family": "rt_reviews",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        # Top reviewers
        if "reviewer_name" in rt.columns:
            for rank, row in enumerate(
                rt.groupBy("reviewer_name").count().orderBy(col("count").desc()).limit(10).collect(), 1
            ):
                records.append({
                    "aggregation_type": "top_reviewers",
                    "metric_name": f"rt_reviewer_rank_{rank}",
                    "dimension": "reviewer_name",
                    "dimension_value": str(row["reviewer_name"]),
                    "family": "rt_reviews",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        # Review volume by date
        if "review_date" in rt.columns:
            try:
                daily = (rt.filter(col("review_date").isNotNull())
                           .groupBy(date_format(col("review_date"), "yyyy-MM-dd").alias("date"))
                           .count().collect())
                for row in daily:
                    records.append({
                        "aggregation_type": "volume_by_date",
                        "metric_name": f"rt_volume_{row['date']}",
                        "dimension": "date",
                        "dimension_value": str(row["date"]),
                        "family": "rt_reviews",
                        "value": float(row["count"]),
                        "unit": "count",
                        "computed_at": now,
                        "run_stamp": run_stamp,
                    })
            except Exception as e:
                logger.warning("Could not compute RT temporal trend: %s", e)

        # Average rating
        if "rating" in rt.columns:
            try:
                avg_r = rt.filter(col("rating").isNotNull()).agg(avg(col("rating").cast("double")).alias("avg")).collect()[0]
                if avg_r["avg"] is not None:
                    records.append({
                        "aggregation_type": "avg_rating_by_source",
                        "metric_name": "rt_avg_rating",
                        "dimension": "source",
                        "dimension_value": "rt",
                        "family": "rt_reviews",
                        "value": round(avg_r["avg"], 2),
                        "unit": "score",
                        "computed_at": now,
                        "run_stamp": run_stamp,
                    })
            except Exception as e:
                logger.warning("Could not compute RT average rating: %s", e)

    # ── titles ───────────────────────────────────
    titles = _read_family(spark, silver_dir, "silver_titles")
    if titles is not None:
        titles_pdf = titles.toPandas().copy()
        for col_name in ("vote_average", "popularity", "rating"):
            if col_name in titles_pdf.columns:
                titles_pdf[col_name] = pd.to_numeric(titles_pdf[col_name], errors="coerce")
        if "release_year" in titles_pdf.columns:
            release_period = titles_pdf.groupby("release_year", dropna=False).size().reset_index(name="count")
            for _, row in release_period.head(10).iterrows():
                records.append({
                    "aggregation_type": "release_period",
                    "metric_name": f"title_release_period_{row['release_year']}",
                    "dimension": "release_year",
                    "dimension_value": str(row["release_year"]),
                    "family": "titles",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })
        # Top titles by vote_average
        if "vote_average" in titles.columns and "canonical_title" in titles.columns:
            for rank, row in enumerate(
                titles.filter(col("vote_average").isNotNull())
                      .withColumn("vote_average_d", col("vote_average").cast("double"))
                      .orderBy(col("vote_average_d").desc()).limit(15).collect(), 1
            ):
                records.append({
                    "aggregation_type": "top_titles_by_rating",
                    "metric_name": f"title_rank_{rank}",
                    "dimension": "canonical_title",
                    "dimension_value": str(row["canonical_title"]),
                    "family": "titles",
                    "value": float(row["vote_average_d"]) if row["vote_average_d"] is not None else 0.0,
                    "unit": "score",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        # Volume by source
        if "source" in titles.columns:
            for row in titles.groupBy("source").count().collect():
                records.append({
                    "aggregation_type": "volume_by_source",
                    "metric_name": f"titles_volume_{row['source']}",
                    "dimension": "source",
                    "dimension_value": str(row["source"]),
                    "family": "titles",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        # Avg popularity
        if "popularity" in titles.columns:
            try:
                pop = titles.agg(avg(col("popularity").cast("double")).alias("avg")).collect()[0]
                if pop["avg"] is not None:
                    records.append({
                        "aggregation_type": "avg_popularity",
                        "metric_name": "titles_avg_popularity",
                        "dimension": "global",
                        "dimension_value": "all",
                        "family": "titles",
                        "value": round(pop["avg"], 2),
                        "unit": "score",
                        "computed_at": now,
                        "run_stamp": run_stamp,
                    })
            except Exception as e:
                logger.warning("Could not compute avg popularity: %s", e)

    # ── tmdb_reviews ─────────────────────────────
    tmdb = _read_family(spark, silver_dir, "silver_tmdb_reviews")
    if tmdb is not None:
        tmdb_pdf = tmdb.toPandas().copy()
        tmdb_pdf = _ensure_sentiment_label(tmdb_pdf, "review_text_clean")
        for col_name in ("rating", "score", "popularity", "vote_average"):
            if col_name in tmdb_pdf.columns:
                tmdb_pdf[col_name] = pd.to_numeric(tmdb_pdf[col_name], errors="coerce")
        sentiment_counts = tmdb_pdf["sentiment_label"].fillna("UNKNOWN").value_counts().to_dict()
        if sentiment_counts:
            for label, count in sentiment_counts.items():
                records.append({
                    "aggregation_type": "sentiment_distribution",
                    "metric_name": f"tmdb_sentiment_{label}",
                    "dimension": "sentiment_label",
                    "dimension_value": str(label),
                    "family": "tmdb_reviews",
                    "value": float(count),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if "review_text_clean" in tmdb_pdf.columns:
            tokens = Counter()
            for text in tmdb_pdf["review_text_clean"].dropna().astype(str):
                tokens.update(_text_tokens(text))
            for token, count in tokens.most_common(10):
                records.append({
                    "aggregation_type": "top_keywords",
                    "metric_name": f"tmdb_top_keyword_{token}",
                    "dimension": "keyword",
                    "dimension_value": token,
                    "family": "tmdb_reviews",
                    "value": float(count),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        if {"review_date", "rating"}.issubset(tmdb_pdf.columns):
            rate_by_day = (tmdb_pdf.assign(day=pd.to_datetime(tmdb_pdf["review_date"], errors="coerce").dt.normalize())
                           .groupby("day", dropna=False)["rating"].mean().reset_index())
            for _, row in rate_by_day.head(10).iterrows():
                records.append({
                    "aggregation_type": "sentiment_trend",
                    "metric_name": f"tmdb_sentiment_trend_{row['day'].date()}",
                    "dimension": "day",
                    "dimension_value": str(row["day"].date()),
                    "family": "tmdb_reviews",
                    "value": round(_safe_float(row["rating"]), 2),
                    "unit": "score",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })
        # Top titles by review count
        if "canonical_title" in tmdb.columns:
            for rank, row in enumerate(
                tmdb.groupBy("canonical_title").count().orderBy(col("count").desc()).limit(15).collect(), 1
            ):
                records.append({
                    "aggregation_type": "top_titles_by_reviews",
                    "metric_name": f"tmdb_title_rank_{rank}",
                    "dimension": "canonical_title",
                    "dimension_value": str(row["canonical_title"]),
                    "family": "tmdb_reviews",
                    "value": float(row["count"]),
                    "unit": "count",
                    "computed_at": now,
                    "run_stamp": run_stamp,
                })

        # Average rating
        if "rating" in tmdb.columns:
            try:
                avg_r = tmdb.filter(col("rating").isNotNull()).agg(avg(col("rating").cast("double")).alias("avg")).collect()[0]
                if avg_r["avg"] is not None:
                    records.append({
                        "aggregation_type": "avg_rating_by_source",
                        "metric_name": "tmdb_avg_rating",
                        "dimension": "source",
                        "dimension_value": "tmdb",
                        "family": "tmdb_reviews",
                        "value": round(avg_r["avg"], 2),
                        "unit": "score",
                        "computed_at": now,
                        "run_stamp": run_stamp,
                    })
            except Exception as e:
                logger.warning("Could not compute TMDB average rating: %s", e)

        # Volume by date
        if "review_date" in tmdb.columns:
            try:
                daily = (tmdb.filter(col("review_date").isNotNull())
                             .groupBy(date_format(col("review_date"), "yyyy-MM-dd").alias("date"))
                             .count().collect())
                for row in daily:
                    records.append({
                        "aggregation_type": "volume_by_date",
                        "metric_name": f"tmdb_volume_{row['date']}",
                        "dimension": "date",
                        "dimension_value": str(row["date"]),
                        "family": "tmdb_reviews",
                        "value": float(row["count"]),
                        "unit": "count",
                        "computed_at": now,
                        "run_stamp": run_stamp,
                    })
            except Exception as e:
                logger.warning("Could not compute TMDB temporal trend: %s", e)

    records.extend([
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_1_dashboard_narrative", "dimension": "4.1 Dashboard Narrative", "dimension_value": "Narrative summary for dashboards", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_1_sentiment_distribution", "dimension": "4.2.1 Sentiment Distribution", "dimension_value": "Sentiment distribution", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_2_sentiment_trend", "dimension": "4.2.2 Sentiment Trend Over Time", "dimension_value": "Sentiment trend over time", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_3_critic_vs_audience", "dimension": "4.2.3 Critic vs Audience Score Comparison", "dimension_value": "Score comparison", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_4_top_keywords", "dimension": "4.2.4 Top Keywords and N-grams", "dimension_value": "Keyword and n-gram summary", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_5_source_volume", "dimension": "4.2.5 Source Volume and Activity Trends", "dimension_value": "Source volume and activity", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
        {"aggregation_type": "definition", "metric_name": "storytelling_definition_4_2_6_release_period", "dimension": "4.2.6 Top Content by Release Period", "dimension_value": "Release period analysis", "family": "dashboard", "value": 1.0, "unit": "definition", "computed_at": now, "run_stamp": run_stamp},
    ])

    if not records:
        logger.warning("No storytelling aggregations computed.")
        schema = ("aggregation_type STRING, metric_name STRING, dimension STRING, "
                  "dimension_value STRING, family STRING, value DOUBLE, unit STRING, "
                  "computed_at STRING, run_stamp STRING")
        return spark.createDataFrame([], schema)

    logger.info("Computed %s storytelling aggregations", len(records))
    return spark.createDataFrame(pd.DataFrame(records))


# ─────────────────────────────────────────────
# Writer
# ─────────────────────────────────────────────

def write_gold_parquet(
    df: DataFrame,
    output_dir: str,
    prefix: str,
    run_stamp: str,
) -> Optional[str]:
    """Write Spark DataFrame to Parquet in Gold layer with timestamped folder name."""
    os.makedirs(output_dir, exist_ok=True)
    folder_name = f"{prefix}_{run_stamp}.parquet"
    file_path = os.path.join(output_dir, folder_name)

    try:
        df.coalesce(1).write.mode("overwrite").parquet(file_path)
        logger.info("Successfully wrote Gold Parquet to %s", file_path)
        return file_path
    except Exception as e:
        logger.error("Failed to write Gold Parquet to %s: %s", file_path, e)
        raise