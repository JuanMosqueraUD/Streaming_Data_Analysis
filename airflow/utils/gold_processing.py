import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, isnull, date_format,
    avg, max, min, length, count
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
    total = df.count()

    # 1. Null rate per column
    for col_name in df.columns:
        null_count = df.filter(isnull(col(col_name))).count()
        null_rate = round(null_count / total * 100, 2) if total > 0 else 0.0
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

    # 2. Total records
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

    # 3. Duplicate rate by canonical_title
    if "canonical_title" in df.columns:
        unique_titles = df.select("canonical_title").distinct().count()
        dup_rate = round((total - unique_titles) / total * 100, 2) if total > 0 else 0.0
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

    # 4. Volume by source
    if "source" in df.columns:
        for row in df.groupBy("source").count().collect():
            records.append({
                "kpi_type": "volume_by_source",
                "metric_name": f"volume_{family}_{row['source']}",
                "family": family,
                "column_name": "source",
                "value": float(row["count"]),
                "unit": "count",
                "computed_at": now,
                "run_stamp": run_stamp,
            })

    # 5. Text statistics for text columns
    text_cols = [c for c in df.columns
                 if "text" in c.lower() and df.schema[c].dataType.typeName() == "string"]
    for text_col in text_cols:
        text_df = df.filter(col(text_col).isNotNull())
        if text_df.count() == 0:
            continue
        row = text_df.select(
            min(length(col(text_col))).alias("min_len"),
            max(length(col(text_col))).alias("max_len"),
        ).collect()[0]
        for stat, val in [("min", row["min_len"]), ("max", row["max_len"])]:
            records.append({
                "kpi_type": "text_statistics",
                "metric_name": f"{stat}_text_length_{family}_{text_col}",
                "family": family,
                "column_name": text_col,
                "value": float(val) if val is not None else 0.0,
                "unit": "characters",
                "computed_at": now,
                "run_stamp": run_stamp,
            })

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

def compute_storytelling_aggregations(spark: SparkSession, run_stamp: str, silver_dir: str) -> DataFrame:
    """Compute storytelling aggregations reading each Silver family separately."""
    records = []
    now = datetime.utcnow().isoformat()

    # ── rt_reviews ──────────────────────────────
    rt = _read_family(spark, silver_dir, "silver_rt_reviews")
    if rt is not None:
        # Sentiment distribution
        if "sentiment_label" in rt.columns:
            for row in rt.groupBy("sentiment_label").count().collect():
                records.append({
                    "aggregation_type": "sentiment_distribution",
                    "metric_name": f"rt_sentiment_{row['sentiment_label']}",
                    "dimension": "sentiment_label",
                    "dimension_value": str(row["sentiment_label"]),
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