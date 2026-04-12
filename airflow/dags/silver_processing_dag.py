import logging
import os
from glob import glob
from datetime import datetime
from typing import Dict, List, Optional

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from utils.bronze_file_utils import group_files_by_silver_family, list_bronze_json_files
from utils.silver_processing import (
    transform_rt_reviews,
    transform_titles,
    transform_tmdb_reviews,
    utc_now_iso,
    write_silver_parquet,
)

logger = logging.getLogger(__name__)

BRONZE_DIR = "/opt/airflow/datalake_bronze"
SILVER_DIR = "/opt/airflow/datalake_silver"


@dag(
    dag_id="silver_processing",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "medallion", "normalization"],
)
def silver_processing_dag():
    start = EmptyOperator(task_id="start")

    @task.sensor(poke_interval=60, timeout=60 * 20, mode="poke")
    def wait_for_bronze_files() -> PokeReturnValue:
        """Wait until at least one Bronze JSON is available."""
        files = glob(os.path.join(BRONZE_DIR, "*.json"))
        if files:
            logger.info("Bronze files detected: %s", len(files))
            return PokeReturnValue(is_done=True, xcom_value=True)

        logger.info("No Bronze JSON files found yet under %s", BRONZE_DIR)
        return PokeReturnValue(is_done=False, xcom_value=False)

    @task()
    def list_files() -> List[str]:
        files = list_bronze_json_files(BRONZE_DIR)
        logger.info("Found %s Bronze files", len(files))
        return files

    @task()
    def classify_files(files: List[str]) -> Dict[str, List[str]]:
        grouped = group_files_by_silver_family(files)
        logger.info(
            "Classified Bronze files -> titles=%s rt_reviews=%s tmdb_reviews=%s ignored=%s",
            len(grouped["silver_titles"]),
            len(grouped["silver_rt_reviews"]),
            len(grouped["silver_tmdb_reviews"]),
            len(grouped["ignored"]),
        )
        if grouped["ignored"]:
            logger.warning("Ignored Bronze files: %s", grouped["ignored"])
        return grouped

    @task()
    def get_run_stamp() -> str:
        """
        Deterministic naming based on logical date for retry-safe idempotency.
        Same DAG run writes the same file names and overwrites atomically.
        """
        context = get_current_context()
        logical_date = context["logical_date"]
        return logical_date.strftime("%Y%m%d_%H%M%S")

    @task()
    def process_titles(grouped: Dict[str, List[str]], run_stamp: str) -> Optional[str]:
        files = grouped.get("silver_titles", [])
        if not files:
            logger.info("No title-level Bronze files to process.")
            return None

        processed_at = utc_now_iso()
        try:
            df = transform_titles(files=files, processed_at=processed_at)
            return write_silver_parquet(
                df=df,
                output_dir=SILVER_DIR,
                prefix="silver_titles",
                run_stamp=run_stamp,
            )
        except Exception as exc:
            logger.exception("Failed to process silver_titles: %s", exc)
            raise

    @task()
    def process_rt_reviews(grouped: Dict[str, List[str]], run_stamp: str) -> Optional[str]:
        files = grouped.get("silver_rt_reviews", [])
        if not files:
            logger.info("No RT review Bronze files to process.")
            return None

        processed_at = utc_now_iso()
        try:
            df = transform_rt_reviews(files=files, processed_at=processed_at)
            return write_silver_parquet(
                df=df,
                output_dir=SILVER_DIR,
                prefix="silver_rt_reviews",
                run_stamp=run_stamp,
            )
        except Exception as exc:
            logger.exception("Failed to process silver_rt_reviews: %s", exc)
            raise

    @task()
    def process_tmdb_reviews(grouped: Dict[str, List[str]], run_stamp: str) -> Optional[str]:
        files = grouped.get("silver_tmdb_reviews", [])
        if not files:
            logger.info("No TMDb review Bronze files to process.")
            return None

        processed_at = utc_now_iso()
        try:
            df = transform_tmdb_reviews(files=files, processed_at=processed_at)
            return write_silver_parquet(
                df=df,
                output_dir=SILVER_DIR,
                prefix="silver_tmdb_reviews",
                run_stamp=run_stamp,
            )
        except Exception as exc:
            logger.exception("Failed to process silver_tmdb_reviews: %s", exc)
            raise

    @task()
    def summarize_outputs(
        titles_path: Optional[str],
        rt_reviews_path: Optional[str],
        tmdb_reviews_path: Optional[str],
    ) -> Dict[str, Optional[str]]:
        summary = {
            "silver_titles": titles_path,
            "silver_rt_reviews": rt_reviews_path,
            "silver_tmdb_reviews": tmdb_reviews_path,
        }
        logger.info("Silver outputs: %s", summary)
        return summary

    bronze_ready = wait_for_bronze_files()
    files = list_files()
    grouped = classify_files(files)
    run_stamp = get_run_stamp()

    titles_path = process_titles(grouped, run_stamp)
    rt_reviews_path = process_rt_reviews(grouped, run_stamp)
    tmdb_reviews_path = process_tmdb_reviews(grouped, run_stamp)

    end = summarize_outputs(titles_path, rt_reviews_path, tmdb_reviews_path)

    start >> bronze_ready >> files >> grouped
    grouped >> run_stamp
    run_stamp >> [titles_path, rt_reviews_path, tmdb_reviews_path] >> end


silver_processing_dag()
