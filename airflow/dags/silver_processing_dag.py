import logging
import os
from glob import glob
from datetime import datetime
from typing import Dict, List, Optional, Set

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
PROCESSED_FILE = os.path.join(SILVER_DIR, "processed_bronze_files.parquet")


@dag(
    dag_id="silver_processing",
    schedule_interval=None,
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
    def get_processed_files() -> Set[str]:
        import pandas as pd
        txt_file = PROCESSED_FILE.replace('.parquet', '.txt')
        if os.path.exists(PROCESSED_FILE):
            df = pd.read_parquet(PROCESSED_FILE)
            return set(df['file_path'].tolist())
        elif os.path.exists(txt_file):
            # Migrate from txt to parquet
            with open(txt_file, 'r', encoding='utf-8') as f:
                file_paths = [line.strip() for line in f if line.strip()]
            new_rows = []
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                parts = filename.rsplit('_', 2)
                if len(parts) >= 3:
                    date_str = parts[-2]
                    time_str = parts[-1].split('.')[0]
                    if len(date_str) == 8 and len(time_str) == 6:
                        creation_day = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        creation_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                        new_rows.append({'file_path': file_path, 'file_name': filename, 'creation_day': creation_day, 'creation_time': creation_time})
            df = pd.DataFrame(new_rows)
            df.to_parquet(PROCESSED_FILE, index=False)
            # Optionally remove txt file
            os.remove(txt_file)
            return set(df['file_path'].tolist())
        return set()

    @task()
    def classify_files(files: List[str], processed_files: Set[str]) -> Dict[str, List[str]]:
        grouped = group_files_by_silver_family(files)
        filtered_grouped = {k: [f for f in v if f not in processed_files] for k, v in grouped.items()}
        logger.info(
            "Classified and filtered Bronze files -> titles=%s rt_reviews=%s tmdb_reviews=%s ignored=%s",
            len(filtered_grouped["silver_titles"]),
            len(filtered_grouped["silver_rt_reviews"]),
            len(filtered_grouped["silver_tmdb_reviews"]),
            len(filtered_grouped["ignored"]),
        )
        if filtered_grouped["ignored"]:
            logger.warning("Ignored Bronze files: %s", filtered_grouped["ignored"])
        return filtered_grouped

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

    @task()
    def save_processed_files(grouped: Dict[str, List[str]], processed_files: Set[str]) -> None:
        import pandas as pd
        existing_df = pd.read_parquet(PROCESSED_FILE) if os.path.exists(PROCESSED_FILE) else pd.DataFrame(columns=['file_path', 'file_name', 'creation_day', 'creation_time'])
        new_files = set(f for files in grouped.values() for f in files)
        new_rows = []
        for file_path in new_files:
            filename = os.path.basename(file_path)
            parts = filename.rsplit('_', 2)
            if len(parts) >= 3:
                date_str = parts[-2]
                time_str = parts[-1].split('.')[0]
                if len(date_str) == 8 and len(time_str) == 6:
                    creation_day = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    creation_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    new_rows.append({'file_path': file_path, 'file_name': filename, 'creation_day': creation_day, 'creation_time': creation_time})
        new_df = pd.DataFrame(new_rows)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(subset='file_path')
        combined_df.to_parquet(PROCESSED_FILE, index=False)
        logger.info("Updated processed files Parquet with %s new files", len(new_df))

    bronze_ready = wait_for_bronze_files()
    files = list_files()
    processed_files = get_processed_files()
    grouped = classify_files(files, processed_files)
    run_stamp = get_run_stamp()

    titles_path = process_titles(grouped, run_stamp)
    rt_reviews_path = process_rt_reviews(grouped, run_stamp)
    tmdb_reviews_path = process_tmdb_reviews(grouped, run_stamp)

    end = summarize_outputs(titles_path, rt_reviews_path, tmdb_reviews_path)
    save_processed = save_processed_files(grouped, processed_files)

    start >> bronze_ready >> files >> processed_files >> grouped
    grouped >> run_stamp
    run_stamp >> [titles_path, rt_reviews_path, tmdb_reviews_path] >> end >> save_processed


silver_processing_dag()
