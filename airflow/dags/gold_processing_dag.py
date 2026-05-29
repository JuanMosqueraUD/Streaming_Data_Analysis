import logging
import os
from datetime import datetime
from typing import Dict, Optional

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from utils.gold_processing import (
    create_spark_session,
    compute_governance_kpis,
    compute_storytelling_aggregations,
    write_gold_parquet,
)

logger = logging.getLogger(__name__)

SILVER_DIR = "/opt/airflow/datalake_silver"
GOLD_DIR = "/opt/airflow/datalake_gold"


@dag(
    dag_id="gold_processing",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["gold", "medallion", "analytics", "aggregation"],
)
def gold_processing_dag():
    """
    Silver-to-Gold DAG: reads each Silver family separately with PySpark
    and produces two Gold layer datasets:
    1. Governance KPIs  — data quality metrics per family
    2. Storytelling     — analytical aggregations for dashboards
    """

    start = EmptyOperator(task_id="start")

    @task()
    def get_run_stamp() -> str:
        context = get_current_context()
        logical_date = context["logical_date"]
        return logical_date.strftime("%Y%m%d_%H%M%S")

    @task()
    def check_silver_files_exist() -> bool:
        import glob
        parquet_files = glob.glob(os.path.join(SILVER_DIR, "silver_*.parquet"))
        exists = len(parquet_files) > 0
        if exists:
            logger.info("Found %s Silver Parquet files", len(parquet_files))
        else:
            logger.warning("No Silver Parquet files found in %s", SILVER_DIR)
        return exists

    @task()
    def initialize_spark() -> str:
        """Validate that Spark + Java are available."""
        spark = create_spark_session(app_name="GoldInit")
        version = spark.version
        logger.info("SparkSession OK: version %s", version)
        spark.stop()
        return version

    @task()
    def compute_governance(run_stamp: str) -> Optional[str]:
        spark = create_spark_session(app_name="GoldGovernance")
        try:
            governance_df = compute_governance_kpis(spark, run_stamp, silver_dir=SILVER_DIR)
            return write_gold_parquet(
                df=governance_df,
                output_dir=GOLD_DIR,
                prefix="governance",
                run_stamp=run_stamp,
            )
        except Exception as e:
            logger.error("Failed to compute governance KPIs: %s", e)
            raise
        finally:
            spark.stop()

    @task()
    def compute_storytelling(run_stamp: str) -> Optional[str]:
        spark = create_spark_session(app_name="GoldStorytelling")
        try:
            storytelling_df = compute_storytelling_aggregations(spark, run_stamp, silver_dir=SILVER_DIR)
            return write_gold_parquet(
                df=storytelling_df,
                output_dir=GOLD_DIR,
                prefix="storytelling",
                run_stamp=run_stamp,
            )
        except Exception as e:
            logger.error("Failed to compute storytelling aggregations: %s", e)
            raise
        finally:
            spark.stop()

    @task()
    def summarize_outputs(
        governance_path: Optional[str],
        storytelling_path: Optional[str],
    ) -> Dict[str, Optional[str]]:
        summary = {
            "governance_parquet": governance_path,
            "storytelling_parquet": storytelling_path,
            "output_directory": GOLD_DIR,
        }
        logger.info("Gold layer processing complete: %s", summary)
        return summary

    # Task flow — read_silver_data removed: merging incompatible schemas
    # across Silver families is not needed; each compute task reads its own family.
    run_stamp = get_run_stamp()
    check_silver = check_silver_files_exist()
    spark_init = initialize_spark()

    governance_output = compute_governance(run_stamp)
    storytelling_output = compute_storytelling(run_stamp)
    summary = summarize_outputs(governance_output, storytelling_output)

    start >> [check_silver, spark_init, run_stamp]
    for upstream in [check_silver, spark_init, run_stamp]:
        upstream >> governance_output
        upstream >> storytelling_output
    [governance_output, storytelling_output] >> summary


gold_processing_dag()