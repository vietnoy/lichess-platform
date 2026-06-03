from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "admin",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    dag_id="pipeline_maintenance",
    default_args=default_args,
    description="Safe cleanup for stale failed pod records and StarRocks catalog drift",
    start_date=datetime(2026, 5, 19),
    schedule="45 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["chess", "ops", "maintenance"],
) as dag:
    cleanup_failed_pods = BashOperator(
        task_id="cleanup_failed_pods",
        bash_command=(
            "python /git/repo/ops/pipeline_maintenance.py "
            "--cleanup-app spark-worker "
            "--cleanup-app starrocks-fe "
            "--cleanup-app starrocks-cn "
            "--repair-polaris-catalog"
        ),
    )
