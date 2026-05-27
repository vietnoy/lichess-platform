from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "admin",
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}


with DAG(
    dag_id="pipeline_health",
    default_args=default_args,
    description="Read-only freshness checks for Kafka, MinIO, Airflow, Spark, and analyzer staging",
    start_date=datetime(2026, 5, 19),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["chess", "ops", "health"],
) as dag:
    run_health_check = BashOperator(
        task_id="run_pipeline_health",
        bash_command="python /git/repo/ops/pipeline_health.py",
    )

    warm_webapp_cache = BashOperator(
        task_id="warm_webapp_cache",
        bash_command=(
            "curl -fsS --max-time 60 -X POST "
            "http://webapp-backend:8000/api/cache/warmup "
            ">/tmp/webapp_cache_warmup.json || true"
        ),
    )

    run_health_check >> warm_webapp_cache
