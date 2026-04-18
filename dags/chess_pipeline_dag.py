from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ─── DAG 1: Kafka → MinIO (runs every 15 min) ─────────────
with DAG(
    dag_id="kafka_to_minio",
    default_args={**default_args, "retries": 0},
    description="Spark Structured Streaming — Kafka to MinIO chess-dev, micro-batch every 10 min",
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=["chess", "ingestion", "kafka", "minio", "spark"],
) as dag_ingest:

    kafka_to_minio = SparkSubmitOperator(
        task_id="spark_kafka_to_minio",
        application="/git/repo/ingestion/kafka_to_minio.py",
        conn_id="spark_default",
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "20002",
            "spark.blockManager.port": "20003",
        },
        verbose=True,
    )

# ─── DAG 2: MinIO → Stockfish annotation → Polaris (daily at 01:00 UTC) ───────
with DAG(
    dag_id="process_to_polaris",
    default_args=default_args,
    description="Parse games, annotate with Stockfish, write player_moves to Polaris Iceberg",
    start_date=datetime(2026, 4, 14),
    schedule="0 1 * * *",
    catchup=True,
    tags=["chess", "processing", "polaris", "stockfish"],
) as dag_process:

    process = BashOperator(
        task_id="run_process_to_polaris",
        bash_command="python /git/repo/processing/process_to_polaris.py",
    ),
    conf={
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "20002",
            "spark.blockManager.port": "20003",
        }

# ─── DAG 3: Load enriched data into StarRocks via Polaris ─────────────────────
with DAG(
    dag_id="init_catalog_starrocks",
    default_args=default_args,
    description="Create/refresh StarRocks external catalog tables from Polaris Iceberg catalog",
    start_date=datetime(2026, 4, 14),
    schedule=None,
    catchup=False,
    tags=["chess", "starrocks", "polaris"],
) as dag_load:

    setup_catalog = BashOperator(
        task_id="setup_polaris_catalog",
        bash_command="""mysql -h $STARROCKS_HOST -P $STARROCKS_PORT -u $STARROCKS_USER -p$STARROCKS_PASSWORD -e "
DROP CATALOG IF EXISTS polaris_catalog;
CREATE EXTERNAL CATALOG IF NOT EXISTS polaris_catalog
PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='rest',
  'iceberg.catalog.uri'='http://polaris:8181/api/catalog',
  'iceberg.catalog.warehouse'='chess_warehouse',
  'iceberg.catalog.credential'='$STARROCKS_POLARIS_CREDENTIAL',
  'iceberg.catalog.scope'='PRINCIPAL_ROLE:ALL',
  'aws.s3.use_instance_profile'='false',
  'aws.s3.access_key'='$MINIO_ACCESS_KEY',
  'aws.s3.secret_key'='$MINIO_SECRET_KEY',
  'aws.s3.endpoint'='http://minio:9000',
  'aws.s3.enable_path_style_access'='true'
);
"
""",
    )

    refresh_catalog = BashOperator(
        task_id="refresh_polaris_catalog",
        bash_command="mysql -h $STARROCKS_HOST -P $STARROCKS_PORT -u $STARROCKS_USER -p$STARROCKS_PASSWORD -e \"REFRESH EXTERNAL TABLE polaris_catalog.prod.chess_raw_events;\"",
    )

    setup_catalog >> refresh_catalog
