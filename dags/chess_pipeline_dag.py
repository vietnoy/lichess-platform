from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ─── DAG 1: Kafka → MinIO (runs every 15 min) ─────────────
with DAG(
    dag_id="kafka_to_minio",
    default_args=default_args,
    description="Consume Kafka topics and flush raw Parquet to MinIO chess-raw bucket",
    start_date=datetime(2026, 4, 14),
    schedule=timedelta(minutes=15),
    catchup=False,
    tags=["chess", "ingestion", "kafka", "minio"],
) as dag_ingest:

    kafka_to_minio = BashOperator(
        task_id="flush_kafka_to_minio",
        bash_command="python /git/repo/ingestion/kafka_to_minio.py",
    )

# ─── DAG 2: Annotate moves (runs every 15 min) ───────────────────────────────
with DAG(
    dag_id="annotate_moves",
    default_args=default_args,
    description="Read raw moves from MinIO, call Lichess Cloud Eval, write enriched Parquet",
    start_date=datetime(2026, 4, 14),
    schedule=timedelta(minutes=15),
    catchup=False,
    tags=["chess", "processing", "annotation"],
) as dag_annotate:

    annotate = BashOperator(
        task_id="run_annotate",
        bash_command="python /git/repo/processing/annotate.py --date {{ ds }}",
    )

# ─── DAG 3: Load enriched data into StarRocks via Polaris (runs every 30 min) ─
with DAG(
    dag_id="init_catalog_starrocks",
    default_args=default_args,
    description="Create/refresh StarRocks external catalog tables from Polaris Iceberg catalog",
    start_date=datetime(2026, 4, 14),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["chess", "starrocks", "polaris"],
) as dag_load:

    setup_catalog = BashOperator(
        task_id="setup_polaris_catalog",
        bash_command="""
    mysql -h ${STARROCKS_HOST} -P ${STARROCKS_PORT} -u ${STARROCKS_USER} << 'EOF'
    DROP CATALOG IF EXISTS polaris_catalog;
    CREATE EXTERNAL CATALOG IF NOT EXISTS polaris_catalog
    PROPERTIES (
    "type"                        = "iceberg",
    "iceberg.catalog.type"        = "rest",
    "iceberg.catalog.uri"         = "http://polaris:8181/api/catalog",
    "iceberg.catalog.warehouse"   = "chess_warehouse",
    "iceberg.catalog.credential"  = "${STARROCKS_POLARIS_CREDENTIAL}",
    "iceberg.catalog.scope"       = "PRINCIPAL_ROLE:ALL",
    "aws.s3.use_instance_profile" = "false",
    "aws.s3.access_key"           = "${MINIO_ACCESS_KEY}",
    "aws.s3.secret_key"           = "${MINIO_SECRET_KEY}",
    "aws.s3.endpoint"             = "http://minio:9000",
    "aws.s3.enable_path_style_access" = "true"
    );
    SET CATALOG polaris_catalog;
    EOF
    """,
    )

    refresh_catalog = BashOperator(
        task_id="refresh_polaris_catalog",
        bash_command="""
    mysql -h ${STARROCKS_HOST} -P ${STARROCKS_PORT} -u ${STARROCKS_USER} \
    -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.moves;"
    """,
    )

    setup_catalog >> refresh_catalog
