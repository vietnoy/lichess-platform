from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=12),
}

# ─── DAG 1: Kafka → MinIO (runs every 15 min) ─────────────
with DAG(
    dag_id="kafka_to_minio",
    default_args={**default_args, "retries": 0},
    description="Spark Structured Streaming — Kafka to MinIO chess-dev, micro-batch every 2 hours",
    start_date=datetime(2026, 4, 14),
    schedule="0 */2 * * *",
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
            "spark.cores.max": "2",
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "1g",
        },
        verbose=True,
    )

# ─── DAG 2: MinIO → Polaris (daily at 01:00 UTC) ─────────────────────────────
with DAG(
    dag_id="process_to_polaris",
    default_args=default_args,
    description="Explode moves with clocks and metadata, write chess_move_events to Polaris Iceberg",
    start_date=datetime(2026, 4, 14),
    schedule="15 1 * * *",
    catchup=True,
    # Serial execution: each date depends on its own MinIO partition and the
    # Spark cluster only has one slot of headroom on this node. Parallel runs
    # blow out the DAG-processor import budget and OOM the scheduler.
    max_active_runs=1,
    tags=["chess", "processing", "polaris"],
) as dag_process:

    _process_conf = {
        "spark.driver.host": "airflow-scheduler",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "20002",
        "spark.blockManager.port": "20003",
        # AWS SDK v2 needs an explicit region or S3FileIO fails at Iceberg
        # commit. Pod-level AWS_REGION env (e6daba9) does not survive the
        # Spark standalone executor fork — set it in the launch contract.
        "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
        "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
        "spark.executorEnv.AWS_REGION": "us-east-1",
        "spark.cores.max": "4",
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2g",
        "spark.executor.memoryOverhead": "512m",
        "spark.driver.memory": "2g",
        "spark.driver.memoryOverhead": "512m",
        "spark.rpc.lookupTimeout": "300s",
        "spark.network.timeout": "300s",
        "spark.executor.heartbeatInterval": "60s",
        "spark.executorEnv.PYSPARK_PYTHON": "python3.13",
    }
    _iceberg_packages = (
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
    )
    _iceberg_pg_packages = _iceberg_packages + ",org.postgresql:postgresql:42.7.4"

    process = SparkSubmitOperator(
        task_id="run_process_to_polaris",
        application="/git/repo/processing/process_to_polaris.py",
        conn_id="spark_default",
        packages=_iceberg_packages,
        conf=_process_conf,
        application_args=["{{ ds }}"],
        verbose=True,
    )

    build_player_games = SparkSubmitOperator(
        task_id="run_build_player_games",
        application="/git/repo/processing/build_player_games.py",
        conn_id="spark_default",
        packages=_iceberg_packages,
        conf=_process_conf,
        application_args=["{{ ds }}"],
        verbose=True,
    )

    refresh_starrocks_catalog = BashOperator(
        task_id="refresh_starrocks_catalog",
        bash_command=r"""
starrocks_mysql() {
  if [ -n "$STARROCKS_PASSWORD" ]; then
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -p"$STARROCKS_PASSWORD" "$@" 2>/tmp/starrocks_mysql.err || {
      if grep -q "Access denied" /tmp/starrocks_mysql.err; then
        mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
      else
        cat /tmp/starrocks_mysql.err >&2
        return 1
      fi
    }
  else
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
  fi
}

starrocks_mysql -e "
CREATE EXTERNAL CATALOG IF NOT EXISTS polaris_catalog
PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='rest',
  'iceberg.catalog.uri'='http://polaris:8181/api/catalog',
  'iceberg.catalog.warehouse'='chess_warehouse',
  'iceberg.catalog.credential'='$POLARIS_ETL_CLIENT_ID:$POLARIS_ETL_CLIENT_SECRET',
  'iceberg.catalog.scope'='PRINCIPAL_ROLE:ALL',
  'aws.s3.use_instance_profile'='false',
  'aws.s3.access_key'='$MINIO_ACCESS_KEY',
  'aws.s3.secret_key'='$MINIO_SECRET_KEY',
  'aws.s3.endpoint'='http://minio:9000',
  'aws.s3.enable_path_style_access'='true'
);
"
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.chess_move_events;"
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_games;"
""",
    )

    process >> build_player_games >> refresh_starrocks_catalog


# ─── DAG 2b: Analyzer eval compaction → derived product partitions ────────────
with DAG(
    dag_id="analyzer_derived_maintenance",
    default_args=default_args,
    description="Compact asynchronous analyzer evals and rebuild only changed date partitions",
    start_date=datetime(2026, 5, 24),
    schedule="30 0-22/2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["chess", "processing", "analyzer", "polaris"],
) as dag_analyzer_derived:
    compact_ondemand = SparkSubmitOperator(
        task_id="run_compact_ondemand_evals",
        application="/git/repo/processing/compact_ondemand_evals.py",
        conn_id="spark_default",
        packages=_iceberg_pg_packages,
        conf=_process_conf,
        verbose=True,
    )

    refresh_analyzer_tables = BashOperator(
        task_id="refresh_analyzer_tables",
        bash_command=r"""
starrocks_mysql() {
  if [ -n "$STARROCKS_PASSWORD" ]; then
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -p"$STARROCKS_PASSWORD" "$@" 2>/tmp/starrocks_mysql.err || {
      if grep -q "Access denied" /tmp/starrocks_mysql.err; then
        mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
      else
        cat /tmp/starrocks_mysql.err >&2
        return 1
      fi
    }
  else
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
  fi
}

starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.move_evaluations_ondemand;" || true
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.critical_positions;" || true
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_weakness_summary;" || true
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_opening_stats;" || true
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_phase_stats;" || true
""",
    )

    compact_ondemand >> refresh_analyzer_tables


# ─── DAG 2c: Nightly analyzer aggregate summaries ────────────────────────────
with DAG(
    dag_id="analyzer_summary_maintenance",
    default_args=default_args,
    description="Nightly full rebuild of analyzer aggregate summary tables",
    start_date=datetime(2026, 5, 24),
    schedule="45 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["chess", "processing", "analyzer", "summaries"],
) as dag_analyzer_summaries:
    rebuild_player_weakness_summary = SparkSubmitOperator(
        task_id="rebuild_player_weakness_summary",
        application="/git/repo/processing/build_player_weakness_summary.py",
        conn_id="spark_default",
        packages=_iceberg_packages,
        conf=_process_conf,
        application_args=["--all"],
        verbose=True,
    )

    rebuild_player_opening_stats = SparkSubmitOperator(
        task_id="rebuild_player_opening_stats",
        application="/git/repo/processing/build_player_opening_stats.py",
        conn_id="spark_default",
        packages=_iceberg_packages,
        conf=_process_conf,
        application_args=["--all"],
        verbose=True,
    )

    rebuild_player_phase_stats = SparkSubmitOperator(
        task_id="rebuild_player_phase_stats",
        application="/git/repo/processing/build_player_phase_stats.py",
        conn_id="spark_default",
        packages=_iceberg_packages,
        conf=_process_conf,
        application_args=["--all"],
        verbose=True,
    )

    refresh_summary_tables = BashOperator(
        task_id="refresh_summary_tables",
        execution_timeout=timedelta(hours=12),
        bash_command=r"""
starrocks_mysql() {
  if [ -n "$STARROCKS_PASSWORD" ]; then
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -p"$STARROCKS_PASSWORD" "$@" 2>/tmp/starrocks_mysql.err || {
      if grep -q "Access denied" /tmp/starrocks_mysql.err; then
        mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
      else
        cat /tmp/starrocks_mysql.err >&2
        return 1
      fi
    }
  else
    mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" "$@"
  fi
}

starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_weakness_summary;"
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_opening_stats;"
starrocks_mysql -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_phase_stats;"
""",
    )

    (
        rebuild_player_weakness_summary
        >> rebuild_player_opening_stats
        >> rebuild_player_phase_stats
        >> refresh_summary_tables
    )

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
        bash_command="""mysql -h $STARROCKS_HOST -P $STARROCKS_PORT -u $STARROCKS_USER -e "
DROP CATALOG IF EXISTS polaris_catalog;
CREATE EXTERNAL CATALOG IF NOT EXISTS polaris_catalog
PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='rest',
  'iceberg.catalog.uri'='http://polaris:8181/api/catalog',
  'iceberg.catalog.warehouse'='chess_warehouse',
  'iceberg.catalog.credential'='$POLARIS_ETL_CLIENT_ID:$POLARIS_ETL_CLIENT_SECRET',
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
        bash_command=r"""
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.chess_move_events;"
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_games;"
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.move_evaluations;" || true
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.move_evaluations_ondemand;" || true
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.critical_positions;" || true
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_weakness_summary;" || true
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_opening_stats;" || true
mysql -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" -u "$STARROCKS_USER" -e "REFRESH EXTERNAL TABLE polaris_catalog.prod.player_phase_stats;" || true
""",
    )

    setup_catalog >> refresh_catalog
