import logging
import os
import sys

from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession

# See process_to_polaris.py — same dotenv >=1.1.0 stack-frame assertion.
load_dotenv(find_dotenv(usecwd=True))

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
POLARIS_URI        = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE  = os.getenv("POLARIS_WAREHOUSE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
    ]
    return (
        SparkSession.builder
        .appName("chess-build-player-phase-stats")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", POLARIS_URI)
        .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_WAREHOUSE)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.polaris.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS polaris.prod.player_phase_stats (
            player_id               STRING NOT NULL,
            date                    DATE   NOT NULL,
            phase                   STRING NOT NULL,
            games_with_positions    INT,
            critical_positions      INT,
            blunders                INT,
            mistakes                INT,
            inaccuracies            INT,
            time_pressure_positions INT,
            avg_eval_swing_cp       DOUBLE,
            max_eval_swing_cp       INT,
            updated_at              TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def date_filter(date_str: str | None) -> str:
    if not date_str:
        return ""
    return f"WHERE date = DATE '{date_str}'"


def build_player_phase_stats_sql(date_str: str | None) -> str:
    return f"""
    WITH source AS (
        SELECT
            player_id,
            game_id,
            date,
            COALESCE(phase, 'unknown') AS phase,
            classification,
            time_pressure,
            eval_swing_cp
        FROM polaris.prod.critical_positions
        {date_filter(date_str)}
    )
    SELECT
        player_id,
        date,
        phase,
        CAST(COUNT(DISTINCT game_id) AS INT) AS games_with_positions,
        CAST(COUNT(*) AS INT) AS critical_positions,
        CAST(SUM(CASE WHEN classification = 'blunder' THEN 1 ELSE 0 END) AS INT) AS blunders,
        CAST(SUM(CASE WHEN classification = 'mistake' THEN 1 ELSE 0 END) AS INT) AS mistakes,
        CAST(SUM(CASE WHEN classification = 'inaccuracy' THEN 1 ELSE 0 END) AS INT) AS inaccuracies,
        CAST(SUM(CASE WHEN time_pressure IN ('under_10s', 'under_30s') THEN 1 ELSE 0 END) AS INT)
            AS time_pressure_positions,
        ROUND(AVG(ABS(eval_swing_cp)), 1) AS avg_eval_swing_cp,
        CAST(MAX(ABS(eval_swing_cp)) AS INT) AS max_eval_swing_cp,
        current_timestamp() AS updated_at
    FROM source
    GROUP BY player_id, date, phase
    """


def run(date_str: str | None) -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)
        logger.info("Building player_phase_stats for date=%s", date_str or "ALL")
        output = spark.sql(build_player_phase_stats_sql(date_str))
        row_count = output.count()
        logger.info("player_phase_stats output rows=%s", row_count)
        if row_count == 0:
            if date_str:
                spark.sql(f"DELETE FROM polaris.prod.player_phase_stats WHERE date = DATE '{date_str}'")
                logger.info("cleared empty player_phase_stats partition for date=%s", date_str)
            return 0

        output.writeTo("polaris.prod.player_phase_stats").overwritePartitions()
        logger.info("Done")
        return row_count
    finally:
        spark.stop()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    date = yesterday if (arg is None or arg >= today) else arg
    run(date)
