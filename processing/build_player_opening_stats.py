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
        .appName("chess-build-player-opening-stats")
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
        CREATE TABLE IF NOT EXISTS polaris.prod.player_opening_stats (
            player_id          STRING NOT NULL,
            date               DATE   NOT NULL,
            opening_eco        STRING,
            opening_name       STRING,
            color              STRING,
            games              INT,
            wins               INT,
            losses             INT,
            draws              INT,
            win_rate_pct       DOUBLE,
            critical_positions INT,
            blunders           INT,
            mistakes           INT,
            inaccuracies       INT,
            avg_eval_swing_cp  DOUBLE,
            updated_at         TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def date_filter(date_str: str | None, alias: str | None = None) -> str:
    if not date_str:
        return ""
    prefix = f"{alias}." if alias else ""
    return f"WHERE {prefix}date = DATE '{date_str}'"


def build_player_opening_stats_sql(date_str: str | None) -> str:
    games_filter = date_filter(date_str)
    critical_filter = date_filter(date_str)
    return f"""
    WITH games AS (
        SELECT
            player_id,
            date,
            COALESCE(opening_eco, '') AS opening_eco,
            COALESCE(opening_name, 'Unknown') AS opening_name,
            color,
            COUNT(DISTINCT game_id) AS games,
            SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN winner IS NOT NULL AND winner <> color THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws
        FROM polaris.prod.player_games
        {games_filter}
        GROUP BY player_id, date, opening_eco, opening_name, color
    ),
    critical AS (
        SELECT
            player_id,
            date,
            COALESCE(opening_eco, '') AS opening_eco,
            COALESCE(opening_name, 'Unknown') AS opening_name,
            color,
            COUNT(*) AS critical_positions,
            SUM(CASE WHEN classification = 'blunder' THEN 1 ELSE 0 END) AS blunders,
            SUM(CASE WHEN classification = 'mistake' THEN 1 ELSE 0 END) AS mistakes,
            SUM(CASE WHEN classification = 'inaccuracy' THEN 1 ELSE 0 END) AS inaccuracies,
            ROUND(AVG(ABS(eval_swing_cp)), 1) AS avg_eval_swing_cp
        FROM polaris.prod.critical_positions
        {critical_filter}
        GROUP BY player_id, date, opening_eco, opening_name, color
    )
    SELECT
        g.player_id,
        g.date,
        NULLIF(g.opening_eco, '') AS opening_eco,
        g.opening_name,
        g.color,
        CAST(g.games AS INT) AS games,
        CAST(g.wins AS INT) AS wins,
        CAST(g.losses AS INT) AS losses,
        CAST(g.draws AS INT) AS draws,
        ROUND(g.wins * 100.0 / g.games, 1) AS win_rate_pct,
        CAST(COALESCE(c.critical_positions, 0) AS INT) AS critical_positions,
        CAST(COALESCE(c.blunders, 0) AS INT) AS blunders,
        CAST(COALESCE(c.mistakes, 0) AS INT) AS mistakes,
        CAST(COALESCE(c.inaccuracies, 0) AS INT) AS inaccuracies,
        c.avg_eval_swing_cp,
        current_timestamp() AS updated_at
    FROM games g
    LEFT JOIN critical c
      ON g.player_id = c.player_id
     AND g.date = c.date
     AND g.opening_eco = c.opening_eco
     AND g.opening_name = c.opening_name
     AND g.color = c.color
    """


def run(date_str: str | None) -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)
        logger.info("Building player_opening_stats for date=%s", date_str or "ALL")
        output = spark.sql(build_player_opening_stats_sql(date_str))
        row_count = output.count()
        logger.info("player_opening_stats output rows=%s", row_count)
        if row_count == 0:
            if date_str:
                spark.sql(f"DELETE FROM polaris.prod.player_opening_stats WHERE date = DATE '{date_str}'")
                logger.info("cleared empty player_opening_stats partition for date=%s", date_str)
            return 0

        output.writeTo("polaris.prod.player_opening_stats").overwritePartitions()
        logger.info("Done")
        return row_count
    finally:
        spark.stop()


def resolve_date_arg(argv: list[str]) -> str | None:
    if len(argv) > 1 and argv[1] == "--all":
        return None

    arg = argv[1] if len(argv) > 1 else None
    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    return yesterday if (arg is None or arg >= today) else arg


if __name__ == "__main__":
    run(resolve_date_arg(sys.argv))
