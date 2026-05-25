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

TEACHABLE_CLASSES = ("blunder", "mistake", "inaccuracy")

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
        .appName("chess-build-critical-positions")
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
        CREATE TABLE IF NOT EXISTS polaris.prod.critical_positions (
            player_id       STRING NOT NULL,
            game_id         STRING NOT NULL,
            ply             INT    NOT NULL,
            date            DATE   NOT NULL,
            fen             STRING,
            played_move     STRING,
            best_move       STRING,
            eval_cp         INT,
            mate            INT,
            eval_swing_cp   INT,
            classification  STRING,
            phase           STRING,
            clock_remaining INT,
            time_pressure   STRING,
            color           STRING,
            opponent_id     STRING,
            opening_eco     STRING,
            opening_name    STRING,
            speed           STRING,
            perf            STRING,
            eval_source     STRING
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def phase_case(ply_expr: str = "ply") -> str:
    return (
        f"CASE WHEN {ply_expr} <= 20 THEN 'opening' "
        f"WHEN {ply_expr} <= 60 THEN 'middlegame' "
        "ELSE 'endgame' END"
    )


def time_pressure_case(clock_expr: str = "clock_remaining") -> str:
    return (
        f"CASE WHEN {clock_expr} IS NULL THEN 'unknown' "
        f"WHEN {clock_expr} < 1000 THEN 'under_10s' "
        f"WHEN {clock_expr} < 3000 THEN 'under_30s' "
        "ELSE 'normal' END"
    )


def date_filter(alias: str, date_str: str | None) -> str:
    if not date_str:
        return ""
    return f"WHERE {alias}.date = DATE '{date_str}'"


def build_critical_positions_sql(
    date_str: str | None,
    include_legacy_daily: bool,
) -> str:
    class_list = ", ".join(f"'{value}'" for value in TEACHABLE_CLASSES)
    player_games_filter = date_filter("pg", date_str)
    move_context_filter = ""
    if date_str:
        move_context_filter = f"WHERE to_date(m.date) = DATE '{date_str}'"

    return f"""
    WITH player_games AS (
        SELECT DISTINCT game_id, player_id, color, opponent_id, date
        FROM polaris.prod.player_games pg
        {player_games_filter}
    ),
    move_context AS (
        SELECT
            m.game_id,
            m.move_number AS ply,
            max(m.whose_moved) AS whose_moved,
            max(m.clock_remaining) AS clock_remaining,
            max(m.opening_eco) AS opening_eco,
            max(m.opening_name) AS opening_name,
            max(m.speed) AS speed,
            max(m.perf) AS perf
        FROM polaris.prod.chess_move_events m
        {move_context_filter}
        GROUP BY m.game_id, m.move_number
    ),
    normalized_evals AS (
        SELECT
            e.player_id,
            e.game_id,
            e.ply,
            e.date,
            e.fen,
            e.played_move,
            e.best_move,
            e.eval_cp,
            e.mate,
            e.eval_swing_cp,
            e.classification,
            'ondemand' AS eval_source,
            1 AS source_priority
        FROM polaris.prod.move_evaluations_ondemand e
        WHERE e.classification IN ({class_list})
        {f"  AND e.date = DATE '{date_str}'" if date_str else ""}
    ),
    ranked_evals AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY game_id, ply, player_id
                ORDER BY source_priority
            ) AS rn
        FROM normalized_evals
    )
    SELECT
        e.player_id,
        e.game_id,
        e.ply,
        e.date,
        e.fen,
        e.played_move,
        e.best_move,
        e.eval_cp,
        e.mate,
        e.eval_swing_cp,
        e.classification,
        {phase_case("e.ply")} AS phase,
        m.clock_remaining,
        {time_pressure_case("m.clock_remaining")} AS time_pressure,
        pg.color,
        pg.opponent_id,
        m.opening_eco,
        m.opening_name,
        m.speed,
        m.perf,
        e.eval_source
    FROM ranked_evals e
    JOIN player_games pg
      ON e.game_id = pg.game_id AND e.player_id = pg.player_id
    LEFT JOIN move_context m
      ON e.game_id = m.game_id AND e.ply = m.ply
    WHERE e.rn = 1
    """


def run(date_str: str | None) -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)
        logger.info("Building critical_positions for date=%s", date_str or "ALL")
        output = spark.sql(build_critical_positions_sql(date_str, include_legacy_daily=False))
        row_count = output.count()
        logger.info("critical_positions output rows=%s", row_count)
        if row_count == 0:
            if date_str:
                spark.sql(f"DELETE FROM polaris.prod.critical_positions WHERE date = DATE '{date_str}'")
                logger.info("cleared empty critical_positions partition for date=%s", date_str)
            return 0

        output.writeTo("polaris.prod.critical_positions").overwritePartitions()
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
