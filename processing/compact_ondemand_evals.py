import logging
import os
import sys

from dotenv import find_dotenv, load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# See process_to_polaris.py — same dotenv >=1.1.0 stack-frame assertion.
load_dotenv(find_dotenv(usecwd=True))

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
POLARIS_URI        = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE  = os.getenv("POLARIS_WAREHOUSE")
POSTGRES_USER      = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD  = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST      = "postgres"
POSTGRES_PORT      = 5432
POSTGRES_DB        = "chess_analyzer_db"
JDBC_URL           = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TEACHABLE_CLASSES = ("blunder", "mistake", "inaccuracy")


def positive_rowcount(rowcount) -> int:
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


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


def build_spark() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
        "org.postgresql:postgresql:42.7.4",
    ]
    return (
        SparkSession.builder
        .appName("chess-compact-ondemand-evals")
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
        CREATE TABLE IF NOT EXISTS polaris.prod.move_evaluations_ondemand (
            game_id        STRING NOT NULL,
            ply            INT    NOT NULL,
            player_id      STRING NOT NULL,
            date           DATE   NOT NULL,
            fen            STRING,
            played_move    STRING,
            best_move      STRING,
            eval_cp        INT,
            mate           INT,
            -- Matches services/analyzer/schema.sql staging column name.
            eval_swing_cp  INT,
            classification STRING,
            evaluated_at   TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def ensure_critical_positions_table(spark: SparkSession) -> None:
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


def read_staging(spark: SparkSession):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "move_evaluations_ondemand")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def enrich_with_dates(spark: SparkSession, staging):
    player_games = (
        spark.table("polaris.prod.player_games")
        .select("game_id", "player_id", "date")
        .dropDuplicates(["game_id", "player_id"])
    )
    return (
        staging
        .join(player_games, on=["game_id", "player_id"], how="inner")
        .dropDuplicates(["game_id", "ply", "player_id"])
    )


def changed_dates(compacted) -> list[str]:
    return sorted(str(row.date) for row in compacted.select("date").distinct().collect())


def append_critical_positions(spark: SparkSession, compacted) -> int:
    """Append critical-position facts from this compaction batch only."""
    ensure_critical_positions_table(spark)
    compacted.createOrReplaceTempView("new_move_evaluations_ondemand")
    class_list = ", ".join(f"'{value}'" for value in TEACHABLE_CLASSES)
    critical_rows = spark.sql(
        f"""
        WITH batch_evals AS (
            SELECT
                player_id,
                game_id,
                ply,
                date,
                fen,
                played_move,
                best_move,
                eval_cp,
                mate,
                eval_swing_cp,
                classification,
                'ondemand' AS eval_source
            FROM new_move_evaluations_ondemand e
            WHERE classification IN ({class_list})
        ),
        player_games AS (
            SELECT DISTINCT game_id, player_id, color, opponent_id, date
            FROM polaris.prod.player_games
        ),
        move_context AS (
            SELECT
                m.game_id,
                m.move_number AS ply,
                max(m.clock_remaining) AS clock_remaining,
                max(m.opening_eco) AS opening_eco,
                max(m.opening_name) AS opening_name,
                max(m.speed) AS speed,
                max(m.perf) AS perf
            FROM polaris.prod.chess_move_events m
            JOIN (SELECT DISTINCT game_id, ply FROM batch_evals) b
              ON m.game_id = b.game_id AND m.move_number = b.ply
            GROUP BY m.game_id, m.move_number
        ),
        candidates AS (
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
            FROM batch_evals e
            JOIN player_games pg
              ON e.game_id = pg.game_id AND e.player_id = pg.player_id
            LEFT JOIN move_context m
              ON e.game_id = m.game_id AND e.ply = m.ply
        )
        SELECT c.*
        FROM candidates c
        LEFT ANTI JOIN polaris.prod.critical_positions existing
          ON c.game_id = existing.game_id
         AND c.ply = existing.ply
         AND c.player_id = existing.player_id
        """
    )
    row_count = critical_rows.count()
    if row_count == 0:
        logger.info("no new critical_positions rows in this compaction")
        return 0
    critical_rows.writeTo("polaris.prod.critical_positions").append()
    logger.info("appended %s critical_positions rows", row_count)
    return row_count


def clear_staging(keys, batch_size: int = 5000) -> int:
    # Key-matched DELETE avoids removing worker inserts that arrive after Spark read staging.
    deleted = 0
    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            chunk = []
            for row in keys:
                chunk.append(row)
                if len(chunk) < batch_size:
                    continue

                values_sql = ", ".join(["(%s, %s, %s)"] * len(chunk))
                params = [
                    value
                    for row in chunk
                    for value in (row.game_id, row.ply, row.player_id)
                ]
                sql = f"""
                    DELETE FROM move_evaluations_ondemand AS m
                    USING (VALUES {values_sql}) AS v(game_id, ply, player_id)
                    WHERE m.game_id = v.game_id
                      AND m.ply = v.ply
                      AND m.player_id = v.player_id
                """
                cur.execute(sql, params)
                deleted += positive_rowcount(cur.rowcount)
                chunk = []

            if chunk:
                values_sql = ", ".join(["(%s, %s, %s)"] * len(chunk))
                params = [
                    value
                    for row in chunk
                    for value in (row.game_id, row.ply, row.player_id)
                ]
                sql = f"""
                    DELETE FROM move_evaluations_ondemand AS m
                    USING (VALUES {values_sql}) AS v(game_id, ply, player_id)
                    WHERE m.game_id = v.game_id
                      AND m.ply = v.ply
                      AND m.player_id = v.player_id
                """
                cur.execute(sql, params)
                deleted += positive_rowcount(cur.rowcount)

    return deleted


def run() -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)

        staging = read_staging(spark)
        staging_count = staging.count()
        logger.info(f"read {staging_count} rows from staging")
        if staging_count == 0:
            return 0

        compacted = enrich_with_dates(spark, staging).persist(StorageLevel.DISK_ONLY)
        try:
            compacted_count = compacted.count()
            logger.info(f"joined with player_games -> {compacted_count} rows after date enrichment")
            if staging_count != compacted_count:
                logger.warning(
                    "compaction dropped %d staging rows (no player_games match); "
                    "they remain in staging for the next run",
                    staging_count - compacted_count,
                )
            if compacted_count == 0:
                return 0

            compacted.writeTo("polaris.prod.move_evaluations_ondemand").append()
            logger.info(f"wrote {compacted_count} rows to iceberg")
            append_critical_positions(spark, compacted)

            dates = changed_dates(compacted)
            logger.info("compacted analyzer partitions: %s", ", ".join(dates))

            keys = compacted.select("game_id", "ply", "player_id").distinct().toLocalIterator()
            deleted_count = clear_staging(keys)
            logger.info(f"cleared {deleted_count} staging rows")
            return compacted_count
        finally:
            compacted.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 1:
        raise SystemExit("Usage: compact_ondemand_evals.py")
    run()
