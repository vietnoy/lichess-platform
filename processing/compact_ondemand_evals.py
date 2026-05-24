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


def positive_rowcount(rowcount) -> int:
    return rowcount if isinstance(rowcount, int) and rowcount > 0 else 0


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


def ensure_change_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analyzer_partition_changes (
            date DATE PRIMARY KEY,
            first_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            new_eval_rows BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            processed_at TIMESTAMPTZ,
            error TEXT
        )
        """
    )


def record_changed_dates(dates: list[str], row_count: int) -> None:
    if not dates:
        return

    per_date_rows = max(row_count // len(dates), 0)
    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            ensure_change_table(cur)
            for date_str in dates:
                cur.execute(
                    """
                    INSERT INTO analyzer_partition_changes (
                        date,
                        first_changed_at,
                        last_changed_at,
                        new_eval_rows,
                        status,
                        processed_at,
                        error
                    )
                    VALUES (%s, now(), now(), %s, 'pending', NULL, NULL)
                    ON CONFLICT (date) DO UPDATE SET
                        last_changed_at = now(),
                        new_eval_rows = analyzer_partition_changes.new_eval_rows + EXCLUDED.new_eval_rows,
                        status = 'pending',
                        processed_at = NULL,
                        error = NULL
                    """,
                    (date_str, per_date_rows),
                )


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

            dates = changed_dates(compacted)
            record_changed_dates(dates, compacted_count)
            logger.info("recorded changed analyzer partitions: %s", ", ".join(dates))

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
