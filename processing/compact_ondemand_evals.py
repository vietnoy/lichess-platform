import logging
import os
import sys

from dotenv import find_dotenv, load_dotenv
import psycopg2
from pyspark.sql import SparkSession

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
DEFAULT_STAGING_BATCH_ROWS = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def staging_batch_rows() -> int:
    raw_value = os.getenv("COMPACT_ONDEMAND_BATCH_ROWS")
    if raw_value is None:
        return DEFAULT_STAGING_BATCH_ROWS
    try:
        value = int(raw_value)
    except ValueError:
        logger.warning(
            "invalid COMPACT_ONDEMAND_BATCH_ROWS=%r; using default %s",
            raw_value,
            DEFAULT_STAGING_BATCH_ROWS,
        )
        return DEFAULT_STAGING_BATCH_ROWS
    if value <= 0:
        logger.warning(
            "non-positive COMPACT_ONDEMAND_BATCH_ROWS=%r; using default %s",
            raw_value,
            DEFAULT_STAGING_BATCH_ROWS,
        )
        return DEFAULT_STAGING_BATCH_ROWS
    return value


def staging_batch_query(batch_rows: int) -> str:
    return f"""
        (
            SELECT
                game_id,
                ply,
                player_id,
                fen,
                played_move,
                best_move,
                eval_cp,
                mate,
                eval_swing_cp,
                classification,
                evaluated_at
            FROM move_evaluations_ondemand
            ORDER BY evaluated_at NULLS LAST, game_id, ply, player_id
            LIMIT {batch_rows}
        ) AS move_evaluations_ondemand_batch
    """


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
        .config("spark.sql.shuffle.partitions", "16")
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
    batch_rows = staging_batch_rows()
    logger.info("reading up to %s rows from analyzer staging", batch_rows)
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", staging_batch_query(batch_rows))
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


def filter_new_evaluations(spark: SparkSession, compacted, dates: list[str]):
    if not dates:
        return compacted
    existing_keys = (
        spark.table("polaris.prod.move_evaluations_ondemand")
        .where(f"date IN ({date_list_sql(dates)})")
        .select("game_id", "ply", "player_id")
        .dropDuplicates(["game_id", "ply", "player_id"])
    )
    return compacted.join(existing_keys, on=["game_id", "ply", "player_id"], how="left_anti")


def changed_dates(compacted) -> list[str]:
    return sorted(str(row.date) for row in compacted.select("date").distinct().collect())


def date_list_sql(dates: list[str]) -> str:
    return ", ".join(f"DATE '{date}'" for date in dates)


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

        compacted = enrich_with_dates(spark, staging)
        try:
            compacted_count = compacted.count()
            logger.info(f"joined with player_games -> {compacted_count} rows after date enrichment")
            if compacted_count < staging_count:
                logger.warning(
                    "compaction dropped %d staging rows (no player_games match); "
                    "they remain in staging for the next run",
                    staging_count - compacted_count,
                )
            elif compacted_count > staging_count:
                logger.warning(
                    "compaction expanded by %d rows before key de-duplication; "
                    "check duplicate player_games/staging keys",
                    compacted_count - staging_count,
                )
            if compacted_count == 0:
                return 0

            dates = changed_dates(compacted)
            logger.info("compacted analyzer partitions: %s", ", ".join(dates))

            new_compacted = filter_new_evaluations(spark, compacted, dates)
            new_count = new_compacted.count()
            logger.info(
                "filtered %d already-compacted rows; %d new rows remain",
                compacted_count - new_count,
                new_count,
            )
            if new_count > 0:
                new_compacted.writeTo("polaris.prod.move_evaluations_ondemand").append()
                logger.info(f"wrote {new_count} rows to iceberg")

            keys = compacted.select("game_id", "ply", "player_id").distinct().toLocalIterator()
            deleted_count = clear_staging(keys)
            logger.info(f"cleared {deleted_count} staging rows")
            return compacted_count
        finally:
            # No explicit persist here. Keeping the joined batch as DISK_ONLY
            # caused Spark workers to fill ephemeral storage during compaction.
            compacted.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 1:
        raise SystemExit("Usage: compact_ondemand_evals.py")
    run()
