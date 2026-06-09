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
DEFAULT_STAGING_DATE_ORDER = "DESC"
CRITICAL_REBUILD_QUEUE_TABLE = "critical_position_rebuild_queue"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def staging_date_order() -> str:
    raw_value = os.getenv("COMPACT_ONDEMAND_DATE_ORDER", DEFAULT_STAGING_DATE_ORDER)
    value = raw_value.strip().upper()
    if value in {"ASC", "DESC"}:
        return value
    logger.warning(
        "invalid COMPACT_ONDEMAND_DATE_ORDER=%r; using default %s",
        raw_value,
        DEFAULT_STAGING_DATE_ORDER,
    )
    return DEFAULT_STAGING_DATE_ORDER


def target_date_literal(date: str) -> str:
    # Dates come from PostgreSQL DATE values. Keep a small guard because the
    # value is interpolated into Spark's JDBC subquery.
    if len(date) != 10 or date[4] != "-" or date[7] != "-":
        raise ValueError(f"invalid date literal: {date!r}")
    year, month, day = date.split("-")
    if not (year.isdigit() and month.isdigit() and day.isdigit()):
        raise ValueError(f"invalid date literal: {date!r}")
    return date


def staging_batch_query(target_date: str) -> str:
    target_date = target_date_literal(target_date)
    return f"""
        (
            SELECT
                game_id,
                ply,
                player_id,
                date,
                fen,
                played_move,
                best_move,
                eval_cp,
                mate,
                eval_swing_cp,
                classification,
                evaluated_at
            FROM move_evaluations_ondemand
            WHERE date = DATE '{target_date}'
            ORDER BY evaluated_at NULLS LAST, game_id, ply, player_id
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


def ensure_critical_rebuild_queue() -> None:
    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CRITICAL_REBUILD_QUEUE_TABLE} (
                    date DATE PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INT NOT NULL DEFAULT 0,
                    row_count INT,
                    last_error TEXT,
                    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    locked_at TIMESTAMPTZ,
                    locked_by TEXT,
                    rerun_after_running BOOLEAN NOT NULL DEFAULT FALSE
                )
                """
            )
            cur.execute(f"ALTER TABLE {CRITICAL_REBUILD_QUEUE_TABLE} ADD COLUMN IF NOT EXISTS row_count INT")
            cur.execute(
                f"""
                ALTER TABLE {CRITICAL_REBUILD_QUEUE_TABLE}
                ADD COLUMN IF NOT EXISTS rerun_after_running BOOLEAN NOT NULL DEFAULT FALSE
                """
            )


def choose_target_date() -> str | None:
    order = staging_date_order()
    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date
                FROM move_evaluations_ondemand
                WHERE date IS NOT NULL
                ORDER BY date {order}
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if not row:
        return None
    target_date = str(row[0])
    logger.info("selected analyzer staging date partition: %s (%s)", target_date, order)
    return target_date


def enqueue_critical_rebuild_dates(dates: list[str]) -> int:
    if not dates:
        return 0

    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CRITICAL_REBUILD_QUEUE_TABLE} (
                    date DATE PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INT NOT NULL DEFAULT 0,
                    row_count INT,
                    last_error TEXT,
                    enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    locked_at TIMESTAMPTZ,
                    locked_by TEXT
                )
                """
            )
            cur.execute(f"ALTER TABLE {CRITICAL_REBUILD_QUEUE_TABLE} ADD COLUMN IF NOT EXISTS row_count INT")
            values_sql = ", ".join(["(%s::date)"] * len(dates))
            cur.execute(
                f"""
                INSERT INTO {CRITICAL_REBUILD_QUEUE_TABLE} (date)
                VALUES {values_sql}
                ON CONFLICT (date) DO UPDATE
                SET
                    status = CASE
                        WHEN {CRITICAL_REBUILD_QUEUE_TABLE}.status = 'running' THEN {CRITICAL_REBUILD_QUEUE_TABLE}.status
                        ELSE 'pending'
                    END,
                    attempts = CASE
                        WHEN {CRITICAL_REBUILD_QUEUE_TABLE}.status = 'running' THEN {CRITICAL_REBUILD_QUEUE_TABLE}.attempts
                        ELSE 0
                    END,
                    last_error = NULL,
                    row_count = CASE
                        WHEN {CRITICAL_REBUILD_QUEUE_TABLE}.status = 'running' THEN {CRITICAL_REBUILD_QUEUE_TABLE}.row_count
                        ELSE NULL
                    END,
                    enqueued_at = now(),
                    updated_at = now(),
                    rerun_after_running = CASE
                        WHEN {CRITICAL_REBUILD_QUEUE_TABLE}.status = 'running' THEN TRUE
                        ELSE FALSE
                    END
                """,
                dates,
            )
            return positive_rowcount(cur.rowcount)


def read_staging(spark: SparkSession, target_date: str):
    logger.info("reading analyzer staging date partition %s", target_date)
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", staging_batch_query(target_date))
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def enrich_with_dates(spark: SparkSession, staging):
    return (
        staging
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

        target_date = choose_target_date()
        if not target_date:
            logger.info("no dated analyzer staging rows to compact")
            return 0

        staging = read_staging(spark, target_date).where(f"date = DATE '{target_date}'")
        staging_count = staging.count()
        logger.info("read %s rows from staging date %s", staging_count, target_date)
        if staging_count == 0:
            return 0

        compacted = enrich_with_dates(spark, staging)
        try:
            compacted_count = compacted.count()
            logger.info(f"deduplicated dated staging rows -> {compacted_count} rows")
            if compacted_count == 0:
                return 0

            compacted = (
                compacted
                .where(f"date = DATE '{target_date}'")
                .orderBy("evaluated_at", "game_id", "ply", "player_id")
            )
            compacted_count = compacted.count()
            logger.info(
                "processing complete analyzer date partition %s with %s rows",
                target_date,
                compacted_count,
            )
            if compacted_count == 0:
                return 0

            new_compacted = filter_new_evaluations(spark, compacted, [target_date])
            new_count = new_compacted.count()
            logger.info(
                "filtered %d already-compacted rows; %d new rows remain",
                compacted_count - new_count,
                new_count,
            )
            if new_count > 0:
                new_compacted.writeTo("polaris.prod.move_evaluations_ondemand").append()
                logger.info(f"wrote {new_count} rows to iceberg")
                enqueued_count = enqueue_critical_rebuild_dates([target_date])
                logger.info("enqueued %s critical-position rebuild dates", enqueued_count)

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
