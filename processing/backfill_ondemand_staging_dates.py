from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Iterable

from dotenv import find_dotenv, load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# See process_to_polaris.py — same dotenv >=1.1.0 stack-frame assertion.
load_dotenv(find_dotenv(usecwd=True))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
POLARIS_URI = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE = os.getenv("POLARIS_WAREHOUSE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "chess_analyzer_db"
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

DEFAULT_BATCH_ROWS = 100_000
DEFAULT_UPDATE_CHUNK_ROWS = 5_000

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
        "org.postgresql:postgresql:42.7.4",
    ]
    return (
        SparkSession.builder
        .appName("chess-backfill-ondemand-staging-dates")
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


def connect_pg():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_pg_date_column() -> None:
    with connect_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE move_evaluations_ondemand ADD COLUMN IF NOT EXISTS date DATE")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS move_eval_ondemand_null_date_order_idx
                ON move_evaluations_ondemand (game_id, ply, player_id)
                WHERE date IS NULL
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS move_eval_ondemand_dated_order_idx
                ON move_evaluations_ondemand (evaluated_at, game_id, ply, player_id)
                WHERE date IS NOT NULL
                """
            )
            cur.execute("ANALYZE move_evaluations_ondemand")


def staging_batch_query(batch_rows: int, last_key: tuple[str, int, str] | None) -> str:
    key_filter = ""
    if last_key is not None:
        game_id, ply, player_id = last_key
        escaped_game_id = game_id.replace("'", "''")
        escaped_player_id = player_id.replace("'", "''")
        key_filter = (
            "AND (game_id, ply, player_id) > "
            f"('{escaped_game_id}', {int(ply)}, '{escaped_player_id}')"
        )
    return f"""
        (
            SELECT game_id, ply, player_id
            FROM move_evaluations_ondemand
            WHERE date IS NULL
              {key_filter}
            ORDER BY game_id, ply, player_id
            LIMIT {batch_rows}
        ) AS move_evaluations_ondemand_date_backfill_batch
    """


def read_staging_keys(spark: SparkSession, batch_rows: int, last_key: tuple[str, int, str] | None):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", staging_batch_query(batch_rows, last_key))
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def enrich_with_dates(spark: SparkSession, staging_keys):
    player_games = (
        spark.table("polaris.prod.player_games")
        .select("game_id", "player_id", "date")
        .dropDuplicates(["game_id", "player_id"])
    )
    return staging_keys.join(player_games, on=["game_id", "player_id"], how="inner")


def update_staging_dates(rows: Iterable, chunk_rows: int) -> int:
    updated = 0
    chunk = []
    with connect_pg() as conn:
        with conn.cursor() as cur:
            for row in rows:
                chunk.append((row.game_id, row.ply, row.player_id, row.date))
                if len(chunk) < chunk_rows:
                    continue
                updated += update_staging_date_chunk(cur, chunk)
                conn.commit()
                chunk = []
            if chunk:
                updated += update_staging_date_chunk(cur, chunk)
                conn.commit()
    return updated


def update_staging_date_chunk(cur, rows: list[tuple]) -> int:
    execute_values(
        cur,
        """
        UPDATE move_evaluations_ondemand AS m
        SET date = v.date
        FROM (VALUES %s) AS v(game_id, ply, player_id, date)
        WHERE m.game_id = v.game_id
          AND m.ply = v.ply::int
          AND m.player_id = v.player_id
          AND m.date IS NULL
        """,
        rows,
        template="(%s, %s, %s, %s::date)",
        page_size=len(rows),
    )
    return cur.rowcount if isinstance(cur.rowcount, int) and cur.rowcount > 0 else 0


def null_date_count() -> int:
    with connect_pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM move_evaluations_ondemand WHERE date IS NULL")
            return int(cur.fetchone()[0])


def run(
    batch_rows: int,
    max_batches: int | None,
    update_chunk_rows: int,
    start_after: tuple[str, int, str] | None = None,
) -> int:
    ensure_pg_date_column()
    before = null_date_count()
    logger.info("staging rows with NULL date before backfill: %s", before)
    if start_after is not None:
        logger.info("resuming after key: %s", start_after)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    total_updated = 0
    last_key = start_after
    batch_number = 0
    try:
        while max_batches is None or batch_number < max_batches:
            batch_number += 1
            started = time.monotonic()
            staging_keys = read_staging_keys(spark, batch_rows, last_key).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                staging_rows = staging_keys.count()
                if staging_rows == 0:
                    logger.info("no more NULL-date staging rows after cursor key %s", last_key)
                    break

                max_key_row = (
                    staging_keys
                    .orderBy(F.col("game_id").desc(), F.col("ply").desc(), F.col("player_id").desc())
                    .limit(1)
                    .collect()[0]
                )
                last_key = (max_key_row.game_id, int(max_key_row.ply), max_key_row.player_id)

                dated = enrich_with_dates(spark, staging_keys).persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    matched_rows = dated.count()
                    updated = update_staging_dates(dated.toLocalIterator(), update_chunk_rows)
                finally:
                    dated.unpersist()
                total_updated += updated

                elapsed = time.monotonic() - started
                logger.info(
                    "batch=%s keys=%s matched=%s updated=%s elapsed=%.1fs rows_per_sec=%.1f last_key=%s",
                    batch_number,
                    staging_rows,
                    matched_rows,
                    updated,
                    elapsed,
                    updated / elapsed if elapsed > 0 else 0,
                    last_key,
                )
            finally:
                staging_keys.unpersist()
    finally:
        spark.stop()

    after = null_date_count()
    logger.info(
        "backfill complete: updated=%s null_date_before=%s null_date_after=%s",
        total_updated,
        before,
        after,
    )
    return total_updated


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--update-chunk-rows", type=int, default=DEFAULT_UPDATE_CHUNK_ROWS)
    parser.add_argument("--start-after-game-id", default=None)
    parser.add_argument("--start-after-ply", type=int, default=None)
    parser.add_argument("--start-after-player-id", default=None)
    args = parser.parse_args(argv)
    if args.batch_rows <= 0:
        parser.error("--batch-rows must be positive")
    if args.max_batches is not None and args.max_batches <= 0:
        parser.error("--max-batches must be positive")
    if args.update_chunk_rows <= 0:
        parser.error("--update-chunk-rows must be positive")
    resume_parts = [args.start_after_game_id, args.start_after_ply, args.start_after_player_id]
    if any(part is not None for part in resume_parts) and not all(part is not None for part in resume_parts):
        parser.error("all --start-after-* arguments are required when resuming")
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    start_after = None
    if args.start_after_game_id is not None:
        start_after = (
            args.start_after_game_id,
            args.start_after_ply,
            args.start_after_player_id,
        )
    run(args.batch_rows, args.max_batches, args.update_chunk_rows, start_after)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
