import logging
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

load_dotenv()

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
BUCKET_DEV         = os.getenv("MINIO_BUCKET_DEV")
POLARIS_URI        = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE  = os.getenv("POLARIS_WAREHOUSE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_spark():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
    ]
    return (
        SparkSession.builder
        .appName("chess-process-to-polaris")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", "8")
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


def _explode_partition(iterator):
    import chess
    import json

    for row in iterator:
        game_id   = row.id
        moves_str = row.moves or ""
        if not game_id or not moves_str.strip():
            continue

        # Parse clocks array — centiseconds remaining after each half-move
        clocks = []
        try:
            if row.clocks:
                clocks = json.loads(row.clocks)
        except Exception:
            pass

        # Parse opening
        opening_eco, opening_name = None, None
        try:
            if row.opening:
                op = json.loads(row.opening)
                opening_eco  = op.get("eco")
                opening_name = op.get("name")
        except Exception:
            pass

        # Parse time control
        clock_initial, clock_increment = None, None
        try:
            if row.clock:
                ck = json.loads(row.clock)
                clock_initial   = ck.get("initial")
                clock_increment = ck.get("increment")
        except Exception:
            pass

        board = chess.Board()
        for idx, san in enumerate(moves_str.strip().split()):
            move_number = idx + 1
            try:
                fen        = board.fen()
                move       = board.push_san(san)
                whose_moved = "white" if move_number % 2 == 1 else "black"

                # clocks[idx] = remaining after this half-move (centiseconds)
                clock_remaining = clocks[idx] if idx < len(clocks) else None

                yield (
                    game_id,
                    move_number,
                    move.uci(),
                    fen,
                    whose_moved,
                    clock_remaining,
                    opening_eco,
                    opening_name,
                    clock_initial,
                    clock_increment,
                    row.speed,
                    row.perf,
                    row.variant,
                )
            except Exception:
                break


_move_schema = StructType([
    StructField("game_id",          StringType(),  True),
    StructField("move_number",      IntegerType(), True),
    StructField("move",             StringType(),  True),
    StructField("fen",              StringType(),  True),
    StructField("whose_moved",      StringType(),  True),
    StructField("clock_remaining",  IntegerType(), True),
    StructField("opening_eco",      StringType(),  True),
    StructField("opening_name",     StringType(),  True),
    StructField("clock_initial",    IntegerType(), True),
    StructField("clock_increment",  IntegerType(), True),
    StructField("speed",            StringType(),  True),
    StructField("perf",             StringType(),  True),
    StructField("variant",          StringType(),  True),
])


def run(date_str: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    moves_path      = f"s3a://{BUCKET_DEV}/moves/date={date_str}"
    game_start_path = f"s3a://{BUCKET_DEV}/game_start/date={date_str}"
    game_end_path   = f"s3a://{BUCKET_DEV}/game_end/date={date_str}"

    logger.info(f"Reading raw game exports for date={date_str}")
    raw_games = spark.read.parquet(moves_path)

    if raw_games.head(1) == []:
        logger.info(f"No games found for date={date_str}, skipping")
        spark.stop()
        return

    # --- 1. Explode moves with clocks and game-level metadata ---
    moves_df = (
        raw_games.rdd
        .mapPartitions(_explode_partition)
        .toDF(_move_schema)
    )

    # --- 2. Join game_start for player metadata ---
    game_start_df = (
        spark.read.parquet(game_start_path)
        .drop("ingested_at")
        .select(
            col("game_id"),
            col("white_id"),
            col("white_rating"),
            col("white_title"),
            col("black_id"),
            col("black_rating"),
            col("black_title"),
            col("tournament_id"),
        )
    )

    # --- 3. Join game_end for result ---
    game_end_df = (
        spark.read.parquet(game_end_path)
        .select(
            col("game_id"),
            col("winner"),
            col("status").alias("end_status"),
        )
    )

    # --- 4. Assemble flat table ---
    player_moves = (
        moves_df
        .join(game_start_df, on="game_id", how="inner")
        .join(game_end_df,   on="game_id", how="inner")
        .withColumn("date", lit(date_str))
    )

    player_moves.writeTo("polaris.prod.chess_move_events").partitionedBy("date").append()
    logger.info(f"Done — date={date_str} written to Polaris")
    spark.stop()


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-16"
    run(date)
