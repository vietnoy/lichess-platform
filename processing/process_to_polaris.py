import logging
import os
import requests

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lit, udf, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

load_dotenv()

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
BUCKET_DEV         = os.getenv("MINIO_BUCKET_DEV")
POLARIS_URI        = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE  = os.getenv("POLARIS_WAREHOUSE")
STOCKFISH_URL      = os.getenv("STOCKFISH_URL")
STOCKFISH_DEPTH    = int(os.getenv("STOCKFISH_DEPTH"))

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
    for row in iterator:
        game_id   = row.id
        moves_str = row.moves or ""
        if not game_id or not moves_str.strip():
            continue
        board = chess.Board()
        for move_number, san in enumerate(moves_str.strip().split(), start=1):
            try:
                fen  = board.fen()
                move = board.push_san(san)
                yield (game_id, move_number, move.uci(), fen)
            except Exception:
                break


_eval_schema = StructType([
    StructField("cp",        IntegerType(), True),
    StructField("best_move", StringType(),  True),
])


def _make_eval_udf(url: str, depth: int):
    @udf(returnType=_eval_schema)
    def stockfish_eval_udf(fen):
        try:
            r = requests.get(url, params={"fen": fen, "depth": depth}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                return (data.get("cp"), data.get("best_move"))
        except Exception:
            pass
        return (None, None)
    return stockfish_eval_udf


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

    # --- 1. Explode moves distributed across all executors ---
    moves_df = (
        raw_games.rdd
        .mapPartitions(_explode_partition)
        .toDF(["game_id", "move_number", "move", "fen"])
    )

    # --- 2. Annotate with Stockfish — each executor calls concurrently ---
    stockfish_eval_udf = _make_eval_udf(STOCKFISH_URL, STOCKFISH_DEPTH)
    moves_df = (
        moves_df
        .withColumn("eval_result", stockfish_eval_udf(col("fen")))
        .withColumn("eval_cp",    col("eval_result.cp"))
        .withColumn("best_move",  col("eval_result.best_move"))
        .drop("eval_result")
    )

    # --- 3. Compute eval_delta and classification via window functions ---
    window = Window.partitionBy("game_id").orderBy("move_number")
    moves_df = (
        moves_df
        .withColumn("eval_delta",  col("eval_cp") - lag("eval_cp", 1).over(window))
        .withColumn("whose_moved", when(col("move_number") % 2 == 1, "white").otherwise("black"))
        .withColumn(
            "classification",
            when(col("eval_delta").isNull(), "unknown")
            .when(
                (col("whose_moved") == "white") & (-col("eval_delta") >= 300) |
                (col("whose_moved") == "black") & ( col("eval_delta") >= 300), "blunder"
            )
            .when(
                (col("whose_moved") == "white") & (-col("eval_delta") >= 100) |
                (col("whose_moved") == "black") & ( col("eval_delta") >= 100), "mistake"
            )
            .when(
                (col("whose_moved") == "white") & (-col("eval_delta") >= 50) |
                (col("whose_moved") == "black") & ( col("eval_delta") >= 50), "inaccuracy"
            )
            .when(
                (col("whose_moved") == "white") & (-col("eval_delta") >= -20) |
                (col("whose_moved") == "black") & ( col("eval_delta") >= -20), "good"
            )
            .otherwise("excellent")
        )
    )

    # --- 4. Join with game metadata and write ---
    game_start_df = spark.read.parquet(game_start_path).drop("ingested_at")
    game_end_df   = spark.read.parquet(game_end_path).select(
        col("game_id"),
        col("winner"),
        col("status").alias("end_status"),
    )

    player_moves = (
        moves_df
        .join(game_start_df, on="game_id", how="inner")
        .join(game_end_df,   on="game_id", how="inner")
        .withColumn("date", lit(date_str))
    )

    player_moves.writeTo("polaris.prod.chess_raw_events").partitionedBy("date").append()
    logger.info(f"Done — date={date_str} written to Polaris")
    spark.stop()


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-16"
    run(date)
