import logging
import os
import requests

import chess
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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
    ]
    return (
        SparkSession.builder
        .appName("chess-process-to-polaris")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", "4")
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


def explode_games_to_moves(games_df: pd.DataFrame) -> pd.DataFrame:
    records = list(games_df[["id", "moves"]].itertuples(index=False, name=None))
    rows = [row for record in records for row in _explode_game(record)]
    return pd.DataFrame(rows)


def _explode_game(record):
    game_id, moves_str = record
    if not game_id or not (moves_str or "").strip():
        return []
    rows  = []
    board = chess.Board()
    for move_number, san in enumerate((moves_str or "").strip().split(), start=1):
        try:
            fen  = board.fen()
            move = board.push_san(san)
            rows.append({
                "game_id":     game_id,
                "move_number": move_number,
                "move":        move.uci(),
                "fen":         fen,
            })
        except Exception:
            break
    return rows


def stockfish_eval(fen: str):
    try:
        r = requests.get(STOCKFISH_URL, params={"fen": fen, "depth": STOCKFISH_DEPTH}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("cp"), data.get("best_move")
    except Exception:
        pass
    return None, None


def classify_move(eval_delta, whose_moved: str) -> str:
    if eval_delta is None or whose_moved is None:
        return "unknown"
    loss = -eval_delta if whose_moved == "white" else eval_delta
    if loss >= 300:
        return "blunder"
    if loss >= 100:
        return "mistake"
    if loss >= 50:
        return "inaccuracy"
    if loss >= -20:
        return "good"
    return "excellent"


def annotate_moves(df: pd.DataFrame) -> pd.DataFrame:
    evals, best_moves = [], []
    for i, row in enumerate(df.itertuples(index=False)):
        cp, bm = stockfish_eval(row.fen)
        evals.append(cp)
        best_moves.append(bm)
        if (i + 1) % 200 == 0:
            logger.info(f"  Annotated {i + 1} moves")

    df = df.copy()
    df["eval_cp"]   = pd.array(evals, dtype=pd.Int64Dtype())
    df["best_move"] = best_moves
    df = df.sort_values(["game_id", "move_number"])
    df["eval_delta"]  = df.groupby("game_id")["eval_cp"].diff()
    df["whose_moved"] = df["move_number"].apply(lambda n: "white" if n % 2 == 1 else "black")
    df["classification"] = df.apply(
        lambda r: classify_move(r["eval_delta"], r["whose_moved"]), axis=1
    )
    return df


def run(date_str: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    moves_path      = f"s3a://{BUCKET_DEV}/moves/date={date_str}"
    game_start_path = f"s3a://{BUCKET_DEV}/game_start/date={date_str}"
    game_end_path   = f"s3a://{BUCKET_DEV}/game_end/date={date_str}"

    logger.info(f"Reading raw game exports for date={date_str}")
    games_pd = spark.read.parquet(moves_path).toPandas()

    if games_pd.empty:
        logger.info(f"No games found for date={date_str}, skipping")
        spark.stop()
        return

    logger.info(f"Loaded exploding into individual moves")
    moves_pd = explode_games_to_moves(games_pd)
    logger.info(f"Exploded to moves annotating with Stockfish")
    moves_pd = annotate_moves(moves_pd)

    logger.info("Reading game_start and game_end")
    game_start_df = spark.read.parquet(game_start_path)
    game_end_df   = spark.read.parquet(game_end_path).select(
        col("game_id"),
        col("winner"),
        col("status").alias("end_status"),
    )

    moves_df = spark.createDataFrame(moves_pd)

    player_moves = (
        moves_df
        .join(game_start_df, on="game_id", how="inner")
        .join(game_end_df,   on="game_id", how="left")
        .withColumn("date", lit(date_str))
    )

    player_moves.writeTo("polaris.prod.chess_raw_events").append()

    logger.info(f"Done — date={date_str} written to Polaris")
    spark.stop()


if __name__ == "__main__":
    date="2026-04-16"
    run(date)
