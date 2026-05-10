import logging
import os
import sys
from datetime import datetime
from typing import Any

import requests
from dotenv import load_dotenv
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
POLARIS_URI = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE = os.getenv("POLARIS_WAREHOUSE")
STOCKFISH_URL = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
TRACKED_PLAYERS = os.getenv("TRACKED_PLAYERS", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


evaluation_schema = StructType([
    StructField("game_id", StringType(), False),
    StructField("ply", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("fen", StringType(), True),
    StructField("played_move", StringType(), True),
    StructField("best_move", StringType(), True),
    StructField("eval_cp", IntegerType(), True),
    StructField("mate", IntegerType(), True),
    StructField("eval_swing_cp_from_prev", IntegerType(), True),
    StructField("classification", StringType(), True),
    StructField("evaluated_at", TimestampType(), True),
])


def build_spark() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
    ]
    return (
        SparkSession.builder
        .appName("chess-analyze-blunders")
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


def parse_tracked_players(raw_players: str) -> list[str]:
    # Preserve original case — chess_move_events stores white_id/black_id verbatim and `isin` is case-sensitive.
    return [player.strip() for player in raw_players.split(",") if player.strip()]


def ensure_table(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS polaris.prod.move_evaluations (
            game_id STRING NOT NULL,
            ply INT NOT NULL,
            date DATE NOT NULL,
            fen STRING,
            played_move STRING,
            best_move STRING,
            eval_cp INT,
            mate INT,
            eval_swing_cp_from_prev INT,
            classification STRING,
            evaluated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def top_players_for_day(spark: SparkSession, date_str: str) -> list[str]:
    rows = spark.sql(
        f"""
        SELECT player_id
        FROM (
            SELECT white_id AS player_id, game_id
            FROM polaris.prod.chess_move_events
            WHERE date = '{date_str}' AND white_id IS NOT NULL
            UNION ALL
            SELECT black_id AS player_id, game_id
            FROM polaris.prod.chess_move_events
            WHERE date = '{date_str}' AND black_id IS NOT NULL
        )
        GROUP BY player_id
        ORDER BY COUNT(DISTINCT game_id) DESC
        LIMIT 5
        """
    ).collect()
    return [row.player_id for row in rows]


def stockfish_eval(session: requests.Session, fen: str) -> dict[str, Any] | None:
    payload = {"fen": fen, "depth": 12}
    for attempt in range(2):
        try:
            response = session.post(STOCKFISH_URL, json=payload, timeout=30)
            if response.status_code < 500:
                response.raise_for_status()
                return response.json()
            response.raise_for_status()
        except (requests.RequestException, ValueError) as exc:
            if attempt == 1:
                logger.warning(f"Stockfish evaluation failed after retry: {exc}")
                return None
    return None


def classify_move(cp_after: int | None, cp_before: int | None, mover: str) -> tuple[int | None, str | None]:
    # Stockfish returns white-relative cp. To classify a move by `mover`, compute the drop
    # from `mover`'s own perspective: positive `drop` = mover's position got worse.
    if cp_after is None or cp_before is None:
        return None, None
    if mover == "white":
        drop = cp_before - cp_after
        swing = cp_after - cp_before
    else:
        drop = cp_after - cp_before
        swing = cp_before - cp_after

    if drop >= 200:
        classification = "blunder"
    elif drop >= 100:
        classification = "mistake"
    elif drop >= 50:
        classification = "inaccuracy"
    else:
        classification = "good"
    return swing, classification


def analyze_rows(
    rows: list[Row],
    date_str: str,
    existing_cp_by_ply: dict[tuple[str, int], int | None],
) -> list[tuple[Any, ...]]:
    # First pass: evaluate every position serially against the single Stockfish replica.
    session = requests.Session()
    evaluated_at = datetime.utcnow()
    raw: list[dict] = []
    for idx, row in enumerate(rows, start=1):
        ev = stockfish_eval(session, row.fen)
        raw.append({
            "row": row,
            "cp": ev.get("cp") if ev else None,
            "mate": ev.get("mate") if ev else None,
            "best_move": ev.get("best_move") if ev else None,
        })
        if idx % 100 == 0:
            logger.info(f"Processed {idx} Stockfish evaluations")

    # Combined cp lookup so we can find the position AFTER each move (= ply+1's pre-move eval).
    cp_lookup: dict[tuple[str, int], int | None] = {
        (r["row"].game_id, r["row"].ply): r["cp"] for r in raw
    }
    for key, cp in existing_cp_by_ply.items():
        cp_lookup.setdefault(key, cp)

    # Second pass: each row's classification describes its own played_move.
    # row N stores fen=before-move-N, played_move=move-N, whose_moved=mover-of-N.
    # Quality of move-N = eval-change from cp_before_N to cp_before_(N+1) in mover-N's perspective.
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    results: list[tuple[Any, ...]] = []
    for r in raw:
        row = r["row"]
        cp_before = r["cp"]
        cp_after = cp_lookup.get((row.game_id, row.ply + 1))
        swing, classification = classify_move(cp_after, cp_before, row.whose_moved)

        results.append((
            row.game_id,
            row.ply,
            target_date,
            row.fen,
            row.played_move,
            r["best_move"],
            cp_before,
            r["mate"],
            swing,
            classification,
            evaluated_at,
        ))

    return results


def run(date_str: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)

    tracked_players = parse_tracked_players(TRACKED_PLAYERS)
    if not tracked_players:
        tracked_players = top_players_for_day(spark, date_str)

    if not tracked_players:
        logger.info(f"No tracked players found for date={date_str}, skipping")
        spark.stop()
        return

    logger.info(f"Analyzing date={date_str} for {len(tracked_players)} tracked players")

    moves = (
        spark.table("polaris.prod.chess_move_events")
        .where(col("date") == lit(date_str))
        .where(col("white_id").isin(tracked_players) | col("black_id").isin(tracked_players))
        .select(
            col("game_id"),
            col("move_number").alias("ply"),
            col("whose_moved"),
            col("move").alias("played_move"),
            col("fen"),
        )
    )

    existing = spark.table("polaris.prod.move_evaluations").where(col("date") == to_date(lit(date_str)))
    remaining = (
        moves
        .join(existing.select("game_id", "ply"), on=["game_id", "ply"], how="left_anti")
        .orderBy("game_id", "ply")
    )

    rows = remaining.collect()
    logger.info(f"Evaluating {len(rows)} moves not already present for date={date_str}")

    if rows:
        existing_cp_by_ply = {
            (row.game_id, row.ply): row.eval_cp
            for row in existing.select("game_id", "ply", "eval_cp").collect()
        }
        result_rows = analyze_rows(rows, date_str, existing_cp_by_ply)
        new_results = spark.createDataFrame(result_rows, evaluation_schema)
        output = existing.unionByName(new_results)
    else:
        output = existing

    output.writeTo("polaris.prod.move_evaluations").overwritePartitions()
    logger.info(f"Done - date={date_str} written to Polaris move_evaluations")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: analyze_blunders.py YYYY-MM-DD")
    run(sys.argv[1])
