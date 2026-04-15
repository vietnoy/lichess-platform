"""
annotate.py — Read raw moves Parquet from MinIO chess-raw, call Lichess Cloud
Eval API per move, write enriched Parquet to Iceberg location so StarRocks
can query via polaris_catalog.

Input:   chess-raw/moves/date=<date>/batch_*.parquet
Output:  chess-raw/iceberg/prod/moves/data/date=<date>/batch_<ts>.parquet

New columns added:
  eval_cp        (int)   — centipawn evaluation (from white's perspective)
  best_move      (str)   — best move in UCI notation
  eval_delta     (int)   — eval change after this move (negative = loss for mover)
  classification (str)   — blunder / mistake / inaccuracy / good / excellent

Run:  python processing/annotate.py [--date YYYY-MM-DD]
"""

import argparse
import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_RAW       = os.getenv("MINIO_BUCKET_RAW", "chess-raw")

# Iceberg table location — where StarRocks reads via polaris_catalog
ICEBERG_PREFIX   = "iceberg/prod/moves/data"

STOCKFISH_URL   = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
STOCKFISH_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "18"))


def build_minio() -> Minio:
    use_ssl = not MINIO_ENDPOINT.startswith("localhost") and not MINIO_ENDPOINT.startswith("minio:")
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=use_ssl)


def get_cloud_eval(fen: str) -> dict | None:
    """Call local Stockfish API. Returns cp + best_move or None on error."""
    try:
        r = requests.get(STOCKFISH_URL, params={"fen": fen, "depth": STOCKFISH_DEPTH}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return {
                "eval_cp":   data.get("cp"),
                "best_move": data.get("best_move"),
            }
        logger.warning(f"Stockfish API {r.status_code} for fen={fen[:40]}")
    except Exception as e:
        logger.warning(f"Stockfish API error: {e}")
    return None


def classify(eval_delta: float | None, whose_moved: str | None) -> str:
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


def already_annotated(minio: Minio, date_str: str, batch_name: str) -> bool:
    obj = f"{ICEBERG_PREFIX}/date={date_str}/{batch_name}"
    try:
        minio.stat_object(BUCKET_RAW, obj)
        return True
    except Exception:
        return False


def list_raw_objects(minio: Minio, date_str: str) -> list[str]:
    prefix  = f"moves/date={date_str}/"
    objects = minio.list_objects(BUCKET_RAW, prefix=prefix)
    return [obj.object_name for obj in objects]


def read_parquet(minio: Minio, obj_path: str) -> pd.DataFrame:
    resp = minio.get_object(BUCKET_RAW, obj_path)
    return pd.read_parquet(io.BytesIO(resp.read()))


def write_parquet(minio: Minio, df: pd.DataFrame, obj_path: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    minio.put_object(
        bucket_name=BUCKET_RAW,
        object_name=obj_path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )


def annotate_batch(df: pd.DataFrame) -> pd.DataFrame:
    evals      = []
    best_moves = []

    for i, row in enumerate(df.itertuples(index=False)):
        fen = getattr(row, "fen", None)
        if fen:
            result = get_cloud_eval(fen)
            evals.append(result["eval_cp"]   if result else None)
            best_moves.append(result["best_move"] if result else None)
        else:
            evals.append(None)
            best_moves.append(None)

        if (i + 1) % 50 == 0:
            logger.info(f"  Annotated {i+1}/{len(df)} moves")

    df = df.copy()
    df["eval_cp"]   = pd.array(evals,      dtype=pd.Int64Dtype())
    df["best_move"] = best_moves

    df = df.sort_values(["game_id", "move_number"])
    df["eval_delta"] = df.groupby("game_id")["eval_cp"].diff()

    df["whose_moved"] = df["move_number"].apply(
        lambda n: "white" if (n % 2 == 1) else "black"
    )

    df["classification"] = df.apply(
        lambda r: classify(r["eval_delta"], r["whose_moved"]), axis=1
    )

    return df


def run(date_str: str):
    minio   = build_minio()
    objects = list_raw_objects(minio, date_str)

    if not objects:
        logger.info(f"No raw move files found for date={date_str} — nothing to annotate")
        return

    logger.info(f"Found {len(objects)} raw file(s) for date={date_str}")
    total_written = 0

    for obj_path in objects:
        batch_name = os.path.basename(obj_path)

        if already_annotated(minio, date_str, batch_name):
            logger.info(f"Skipping already annotated: {batch_name}")
            continue

        logger.info(f"Processing: {obj_path}")
        df = read_parquet(minio, obj_path)
        logger.info(f"  Loaded {len(df):,} moves")

        df = annotate_batch(df)

        out_path = f"{ICEBERG_PREFIX}/date={date_str}/{batch_name}"
        write_parquet(minio, df, out_path)
        logger.info(f"  Written → s3://{BUCKET_RAW}/{out_path}")
        total_written += len(df)

    logger.info(f"Done — {total_written:,} moves annotated for date={date_str}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Date partition to annotate (default: today UTC)",
    )
    args = parser.parse_args()
    run(date_str=args.date)
