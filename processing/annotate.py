"""
annotate.py — Read raw moves Parquet from MinIO, call Lichess Cloud Eval API
per move, write enriched Parquet back to chess-enriched bucket.

Input:   chess-raw/moves/date=<date>/batch_*.parquet
Output:  chess-enriched/moves/date=<date>/batch_<ts>.parquet

New columns added:
  eval_cp      (int)   — centipawn evaluation from Lichess Cloud Eval
  best_move    (str)   — best move in UCI notation
  eval_delta   (int)   — change in eval vs previous move (blunder signal)
  classification (str) — blunder / mistake / inaccuracy / good / excellent

Run:  python annotate.py [--date YYYY-MM-DD]
"""

import argparse
import io
import logging
import os
import time
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
BUCKET_RAW       = os.getenv("MINIO_BUCKET_RAW",      "chess-raw")
BUCKET_ENRICHED  = os.getenv("MINIO_BUCKET_ENRICHED", "chess-enriched")

CLOUD_EVAL_URL   = "https://lichess.org/api/cloud-eval"
RATE_LIMIT_DELAY = 0.5   # seconds between API calls — Lichess is generous but not unlimited


def build_minio() -> Minio:
    use_ssl = not MINIO_ENDPOINT.startswith("localhost") and not MINIO_ENDPOINT.startswith("minio:")
    client  = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=use_ssl)
    if not client.bucket_exists(BUCKET_ENRICHED):
        client.make_bucket(BUCKET_ENRICHED)
        logger.info(f"Created bucket: {BUCKET_ENRICHED}")
    return client


def get_cloud_eval(fen: str) -> dict | None:
    """Call Lichess Cloud Eval API. Returns dict with cp + best_move, or None on miss."""
    try:
        r = requests.get(
            CLOUD_EVAL_URL,
            params={"fen": fen, "multiPv": 1},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            pvs  = data.get("pvs", [])
            if pvs:
                return {
                    "eval_cp":   pvs[0].get("cp"),
                    "best_move": pvs[0].get("moves", "").split()[0] if pvs[0].get("moves") else None,
                }
        elif r.status_code == 404:
            return None  # position not in cloud eval cache
        else:
            logger.warning(f"Cloud eval returned {r.status_code} for fen={fen[:40]}")
    except Exception as e:
        logger.warning(f"Cloud eval error: {e}")
    return None


def classify(eval_delta: int | None) -> str:
    if eval_delta is None:
        return "unknown"
    loss = -eval_delta  # positive = lost centipawns
    if loss >= 300:
        return "blunder"
    if loss >= 100:
        return "mistake"
    if loss >= 50:
        return "inaccuracy"
    if loss >= -20:
        return "good"
    return "excellent"


def list_raw_objects(minio: Minio, date_str: str) -> list[str]:
    prefix  = f"moves/date={date_str}/"
    objects = minio.list_objects(BUCKET_RAW, prefix=prefix)
    return [obj.object_name for obj in objects]


def read_parquet_from_minio(minio: Minio, bucket: str, obj_path: str) -> pd.DataFrame:
    resp = minio.get_object(bucket, obj_path)
    buf  = io.BytesIO(resp.read())
    return pd.read_parquet(buf)


def write_parquet_to_minio(minio: Minio, df: pd.DataFrame, obj_path: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)
    minio.put_object(
        bucket_name=BUCKET_ENRICHED,
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
            time.sleep(RATE_LIMIT_DELAY)
        else:
            evals.append(None)
            best_moves.append(None)

        if (i + 1) % 100 == 0:
            logger.info(f"  Annotated {i+1}/{len(df)} moves")

    df = df.copy()
    df["eval_cp"]   = evals
    df["best_move"] = best_moves

    # eval_delta: difference in eval per game, from white's perspective
    df = df.sort_values(["game_id", "move_number"])
    df["eval_delta"] = df.groupby("game_id")["eval_cp"].diff()
    df["classification"] = df["eval_delta"].apply(classify)

    return df


def run(date_str: str):
    minio   = build_minio()
    objects = list_raw_objects(minio, date_str)

    if not objects:
        logger.info(f"No raw move files found for date={date_str}")
        return

    logger.info(f"Found {len(objects)} raw file(s) for date={date_str}")

    for obj_path in objects:
        logger.info(f"Processing: {obj_path}")
        df = read_parquet_from_minio(minio, BUCKET_RAW, obj_path)
        logger.info(f"  Loaded {len(df):,} moves")

        df = annotate_batch(df)

        ts       = int(time.time())
        out_path = f"moves/date={date_str}/batch_{ts}.parquet"
        write_parquet_to_minio(minio, df, out_path)
        logger.info(f"  Written → s3://{BUCKET_ENRICHED}/{out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Date partition to annotate (default: today UTC)",
    )
    args = parser.parse_args()
    run(date_str=args.date)
