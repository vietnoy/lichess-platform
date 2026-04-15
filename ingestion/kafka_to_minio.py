"""
kafka_to_minio.py — Kafka consumer that writes raw Parquet to MinIO.

Topics consumed:
  lichess.game_start  →  chess-raw/game_start/date=YYYY-MM-DD/batch_<ts>.parquet
  lichess.moves       →  chess-raw/moves/date=YYYY-MM-DD/batch_<ts>.parquet
  lichess.game_end    →  chess-raw/game_end/date=YYYY-MM-DD/batch_<ts>.parquet

Run standalone:  python kafka_to_minio.py
Run via Airflow: BashOperator → python /ingestion/kafka_to_minio.py --batch-seconds 300
"""

import argparse
import io
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
BUCKET_RAW         = os.getenv("MINIO_BUCKET_RAW", "chess-raw")

TOPICS = ["lichess.game_start", "lichess.moves", "lichess.game_end"]


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "sasl.username":      CLUSTER_API_KEY,
        "sasl.password":      CLUSTER_API_SECRET,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "group.id":           "kafka-to-minio",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })


def build_minio() -> Minio:
    use_ssl = not MINIO_ENDPOINT.startswith("localhost") and not MINIO_ENDPOINT.startswith("minio:")
    client  = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=use_ssl)
    if not client.bucket_exists(BUCKET_RAW):
        client.make_bucket(BUCKET_RAW)
        logger.info(f"Created bucket: {BUCKET_RAW}")
    return client


def flush_to_minio(minio: Minio, topic: str, records: list[dict]):
    if not records:
        return

    df        = pd.DataFrame(records)
    table     = pa.Table.from_pandas(df, preserve_index=False)
    buf       = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    buf.seek(0)

    date_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ts        = int(time.time())
    topic_key = topic.split(".")[-1]  # game_start / moves / game_end
    obj_path  = f"{topic_key}/date={date_str}/batch_{ts}.parquet"

    minio.put_object(
        bucket_name=BUCKET_RAW,
        object_name=obj_path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    logger.info(f"Flushed {len(records):,} records → s3://{BUCKET_RAW}/{obj_path}")


def run(batch_seconds: int = 300):
    consumer = build_consumer()
    minio    = build_minio()
    consumer.subscribe(TOPICS)

    buffers:    dict[str, list] = defaultdict(list)
    last_flush: dict[str, float] = {t: time.time() for t in TOPICS}

    logger.info(f"Consuming {TOPICS}  (flush every {batch_seconds}s)")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                topic  = msg.topic()
                record = json.loads(msg.value().decode("utf-8"))
                buffers[topic].append(record)

            now = time.time()
            for topic in TOPICS:
                if now - last_flush[topic] >= batch_seconds:
                    if buffers[topic]:
                        flush_to_minio(minio, topic, buffers[topic])
                        buffers[topic] = []
                    last_flush[topic] = now

    except KeyboardInterrupt:
        logger.info("Shutting down — flushing remaining buffers...")
        for topic in TOPICS:
            if buffers[topic]:
                flush_to_minio(minio, topic, buffers[topic])
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-seconds", type=int, default=300,
                        help="Flush buffer to MinIO every N seconds (default 300)")
    args = parser.parse_args()
    run(batch_seconds=args.batch_seconds)
