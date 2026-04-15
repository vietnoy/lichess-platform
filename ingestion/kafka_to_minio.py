"""
kafka_to_minio.py — Kafka consumer that writes raw Parquet to MinIO.

Topics consumed:
  lichess.game_start  →  chess-raw/game_start/date=YYYY-MM-DD/batch_<ts>.parquet
  lichess.moves       →  chess-raw/moves/date=YYYY-MM-DD/batch_<ts>.parquet
  lichess.game_end    →  chess-raw/game_end/date=YYYY-MM-DD/batch_<ts>.parquet

Triggered by Airflow every 15 minutes — drains all available messages then exits.
"""

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
BUCKET_DEV         = os.getenv("MINIO_BUCKET_DEV")

TOPICS        = ["lichess.game_start", "lichess.moves", "lichess.game_end"]
IDLE_TIMEOUT  = 10


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
    if not client.bucket_exists(BUCKET_DEV):
        client.make_bucket(BUCKET_DEV)
        logger.info(f"Created bucket: {BUCKET_DEV}")
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
    topic_key = topic.split(".")[-1]
    obj_path  = f"{topic_key}/date={date_str}/batch_{ts}.parquet"

    minio.put_object(
        bucket_name=BUCKET_DEV,
        object_name=obj_path,
        data=buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )
    logger.info(f"Flushed {len(records):,} records → s3://{BUCKET_DEV}/{obj_path}")


def run():
    consumer = build_consumer()
    minio    = build_minio()
    consumer.subscribe(TOPICS)

    buffers:  dict[str, list] = defaultdict(list)
    last_msg  = time.time()

    logger.info(f"Consuming {TOPICS} — will exit after {IDLE_TIMEOUT}s idle...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.time() - last_msg >= IDLE_TIMEOUT:
                    logger.info("Queue drained — flushing and exiting.")
                    break
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                topic  = msg.topic()
                record = json.loads(msg.value().decode("utf-8"))
                buffers[topic].append(record)
                last_msg = time.time()

    finally:
        for topic in TOPICS:
            flush_to_minio(minio, topic, buffers[topic])
        consumer.close()


if __name__ == "__main__":
    run()
