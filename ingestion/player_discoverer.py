import json
import logging
import os
import signal
import sqlite3
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

PLAYER_DB      = os.getenv("PLAYER_DB", "/tmp/chess_players.db")
TOPIC_GAME_START = "lichess.game_start"
FLUSH_INTERVAL   = 30

_shutdown = threading.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received.")
    _shutdown.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def build_consumer():
    return Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVER,
        "sasl.username":      CLUSTER_API_KEY,
        "sasl.password":      CLUSTER_API_SECRET,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "group.id":           "player-discoverer",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })


def init_db():
    con = sqlite3.connect(PLAYER_DB)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id         TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen  TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()


def save_players(new_players):
    if not new_players:
        return
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(PLAYER_DB)
    con.executemany(
        "INSERT OR IGNORE INTO players (id, first_seen, last_seen) VALUES (?, ?, ?)",
        [(p, now, now) for p in new_players],
    )
    con.commit()
    total = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    con.close()
    logger.info(f"Saved {len(new_players)} new player(s) — total: {total}")


def run():
    init_db()

    con      = sqlite3.connect(PLAYER_DB)
    existing = set(r[0] for r in con.execute("SELECT id FROM players").fetchall())
    con.close()
    logger.info(f"player_discoverer started — {len(existing)} players already tracked")

    consumer  = build_consumer()
    consumer.subscribe([TOPIC_GAME_START])

    new_batch  = set()
    last_flush = time.time()

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
            else:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    for player in [event.get("white_id"), event.get("black_id")]:
                        if player and player not in existing:
                            existing.add(player)
                            new_batch.add(player)
                except Exception as e:
                    logger.warning(f"Failed to parse event: {e}")

            if new_batch and (time.time() - last_flush >= FLUSH_INTERVAL):
                save_players(new_batch)
                new_batch.clear()
                last_flush = time.time()

    finally:
        if new_batch:
            save_players(new_batch)
        consumer.close()
        logger.info("player_discoverer shut down")


if __name__ == "__main__":
    run()
