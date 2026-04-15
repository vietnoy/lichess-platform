"""
player_discoverer.py — Kafka consumer that grows the tracked player list.

Consumes lichess.game_start events. For each game, extracts both players.
Any player not already in the known list is added to DISCOVERED_FILE.
The secondary stream in stream_ingestor.py reloads this file every hour.

Zero HTTP calls — this process never talks to the Lichess API.

Run:  python ingestion/player_discoverer.py
"""

import json
import logging
import os
import signal
import threading
import time
from pathlib import Path

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

TOPIC_GAME_START   = "lichess.game_start"
DISCOVERED_FILE    = Path(os.getenv("PLAYER_LIST_FILE", "/tmp/chess_discovered.json"))

# Flush discovered list to disk every N seconds (not on every event)
FLUSH_INTERVAL = 30
# Cap how many discovered players we track (secondary stream limit)
MAX_DISCOVERED = 300

_shutdown = threading.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received.")
    _shutdown.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVER,
        "sasl.username":      CLUSTER_API_KEY,
        "sasl.password":      CLUSTER_API_SECRET,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "group.id":           "player-discoverer",
        "auto.offset.reset":  "latest",  # only new games matter
        "enable.auto.commit": True,
    })


def load_discovered() -> dict:
    """Load existing discovered players from disk."""
    if DISCOVERED_FILE.exists():
        try:
            return json.loads(DISCOVERED_FILE.read_text())
        except Exception as e:
            logger.warning(f"Failed to load {DISCOVERED_FILE}: {e}")
    return {"players": [], "count": 0}


def save_discovered(data: dict):
    """Atomically write discovered players to disk."""
    tmp = DISCOVERED_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(DISCOVERED_FILE)


def run():
    # Load existing discovered state (survives restarts)
    state     = load_discovered()
    known_set = set(state.get("players", []))
    new_batch = set()
    last_flush = time.time()

    consumer = build_consumer()
    consumer.subscribe([TOPIC_GAME_START])

    logger.info(f"player_discoverer started — {len(known_set)} players already tracked")

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
                    white = event.get("white_id")
                    black = event.get("black_id")

                    for player in [white, black]:
                        if player and player not in known_set:
                            known_set.add(player)
                            new_batch.add(player)
                except Exception as e:
                    logger.warning(f"Failed to parse event: {e}")

            # Flush to disk periodically
            if new_batch and (time.time() - last_flush >= FLUSH_INTERVAL):
                # Keep list capped at MAX_DISCOVERED (LRU-style: keep newest)
                all_players = list(known_set)[-MAX_DISCOVERED:]
                known_set   = set(all_players)
                data        = {"players": all_players, "count": len(all_players)}
                save_discovered(data)
                logger.info(
                    f"Discovered {len(new_batch)} new player(s) this cycle — "
                    f"total tracked: {len(all_players)}"
                )
                new_batch.clear()
                last_flush = time.time()

    finally:
        # Final flush before exit
        if new_batch:
            all_players = list(known_set)[-MAX_DISCOVERED:]
            save_discovered({"players": all_players, "count": len(all_players)})
            logger.info(f"Final flush: {len(all_players)} discovered players saved")
        consumer.close()
        logger.info("player_discoverer shut down")


if __name__ == "__main__":
    run()
