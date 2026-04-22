import json
import logging
import os
import queue
import signal
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

LICHESS_TOKEN      = os.getenv("LICHESS_TOKEN")
BOOTSTRAP_SERVER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

TOPIC_GAME_START = "lichess.game_start"
TOPIC_GAME_END   = "lichess.game_end"
TOPIC_MOVES      = "lichess.moves"

HDR_NDJSON = {
    "Authorization": f"Bearer {LICHESS_TOKEN}",
    "Accept":        "application/x-ndjson",
    "Content-Type":  "text/plain",
}
HDR_JSON = {
    "Authorization": f"Bearer {LICHESS_TOKEN}",
    "Accept":        "application/json",
}

PLAYER_DB        = os.getenv("PLAYER_DB", "/tmp/chess_players.db")
POLL_INTERVAL    = 30
RATE_LIMIT_WAIT  = 90
STREAM_ROTATE_AT = 900
STREAM_ID_BASE   = "chess-analytics"

_shutdown      = threading.Event()
_producer_lock = threading.Lock()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received.")
    _shutdown.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def build_producer():
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "sasl.username":     CLUSTER_API_KEY,
        "sasl.password":     CLUSTER_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "acks":              "all",
    })


def produce(producer, topic, key, value):
    with _producer_lock:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
        )
        producer.poll(0)


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


def load_players():
    try:
        con = sqlite3.connect(PLAYER_DB)
        rows = con.execute("SELECT id FROM players").fetchall()
        con.close()
        return [r[0] for r in rows]
    except Exception as e:
        logger.warning(f"Could not load players: {e}")
        return []


def fetch_top_players():
    seen = set()
    ids  = []
    for style in ["bullet", "blitz", "rapid"]:
        try:
            r = requests.get(
                f"https://lichess.org/api/player/top/300/{style}",
                headers=HDR_JSON,
                timeout=15,
            )
            if r.status_code == 200:
                for u in r.json().get("users", []):
                    uid = u["id"]
                    if uid not in seen:
                        seen.add(uid)
                        ids.append(uid)
                logger.info(f"Fetched top {style} — {len(ids)} unique players so far")
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error fetching top {style}: {e}")
    return ids


def seed_db(players):
    now = datetime.now(timezone.utc).isoformat()
    con = sqlite3.connect(PLAYER_DB)
    con.executemany(
        "INSERT OR IGNORE INTO players (id, first_seen, last_seen) VALUES (?, ?, ?)",
        [(p, now, now) for p in players],
    )
    con.commit()
    count = con.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    con.close()
    logger.info(f"Seeded DB — total players: {count}")


class GameStream:
    def __init__(self, stream_id, producer, export_queue):
        self.stream_id    = stream_id
        self.producer     = producer
        self.export_queue = export_queue
        self._lock        = threading.Lock()
        self.active       = set()
        self.finished     = set()
        self.stop_event   = threading.Event()
        self._response    = None
        self._resp_lock   = threading.Lock()
        self.thread       = threading.Thread(
            target=self._run,
            name=f"stream-{stream_id}",
            daemon=True,
        )

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        with self._resp_lock:
            if self._response:
                try:
                    self._response.close()
                except Exception:
                    pass

    def join(self, timeout=10):
        self.thread.join(timeout=timeout)

    def total(self):
        with self._lock:
            return len(self.active) + len(self.finished)

    def needs_rotation(self):
        return self.total() >= STREAM_ROTATE_AT

    def active_ids(self):
        with self._lock:
            return list(self.active)

    def add_games(self, game_ids):
        if not game_ids:
            return False
        try:
            r = requests.post(
                f"https://lichess.org/api/stream/games/{self.stream_id}/add",
                headers=HDR_NDJSON,
                data=",".join(game_ids),
                timeout=10,
            )
        except Exception as e:
            logger.warning(f"[Stream {self.stream_id}] Add error: {e}")
            return False
        if r.status_code == 200:
            with self._lock:
                self.active.update(game_ids)
            logger.info(f"[Stream {self.stream_id}] Added {len(game_ids)} — total: {self.total()}")
            return True
        logger.warning(f"[Stream {self.stream_id}] Add failed: HTTP {r.status_code}")
        return False

    def _handle_event(self, ev):
        gid         = ev.get("id")
        status_name = ev.get("statusName", "")
        wp          = ev.get("players", {}).get("white", {})
        bp          = ev.get("players", {}).get("black", {})

        if not gid:
            return

        if status_name == "started":
            msg = {
                "game_id"      : gid,
                "timestamp"    : datetime.now(timezone.utc).isoformat(),
                "speed"        : ev.get("speed"),
                "rated"        : ev.get("rated"),
                "variant"      : ev.get("variant"),
                "white_id"     : wp.get("userId") or wp.get("user", {}).get("id") or wp.get("id"),
                "white_rating" : wp.get("rating"),
                "white_title"  : wp.get("title") or wp.get("user", {}).get("title"),
                "black_id"     : bp.get("userId") or bp.get("user", {}).get("id") or bp.get("id"),
                "black_rating" : bp.get("rating"),
                "black_title"  : bp.get("title") or bp.get("user", {}).get("title"),
                "source"       : ev.get("source"),
                "tournament_id": (ev.get("tournament") or {}).get("id"),
            }
            produce(self.producer, TOPIC_GAME_START, gid, msg)
            logger.info(f"[GAME START] {gid}  {msg['white_id']} vs {msg['black_id']}")

        elif status_name not in ("", "created", "started"):
            msg = {
                "game_id"  : gid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "winner"   : ev.get("winner"),
                "status"   : status_name,
            }
            produce(self.producer, TOPIC_GAME_END, gid, msg)
            logger.info(f"[GAME END] {gid}  winner={msg['winner']}  status={status_name}")
            with self._lock:
                self.active.discard(gid)
                self.finished.add(gid)
            self.export_queue.put(gid)

    def _run(self):
        backoff = 30
        logger.info(f"[Stream {self.stream_id}] Starting")

        while not self.stop_event.is_set():
            try:
                response = requests.post(
                    f"https://lichess.org/api/stream/games/{self.stream_id}",
                    headers=HDR_NDJSON,
                    data="",
                    stream=True,
                    timeout=86400,
                )
                with self._resp_lock:
                    self._response = response

                if response.status_code == 429:
                    wait = max(int(response.headers.get("Retry-After", RATE_LIMIT_WAIT)), RATE_LIMIT_WAIT)
                    logger.warning(f"[Stream {self.stream_id}] Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    logger.error(f"[Stream {self.stream_id}] HTTP {response.status_code}, backoff {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 120)
                    continue

                backoff = 30
                logger.info(f"[Stream {self.stream_id}] Connected")

                with self._lock:
                    to_readd = list(self.active)
                if to_readd:
                    time.sleep(1)
                    self.add_games(to_readd)

                for raw in response.iter_lines():
                    if self.stop_event.is_set():
                        break
                    if not raw:
                        continue
                    try:
                        ev = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    self._handle_event(ev)

                if self.stop_event.is_set():
                    break

                logger.info(f"[Stream {self.stream_id}] Disconnected, reconnecting in {backoff}s")
                time.sleep(backoff)

            except Exception as e:
                if self.stop_event.is_set():
                    break
                logger.warning(f"[Stream {self.stream_id}] Error: {e}, backoff {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)

        logger.info(f"[Stream {self.stream_id}] Stopped")


def export_worker(export_queue, producer):
    batch_wait    = 10
    max_batch     = 300

    while not _shutdown.is_set():
        batch     = []
        deadline  = time.time() + batch_wait

        while len(batch) < max_batch and time.time() < deadline:
            try:
                remaining = max(0.1, deadline - time.time())
                batch.append(export_queue.get(timeout=remaining))
            except queue.Empty:
                break

        if not batch:
            continue

        try:
            r = requests.post(
                "https://lichess.org/api/games/export/_ids?moves=true&clocks=true&opening=true",
                headers={**HDR_JSON, "Content-Type": "text/plain", "Accept": "application/x-ndjson"},
                data=",".join(batch),
                timeout=30,
            )

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", RATE_LIMIT_WAIT))
                logger.warning(f"[Export] Rate limited, waiting {wait}s")
                for gid in batch:
                    export_queue.put(gid)
                time.sleep(wait)
                continue

            if r.status_code != 200:
                logger.warning(f"[Export] HTTP {r.status_code}")
                for gid in batch:
                    export_queue.put(gid)
                continue

            count = 0
            for line in r.text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    game = json.loads(line)
                    produce(producer, TOPIC_MOVES, game["id"], game)
                    count += 1
                except Exception as e:
                    logger.warning(f"[Export] Failed to parse line: {e}")

            logger.info(f"[Export] Fetched {count}/{len(batch)} games")

        except Exception as e:
            logger.warning(f"[Export] Error: {e}")
            for gid in batch:
                export_queue.put(gid)
            time.sleep(5)


def status_poller_loop(game_queue):
    known_games = set()

    while not _shutdown.is_set():
        players = load_players()
        logger.info(f"Status poll — {len(players)} players")

        for i in range(0, len(players), 100):
            if _shutdown.is_set():
                break
            batch = players[i:i + 100]
            try:
                r = requests.get(
                    "https://lichess.org/api/users/status",
                    params={"ids": ",".join(batch), "withGameIds": "true"},
                    headers=HDR_JSON,
                    timeout=10,
                )
                for user in r.json():
                    playing_id = user.get("playingId")
                    if playing_id and playing_id not in known_games:
                        known_games.add(playing_id)
                        game_queue.put(playing_id)
                        logger.info(f"New game: {playing_id} ({user['id']})")
            except Exception as e:
                logger.warning(f"Status poll error: {e}")
            time.sleep(1)

        elapsed = 0
        while elapsed < POLL_INTERVAL and not _shutdown.is_set():
            time.sleep(1)
            elapsed += 1


def game_adder_loop(game_queue, streams, export_queue, producer):
    stream_counter = len(streams)
    pending_carry  = []  # active games from rotated stream waiting to be re-added

    while not _shutdown.is_set():
        try:
            pending = list(pending_carry)
            pending_carry = []
            try:
                while True:
                    pending.append(game_queue.get_nowait())
            except queue.Empty:
                pass

            current = streams[-1]

            if not current.thread.is_alive():
                logger.warning(f"[Stream {current.stream_id}] Thread dead — restarting")
                active_ids = current.active_ids()
                current.stop()
                stream_counter += 1
                new_id     = f"{STREAM_ID_BASE}-{stream_counter}"
                new_stream = GameStream(new_id, producer, export_queue)
                new_stream.start()
                streams.append(new_stream)
                pending = active_ids + pending
                current = new_stream

            if pending:
                if current.needs_rotation():
                    active_ids = current.active_ids()
                    current.stop()
                    stream_counter += 1
                    new_id     = f"{STREAM_ID_BASE}-{stream_counter}"
                    new_stream = GameStream(new_id, producer, export_queue)
                    new_stream.start()
                    streams.append(new_stream)
                    logger.info(f"Stream rotated to {new_id}, carrying {len(active_ids)} active games")
                    pending = active_ids + pending
                    current = new_stream
                    time.sleep(2)  # let new stream connect before adding

                if not current.add_games(pending):
                    pending_carry = pending  # retry next iteration

        except Exception as e:
            logger.error(f"[game-adder] Unexpected error: {e}")
            time.sleep(5)

        time.sleep(5)


def run():
    init_db()

    player_count = len(load_players())
    if player_count == 0:
        logger.info("No players in DB — fetching top players from Lichess...")
        players = fetch_top_players()
        if not players:
            logger.error("Could not fetch any players — exiting")
            return
        seed_db(players)
        player_count = len(load_players())

    logger.info(f"Starting with {player_count} tracked players")

    producer     = build_producer()
    game_queue   = queue.Queue()
    export_queue = queue.Queue()

    stream = GameStream(f"{STREAM_ID_BASE}-1", producer, export_queue)
    stream.start()
    streams = [stream]

    threading.Thread(
        target=status_poller_loop,
        args=(game_queue,),
        daemon=True,
        name="status-poller",
    ).start()

    threading.Thread(
        target=game_adder_loop,
        args=(game_queue, streams, export_queue, producer),
        daemon=True,
        name="game-adder",
    ).start()

    threading.Thread(
        target=export_worker,
        args=(export_queue, producer),
        daemon=True,
        name="export-worker",
    ).start()

    try:
        _shutdown.wait()
    except KeyboardInterrupt:
        _shutdown.set()

    logger.info("Shutting down...")
    for s in streams:
        s.stop()
    for s in streams:
        s.join(timeout=10)
    producer.flush()
    logger.info("Shutdown complete")


if __name__ == "__main__":
    run()
