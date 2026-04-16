"""
stream_ingestor.py — Multi-stream ingestor with rate-limit-safe reconnects.

Primary stream:  tracks up to 900 seed players across 3 staggered connections.
Secondary stream: tracks discovered players (written by player_discoverer.py).

Key behaviours:
  - No /api/users/status polling — zero status calls
  - Staggered batch reconnects: 15s gap between each batch connection
  - 429 → 90s mandatory cooldown, then reconnect that batch only
  - Bulk sweep on reconnect: fetches completed games missed during downtime
  - game_id used as Kafka message key for downstream deduplication

Run:  python ingestion/stream_ingestor.py
"""

import chess
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

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
TOPIC_MOVES      = "lichess.moves"
TOPIC_GAME_END   = "lichess.game_end"

HDR_NDJSON = {
    "Authorization": f"Bearer {LICHESS_TOKEN}",
    "Accept": "application/x-ndjson",
    "Content-Type": "text/plain",
}
HDR_JSON = {
    "Authorization": f"Bearer {LICHESS_TOKEN}",
    "Accept": "application/json",
}

# Shared file written by player_discoverer.py
DISCOVERED_FILE    = Path(os.getenv("PLAYER_LIST_FILE", "/tmp/chess_discovered.json"))
RECONNECT_STAGGER  = 15    # seconds between each batch connection at startup
RATE_LIMIT_COOLDOWN = 90   # mandatory wait on 429
MAX_BATCH          = 300   # Lichess hard limit per connection
# Refresh secondary stream with discovered players every N seconds
SECONDARY_REFRESH  = int(os.getenv("SECONDARY_REFRESH", "3600"))

_shutdown = threading.Event()
_producer_lock = threading.Lock()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received.")
    _shutdown.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "sasl.username":     CLUSTER_API_KEY,
        "sasl.password":     CLUSTER_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "acks":              "all",
    })


def produce(producer: Producer, topic: str, key: str, value: dict):
    with _producer_lock:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
        )
        producer.poll(0)


def get_top_players() -> list[str]:
    """Fetch top players across bullet/blitz/rapid — called ONCE at startup."""
    game_styles = ["bullet", "blitz", "rapid"]
    seen = set()
    ids  = []
    for style in game_styles:
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
                logger.info(f"Fetched {style} top players — total unique so far: {len(ids)}")
            else:
                logger.warning(f"top/{style} returned {r.status_code}")
            time.sleep(2)  # be polite between styles
        except Exception as e:
            logger.error(f"Error fetching top {style} players: {e}")
    return ids


def load_discovered_players(primary_ids: set) -> list[str]:
    """Load players discovered by player_discoverer.py, excluding primaries."""
    if not DISCOVERED_FILE.exists():
        return []
    try:
        data = json.loads(DISCOVERED_FILE.read_text())
        players = [p for p in data.get("players", []) if p not in primary_ids]
        return players[:MAX_BATCH]
    except Exception as e:
        logger.warning(f"Failed to read discovered players: {e}")
    return []


def bulk_sweep(producer: Producer, player_ids: list[str], since_ts: int):
    """
    Fetch recently completed games for players missed during downtime.
    since_ts: Unix timestamp in milliseconds of last disconnect.
    Only sweeps the first 10 players to avoid rate limiting.
    """
    logger.info(f"Bulk sweep for {min(len(player_ids), 10)} players since {since_ts}")
    swept = 0
    for uid in player_ids[:10]:
        try:
            r = requests.get(
                f"https://lichess.org/api/games/user/{uid}",
                headers=HDR_NDJSON,
                params={"since": since_ts, "max": 5, "ongoing": "false"},
                timeout=15,
                stream=True,
            )
            for line in r.iter_lines():
                if not line:
                    continue
                try:
                    game = json.loads(line)
                    gid  = game.get("id")
                    if not gid:
                        continue
                    players_data = game.get("players", {})
                    wp = players_data.get("white", {}).get("user", {})
                    bp = players_data.get("black", {}).get("user", {})
                    msg = {
                        "game_id"      : gid,
                        "timestamp"    : datetime.now(timezone.utc).isoformat(),
                        "speed"        : game.get("speed"),
                        "rated"        : game.get("rated"),
                        "variant"      : game.get("variant"),
                        "white_id"     : wp.get("id"),
                        "white_rating" : players_data.get("white", {}).get("rating"),
                        "white_title"  : wp.get("title"),
                        "black_id"     : bp.get("id"),
                        "black_rating" : players_data.get("black", {}).get("rating"),
                        "black_title"  : bp.get("title"),
                        "source"       : "bulk_sweep",
                        "tournament_id": game.get("tournament", {}).get("id"),
                    }
                    produce(producer, TOPIC_GAME_START, gid, msg)
                    swept += 1
                except Exception:
                    pass
            time.sleep(1)  # space out bulk requests
        except Exception as e:
            logger.warning(f"Bulk sweep failed for {uid}: {e}")
    logger.info(f"Bulk sweep complete — {swept} games recovered")


def process_event(ev: dict, producer: Producer, boards: dict, prev_clocks: dict):
    etype = ev.get("type")

    if etype == "gameStart":
        g   = ev.get("game", {})
        gid = g.get("gameId") or g.get("id")
        if not gid:
            return
        boards[gid]      = chess.Board()
        prev_clocks[gid] = (None, None)
        wp  = g.get("white", {})
        bp  = g.get("black", {})
        msg = {
            "game_id"      : gid,
            "timestamp"    : datetime.now(timezone.utc).isoformat(),
            "speed"        : g.get("speed"),
            "rated"        : g.get("rated"),
            "variant"      : (g.get("variant", {}).get("key")
                              if isinstance(g.get("variant"), dict)
                              else g.get("variant")),
            "white_id"     : wp.get("id"),
            "white_rating" : wp.get("rating"),
            "white_title"  : wp.get("title"),
            "black_id"     : bp.get("id"),
            "black_rating" : bp.get("rating"),
            "black_title"  : bp.get("title"),
            "source"       : g.get("source"),
            "tournament_id": g.get("tournamentId"),
        }
        produce(producer, TOPIC_GAME_START, gid, msg)
        logger.info(f"[GAME START] {gid}  {wp.get('id')} vs {bp.get('id')}")

    elif etype == "gameState":
        gid        = ev.get("gameId")
        if not gid:
            return
        moves_list = ev.get("moves", "").split()
        wc         = ev.get("wc", ev.get("wtime", 0))
        bc         = ev.get("bc", ev.get("btime", 0))
        wc         = wc // 1000 if wc > 1000 else wc
        bc         = bc // 1000 if bc > 1000 else bc

        if gid not in boards:
            boards[gid]      = chess.Board()
            prev_clocks[gid] = (None, None)

        board = boards[gid]
        board.reset()
        for m in moves_list:
            try:
                board.push(chess.Move.from_uci(m))
            except Exception:
                pass

        ply         = len(moves_list)
        lm          = moves_list[-1] if moves_list else None
        if not lm:
            return

        phase       = "opening" if ply <= 20 else ("middlegame" if ply <= 60 else "endgame")
        whose_moved = "white" if ply % 2 == 1 else "black"
        prev_wc, prev_bc = prev_clocks.get(gid, (None, None))
        time_spent  = ((prev_wc - wc) if whose_moved == "white" and prev_wc is not None
                       else (prev_bc - bc) if whose_moved == "black" and prev_bc is not None
                       else None)
        prev_clocks[gid] = (wc, bc)

        msg = {
            "game_id"      : gid,
            "timestamp"    : datetime.now(timezone.utc).isoformat(),
            "move"         : lm,
            "fen"          : board.fen(),
            "white_clock"  : wc,
            "black_clock"  : bc,
            "move_number"  : board.fullmove_number,
            "game_phase"   : phase,
            "time_spent_s" : time_spent,
            "time_pressure": wc < 10 or bc < 10,
            "is_check"     : board.is_check(),
        }
        produce(producer, TOPIC_MOVES, gid, msg)
        logger.info(f"[MOVE] {gid}  #{board.fullmove_number}  {lm}  phase={phase}")

    elif etype == "gameFinish":
        g      = ev.get("game", {})
        gid    = g.get("gameId") or g.get("id")
        if not gid:
            return
        status = g.get("status", {})
        msg    = {
            "game_id"  : gid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "winner"   : g.get("winner"),
            "status"   : status.get("name") if isinstance(status, dict) else status,
        }
        produce(producer, TOPIC_GAME_END, gid, msg)
        logger.info(f"[GAME END] {gid}  winner={msg['winner']}  status={msg['status']}")
        boards.pop(gid, None)
        prev_clocks.pop(gid, None)


def stream_batch(producer: Producer, batch_id: int, players: list[str], disconnect_ts: int):
    """
    Long-lived stream for one batch of players.
    Runs in its own thread. Reconnects indefinitely until _shutdown is set.
    disconnect_ts: timestamp (ms) of when this batch last disconnected (for bulk sweep).
    """
    body            = ",".join(players)
    boards:      dict = {}
    prev_clocks: dict = {}
    backoff          = 30   # start conservatively after a potential IP rate-limit
    last_disconnect  = disconnect_ts
    # minimum seconds a stream must stay alive to count as "healthy"
    HEALTHY_THRESHOLD = 60

    logger.info(f"[Batch {batch_id}] Starting stream for {len(players)} players")

    while not _shutdown.is_set():
        try:
            # Sweep for missed games BEFORE opening the stream so we don't
            # block iter_lines() and cause Lichess to time out the connection
            if last_disconnect and (int(time.time() * 1000) - last_disconnect) > 30_000:
                bulk_sweep(producer, players, last_disconnect)

            response = requests.post(
                url="https://lichess.org/api/stream/games-by-users",
                headers=HDR_NDJSON,
                params={"withCurrentGames": "true"},
                data=body,
                stream=True,
                timeout=86400,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RATE_LIMIT_COOLDOWN))
                wait = max(retry_after, RATE_LIMIT_COOLDOWN)
                logger.warning(f"[Batch {batch_id}] Rate limited — Retry-After={retry_after}s, waiting {wait}s")
                time.sleep(wait)
                backoff = 30
                continue

            if response.status_code != 200:
                logger.error(f"[Batch {batch_id}] HTTP {response.status_code} — backoff {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue

            connect_time = time.time()
            logger.info(f"[Batch {batch_id}] Stream connected")

            for raw in response.iter_lines():
                if _shutdown.is_set():
                    return
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"[Batch {batch_id}] Non-JSON: {raw[:80]}")
                    continue
                process_event(ev, producer, boards, prev_clocks)

            # Stream closed by server
            uptime = time.time() - connect_time
            last_disconnect = int(time.time() * 1000)

            if uptime < HEALTHY_THRESHOLD:
                # Connection died too fast — likely soft rate-limit; back off longer
                backoff = min(backoff * 2, 120)
                logger.warning(
                    f"[Batch {batch_id}] Stream only lived {uptime:.0f}s "
                    f"(unhealthy) — backoff {backoff}s"
                )
            else:
                backoff = 30  # healthy session, reset to conservative default
                logger.info(f"[Batch {batch_id}] Stream closed after {uptime:.0f}s — reconnecting in {backoff}s")

            time.sleep(backoff)

        except Exception as e:
            logger.warning(f"[Batch {batch_id}] Stream error: {e} — backoff {backoff}s")
            last_disconnect = int(time.time() * 1000)
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)

    logger.info(f"[Batch {batch_id}] Thread exiting")


def secondary_stream_loop(producer: Producer, primary_ids: set):
    """
    Secondary stream thread: tracks dynamically discovered players.
    Reloads DISCOVERED_FILE every SECONDARY_REFRESH seconds.
    """
    logger.info("Secondary stream thread starting")
    backoff    = 30
    HEALTHY_THRESHOLD = 60

    while not _shutdown.is_set():
        discovered = load_discovered_players(primary_ids)
        if not discovered:
            logger.info("No discovered players yet — secondary stream idle, checking in 60s")
            time.sleep(60)
            continue

        logger.info(f"Secondary stream: {len(discovered)} discovered players")
        body         = ",".join(discovered)
        boards:      dict = {}
        prev_clocks: dict = {}

        try:
            response = requests.post(
                url="https://lichess.org/api/stream/games-by-users",
                headers=HDR_NDJSON,
                params={"withCurrentGames": "true"},
                data=body,
                stream=True,
                timeout=86400,
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RATE_LIMIT_COOLDOWN))
                wait = max(retry_after, RATE_LIMIT_COOLDOWN)
                logger.warning(f"Secondary stream rate limited — Retry-After={retry_after}s, waiting {wait}s")
                time.sleep(wait)
                backoff = 30
                continue

            if response.status_code != 200:
                logger.error(f"Secondary stream HTTP {response.status_code} — backoff {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue

            connect_time = time.time()
            logger.info("Secondary stream connected")

            for raw in response.iter_lines():
                if _shutdown.is_set():
                    return
                if time.time() - connect_time >= SECONDARY_REFRESH:
                    logger.info("Secondary stream: refresh interval — reconnecting with updated list")
                    break
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                process_event(ev, producer, boards, prev_clocks)

            uptime = time.time() - connect_time
            if uptime < HEALTHY_THRESHOLD:
                backoff = min(backoff * 2, 120)
                logger.warning(f"Secondary stream only lived {uptime:.0f}s (unhealthy) — backoff {backoff}s")
            else:
                backoff = 30
                logger.info(f"Secondary stream closed after {uptime:.0f}s — reconnecting in {backoff}s")
            time.sleep(backoff)

        except Exception as e:
            logger.warning(f"Secondary stream error: {e} — backoff {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)


def run():
    logger.info("stream_ingestor starting up — fetching seed players (once)")
    primary_ids = get_top_players()

    if not primary_ids:
        logger.error("Could not fetch any seed players — exiting")
        return

    producer = build_producer()

    # Split primary players into batches of 300
    batches = [primary_ids[i:i + MAX_BATCH] for i in range(0, len(primary_ids), MAX_BATCH)]
    logger.info(f"Primary players: {len(primary_ids)} across {len(batches)} stream(s)")

    threads = []

    # Start primary batch threads with staggered connects to avoid burst
    for idx, batch in enumerate(batches):
        if idx > 0:
            logger.info(f"Stagger: waiting {RECONNECT_STAGGER}s before batch {idx + 1}")
            time.sleep(RECONNECT_STAGGER)
        t = threading.Thread(
            target=stream_batch,
            args=(producer, idx + 1, batch, 0),
            daemon=True,
            name=f"primary-batch-{idx + 1}",
        )
        t.start()
        threads.append(t)

    # Start secondary stream thread for discovered players
    sec = threading.Thread(
        target=secondary_stream_loop,
        args=(producer, set(primary_ids)),
        daemon=True,
        name="secondary-stream",
    )
    sec.start()
    threads.append(sec)

    try:
        while not _shutdown.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        _shutdown.set()

    logger.info("Flushing producer...")
    producer.flush()
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    run()
