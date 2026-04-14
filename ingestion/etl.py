import requests
import json
import threading
import time
import chess
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

# get environment variables
load_dotenv()
LICHESS_TOKEN      = os.getenv("LICHESS_TOKEN")
BOOTSTRAP_SERVER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

TOPIC_GAME_START = "lichess.game_start"
TOPIC_MOVES      = "lichess.moves"
TOPIC_GAME_END   = "lichess.game_end"

HDR_JSON   = {"Authorization": f"Bearer {LICHESS_TOKEN}", "Accept": "application/json"}
HDR_NDJSON = {"Authorization": f"Bearer {LICHESS_TOKEN}", "Accept": "application/x-ndjson"}

# setup basic logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


# setup kafka producer
def create_producer() -> Producer:
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "sasl.username":     CLUSTER_API_KEY,
        "sasl.password":     CLUSTER_API_SECRET,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "acks":              "all"
    }
    return Producer(config)


# send a message to kafka topic
def produce(producer: Producer, topic: str, key: str, value: dict):
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8")
    )
    producer.poll(0)


# get top players from lichess
def get_top_players() -> set:
    game_styles = ["bullet", "blitz", "rapid"]
    player_ids  = set()

    for style in game_styles:
        try:
            response = requests.get(
                url=f"https://lichess.org/api/player/top/50/{style}",
                headers=HDR_JSON,
                timeout=10
            )
            data  = response.json()
            users = data["users"]
            for user in users:
                player_ids.add(user["id"])
        except Exception as e:
            logger.error(f"Error retrieving top players: {e}")

    logger.info(f"Collected {len(player_ids)} unique players")
    return player_ids


# check which players are online and who is mid-game
def get_active_players(player_ids: set) -> tuple:
    player_ids = list(player_ids)
    online     = []
    mid_game   = {}

    for i in range(0, len(player_ids), 100):
        batch = player_ids[i:i+100]
        try:
            response = requests.get(
                url="https://lichess.org/api/users/status",
                headers=HDR_JSON,
                params={"ids": ",".join(batch), "withGameIds": "true"},
                timeout=10
            )
            data = response.json()
            for user in data:
                if user.get("online"):
                    online.append(user["id"])
                if user.get("playing") and user.get("playingId"):
                    mid_game[user["id"]] = user["playingId"]
        except Exception as e:
            logger.error(f"Error retrieving active players: {e}")

    logger.info(f"Online: {len(online)}  |  Mid-game: {len(mid_game)}")
    return online, mid_game


# stream a single game that is already in progress and send events to kafka
def stream_active_game(producer: Producer, game_id: str):
    url   = f"https://lichess.org/api/stream/game/{game_id}"
    board = chess.Board()
    prev_wc = prev_bc = None

    try:
        response = requests.get(url, headers=HDR_NDJSON, stream=True, timeout=60)

        for raw in response.iter_lines():
            if not raw:
                continue

            logger.debug(f"[RAW stream_active_game] {raw[:200]}")
            ev = json.loads(raw)

            # game metadata — first event when we connect
            if "players" in ev:
                if "winner" in ev or ev.get("status"):
                    msg = {
                        "game_id"  : game_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "winner"   : ev.get("winner"),
                        "status"   : ev.get("status"),
                    }
                    produce(producer, TOPIC_GAME_END, game_id, msg)
                    logger.info(f"[GAME END] {game_id}  winner={msg['winner']}  status={msg['status']}")
                    break
                wp  = ev["players"].get("white", {})
                bp  = ev["players"].get("black", {})
                msg = {
                    "game_id"      : game_id,
                    "timestamp"    : datetime.now(timezone.utc).isoformat(),
                    "speed"        : ev.get("speed"),
                    "rated"        : ev.get("rated"),
                    "variant"      : ev.get("variant", {}).get("key"),
                    "white_id"     : wp.get("user", {}).get("id"),
                    "white_rating" : wp.get("rating"),
                    "white_title"  : wp.get("user", {}).get("title"),
                    "black_id"     : bp.get("user", {}).get("id"),
                    "black_rating" : bp.get("rating"),
                    "black_title"  : bp.get("user", {}).get("title"),
                    "source"       : ev.get("source"),
                    "tournament_id": ev.get("tournamentId"),
                }
                produce(producer, TOPIC_GAME_START, game_id, msg)
                logger.info(f"[GAME START] {game_id}")

            # move event
            elif "lm" in ev:
                lm = ev["lm"]
                wc = ev.get("wc", 0)
                bc = ev.get("bc", 0)

                try:
                    board.push(chess.Move.from_uci(lm))
                    ply         = len(board.move_stack)
                    phase       = "opening" if ply <= 20 else ("middlegame" if ply <= 60 else "endgame")
                    whose_moved = "white" if not board.turn else "black"
                    time_spent  = (prev_wc - wc) if whose_moved == "white" and prev_wc else \
                                  (prev_bc - bc) if whose_moved == "black" and prev_bc else None
                except Exception:
                    ply = phase = time_spent = None

                msg = {
                    "game_id"      : game_id,
                    "timestamp"    : datetime.now(timezone.utc).isoformat(),
                    "move"         : lm,
                    "fen"          : ev.get("fen"),
                    "white_clock"  : wc,
                    "black_clock"  : bc,
                    "move_number"  : board.fullmove_number,
                    "game_phase"   : phase,
                    "time_spent_s" : time_spent,
                    "time_pressure": wc < 10 or bc < 10,
                    "is_check"     : board.is_check(),
                }
                produce(producer, TOPIC_MOVES, game_id, msg)
                logger.info(f"[MOVE] {game_id}  #{board.fullmove_number}  {lm}  phase={phase}")
                prev_wc, prev_bc = wc, bc

            # game over
            elif "winner" in ev or "status" in ev:
                msg = {
                    "game_id"  : game_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "winner"   : ev.get("winner"),
                    "status"   : ev.get("status"),
                }
                produce(producer, TOPIC_GAME_END, game_id, msg)
                logger.info(f"[GAME END] {game_id}  winner={msg['winner']}  status={msg['status']}")
                break

    except Exception as e:
        logger.error(f"Error streaming game {game_id}: {e}")


# stream games-by-users — catches any new game started by tracked players
def stream_games_by_users(producer: Producer, player_ids: list):
    # split into batches of 300 (lichess limit per connection)
    for i in range(0, len(player_ids), 300):
        batch = player_ids[i:i+300]
        t = threading.Thread(
            target=_stream_batch,
            args=(producer, batch),
            daemon=True
        )
        t.start()
        logger.info(f"Opened stream for batch {i//300 + 1} ({len(batch)} players)")


def _stream_batch(producer: Producer, player_ids: list):
    body = "\n".join(player_ids)
    boards      = {}
    prev_clocks = {}

    # setup the number of retry time for opening a connection
    max_retries = 5
    retry_attempt = 1

    while retry_attempt < max_retries:
        try:
            response = requests.post(
                url="https://lichess.org/api/stream/games-by-users",
                headers={**HDR_NDJSON, "Content-Type": "text/plain"},
                params={"withCurrentGames": "true"},
                data=body,
                stream=True,
                timeout=86400  # keep alive for 24 hours
            )

            for raw in response.iter_lines():
                if not raw:
                    continue

                logger.debug(f"[RAW _stream_batch] {raw[:200]}")
                ev    = json.loads(raw)
                etype = ev.get("type")

                # new game started
                if etype == "gameStart":
                    g   = ev.get("game", {})
                    gid = g.get("gameId") or g.get("id")
                    if not gid:
                        continue

                    boards[gid]      = chess.Board()
                    prev_clocks[gid] = (None, None)

                    wp  = g.get("white", {})
                    bp  = g.get("black", {})
                    msg = {
                        "game_id"      : gid,
                        "timestamp"    : datetime.now(timezone.utc).isoformat(),
                        "speed"        : g.get("speed"),
                        "rated"        : g.get("rated"),
                        "variant"      : g.get("variant", {}).get("key") if isinstance(g.get("variant"), dict) else g.get("variant"),
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
                    logger.info(f"[GAME START] {gid}")

                # move made
                elif etype == "gameState":
                    gid        = ev.get("gameId")
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
                    phase       = "opening" if ply <= 20 else ("middlegame" if ply <= 60 else "endgame")
                    whose_moved = "white" if ply % 2 == 1 else "black"
                    prev_wc, prev_bc = prev_clocks.get(gid, (None, None))
                    time_spent  = (prev_wc - wc) if whose_moved == "white" and prev_wc else \
                                (prev_bc - bc) if whose_moved == "black" and prev_bc else None
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

                # game finished
                elif etype == "gameFinish":
                    g      = ev.get("game", {})
                    gid    = g.get("gameId") or g.get("id")
                    status = g.get("status", {})
                    msg    = {
                        "game_id"  : gid,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "winner"   : g.get("winner"),
                        "status"   : status.get("name") if isinstance(status, dict) else status,
                    }
                    produce(producer, TOPIC_GAME_END, gid, msg)
                    logger.info(f"[GAME END] {gid}  winner={msg['winner']}  status={msg['status']}")

        except Exception as e:
            wait = min(5 * (2 ** retry_attempt), 60)
            logger.warning(f"Stream dropped, reconnecting in {wait}s: {e} ({retry_attempt}/{max_retries})")
            time.sleep(wait)
            retry_attempt += 1


if __name__ == "__main__":
    # step 1: get players
    player_ids = get_top_players()

    # step 2: check who is online and mid-game
    online, mid_game = get_active_players(player_ids)

    # step 3: connect to kafka
    logger.info("Connecting to Kafka...")
    producer = create_producer()
    logger.info("Connected.")

    # step 4: stream games already in progress
    unique_games = list(set(mid_game.values()))
    logger.info(f"Streaming {len(unique_games)} active games...")
    for game_id in unique_games:
        t = threading.Thread(target=stream_active_game, args=(producer, game_id), daemon=True)
        t.start()

    # step 5: open games-by-users stream to catch new games
    logger.info(f"Opening games-by-users stream for {len(player_ids)} players...")
    stream_games_by_users(producer, list(player_ids))

    # keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down.")
        producer.flush()
