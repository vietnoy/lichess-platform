import json
import logging
import os
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")

TOPIC_GAME_START = "lichess.game_start"
TOPIC_MOVES      = "lichess.moves"
TOPIC_GAME_END   = "lichess.game_end"

DB_DSN = "host=localhost port=5432 dbname=lichess user=lichess password=lichess"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":  BOOTSTRAP_SERVER,
        "sasl.username":      CLUSTER_API_KEY,
        "sasl.password":      CLUSTER_API_SECRET,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "group.id":           "lichess-consumer-v1",
        "auto.offset.reset":  "earliest",
    })


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id   VARCHAR PRIMARY KEY,
                title       VARCHAR,
                last_seen_at TIMESTAMP,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id        VARCHAR PRIMARY KEY,
                white_id       VARCHAR REFERENCES players(player_id),
                black_id       VARCHAR REFERENCES players(player_id),
                white_rating   INTEGER,
                black_rating   INTEGER,
                white_title    VARCHAR,
                black_title    VARCHAR,
                speed          VARCHAR,
                rated          BOOLEAN,
                variant        VARCHAR,
                source         VARCHAR,
                tournament_id  VARCHAR,
                opening_eco    VARCHAR,
                opening_name   VARCHAR,
                winner         VARCHAR,
                status         VARCHAR,
                started_at     TIMESTAMP,
                ended_at       TIMESTAMP,
                total_moves    INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS moves (
                id            SERIAL PRIMARY KEY,
                game_id       VARCHAR REFERENCES games(game_id),
                move_number   INTEGER,
                player_id     VARCHAR REFERENCES players(player_id),
                move_uci      VARCHAR,
                fen_after     TEXT,
                white_clock   INTEGER,
                black_clock   INTEGER,
                time_spent_s  INTEGER,
                time_pressure BOOLEAN,
                game_phase    VARCHAR,
                is_check      BOOLEAN,
                is_capture    BOOLEAN,
                played_at     TIMESTAMP
            )
        """)
    conn.commit()
    logger.info("Tables ready.")


def upsert_player(cur, player_id, title, timestamp):
    if not player_id:
        return
    cur.execute("""
        INSERT INTO players (player_id, title, last_seen_at, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (player_id) DO UPDATE
            SET title        = EXCLUDED.title,
                last_seen_at = EXCLUDED.last_seen_at
    """, (player_id, title, timestamp))


def handle_game_start(cur, ev):
    upsert_player(cur, ev.get("white_id"), ev.get("white_title"), ev.get("timestamp"))
    upsert_player(cur, ev.get("black_id"), ev.get("black_title"), ev.get("timestamp"))
    cur.execute("""
        INSERT INTO games (
            game_id, white_id, black_id, white_rating, black_rating,
            white_title, black_title, speed, rated, variant, source,
            tournament_id, started_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (game_id) DO NOTHING
    """, (
        ev.get("game_id"),
        ev.get("white_id"),
        ev.get("black_id"),
        ev.get("white_rating"),
        ev.get("black_rating"),
        ev.get("white_title"),
        ev.get("black_title"),
        ev.get("speed"),
        ev.get("rated"),
        ev.get("variant"),
        ev.get("source"),
        ev.get("tournament_id"),
        ev.get("timestamp"),
    ))
    logger.info(f"[GAME START] {ev.get('game_id')}")


def handle_move(cur, ev):
    move_number = ev.get("move_number", 0)
    if move_number % 2 == 1:
        player_id = ev.get("white_id")
    else:
        player_id = ev.get("black_id")

    cur.execute("""
        INSERT INTO moves (
            game_id, move_number, player_id, move_uci, fen_after,
            white_clock, black_clock, time_spent_s, time_pressure,
            game_phase, is_check, is_capture, played_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        ev.get("game_id"),
        move_number,
        player_id,
        ev.get("move"),
        ev.get("fen"),
        ev.get("white_clock"),
        ev.get("black_clock"),
        ev.get("time_spent_s"),
        ev.get("time_pressure"),
        ev.get("game_phase"),
        ev.get("is_check"),
        ev.get("is_capture"),
        ev.get("timestamp"),
    ))


def handle_game_end(cur, ev):
    status = ev.get("status")
    if isinstance(status, dict):
        status = status.get("name")
    cur.execute("""
        UPDATE games
        SET winner    = %s,
            status    = %s,
            ended_at  = %s
        WHERE game_id = %s
    """, (
        ev.get("winner"),
        status,
        ev.get("timestamp"),
        ev.get("game_id"),
    ))
    logger.info(f"[GAME END]   {ev.get('game_id')}  winner={ev.get('winner')}  status={status}")


def main():
    conn     = psycopg2.connect(DB_DSN)
    consumer = create_consumer()
    consumer.subscribe([TOPIC_GAME_START, TOPIC_MOVES, TOPIC_GAME_END])
    init_db(conn)

    logger.info("Consumer started. Waiting for messages...")

    move_batch = []
    BATCH_SIZE = 50

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            ev    = json.loads(msg.value().decode("utf-8"))

            with conn.cursor() as cur:
                if topic == TOPIC_GAME_START:
                    handle_game_start(cur, ev)
                    conn.commit()

                elif topic == TOPIC_MOVES:
                    move_batch.append(ev)
                    if len(move_batch) >= BATCH_SIZE:
                        for m in move_batch:
                            handle_move(cur, m)
                        conn.commit()
                        logger.info(f"Flushed {len(move_batch)} moves")
                        move_batch = []

                elif topic == TOPIC_GAME_END:
                    if move_batch:
                        for m in move_batch:
                            handle_move(cur, m)
                        move_batch = []
                    handle_game_end(cur, ev)
                    conn.commit()

    except KeyboardInterrupt:
        if move_batch:
            with conn.cursor() as cur:
                for m in move_batch:
                    handle_move(cur, m)
            conn.commit()
        logger.info("Stopped.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
