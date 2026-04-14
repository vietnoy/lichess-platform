import random
import chess
import psycopg2
from datetime import datetime, timedelta

DB_DSN = "host=localhost port=5432 dbname=lichess user=lichess password=lichess"

PLAYERS = [
    ("hikaru",           "GM",   3102),
    ("magnuscarlsen",    "GM",   3089),
    ("drnykterstein",    "GM",   3076),
    ("firouzja2003",     "GM",   3087),
    ("lachessis",        "GM",   2950),
    ("penguingim1",      "GM",   3021),
    ("vincentkeymer",    "GM",   2980),
    ("nodirbek",         "GM",   2960),
    ("anishgiri",        "GM",   2940),
    ("fabianocaruana",   "GM",   3010),
    ("chessbrah",        "IM",   2780),
    ("gothamchess",      "IM",   2650),
    ("zhigalko_sergei",  "GM",   2890),
    ("blitzking88",      None,   1842),
    ("rapidmaster77",    None,   1756),
    ("tacticallion",     None,   1923),
    ("endgamewizard",    None,   1688),
    ("openingexpert",    None,   2105),
    ("bulletspammer",    None,   1540),
    ("chefshouse",       None,   1795),
]

OPENINGS = [
    ("B90", "Sicilian Defense: Najdorf Variation",       ["e2e4", "c7c5", "g1f3", "d7d6", "d2d4", "c5d4", "f3d4", "g8f6", "b1c3", "a7a6"]),
    ("C65", "Ruy Lopez: Berlin Defense",                 ["e2e4", "e7e5", "g1f3", "b8c6", "f1b5", "g8f6"]),
    ("D37", "Queen's Gambit Declined",                   ["d2d4", "d7d5", "c2c4", "e7e6", "b1c3", "g8f6", "c1g5", "f8e7"]),
    ("E97", "King's Indian Defense: Classical Variation",["d2d4", "g8f6", "c2c4", "g7g6", "b1c3", "f8g7", "e2e4", "d7d6", "g1f3", "e8g8"]),
    ("C54", "Italian Game: Classical Variation",         ["e2e4", "e7e5", "g1f3", "b8c6", "f1c4", "f8c5", "c2c3", "g8f6"]),
    ("A45", "Queen's Pawn Game",                         ["d2d4", "g8f6", "c1g5"]),
    ("B12", "Caro-Kann Defense",                         ["e2e4", "c7c6", "d2d4", "d7d5"]),
    ("C01", "French Defense: Exchange Variation",        ["e2e4", "e7e6", "d2d4", "d7d5", "e4d5", "e6d5"]),
    ("D80", "Grunfeld Defense",                          ["d2d4", "g8f6", "c2c4", "g7g6", "b1c3", "d7d5"]),
    ("E60", "King's Indian Defense",                     ["d2d4", "g8f6", "c2c4", "g7g6"]),
]

SPEEDS  = ["bullet", "bullet", "blitz", "blitz", "blitz", "rapid"]
SOURCES = ["pool", "pool", "pool", "friend", "tournament"]


def rand_id(n=8):
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=n))


def play_game(opening_moves, target_moves):
    """Play a chess game: opening moves first, then random legal moves."""
    board   = chess.Board()
    records = []

    for uci in opening_moves:
        move = chess.Move.from_uci(uci)
        if move in board.legal_moves:
            board.push(move)
            records.append((uci, board.fen(), board.is_check(), False))
        else:
            break

    while not board.is_game_over() and len(records) < target_moves:
        legal = list(board.legal_moves)
        if not legal:
            break
        move  = random.choice(legal)
        uci   = move.uci()
        is_capture = board.is_capture(move)
        board.push(move)
        records.append((uci, board.fen(), board.is_check(), is_capture))

    outcome = board.outcome()
    if outcome:
        if outcome.winner is True:
            winner = "white"
            status = "mate" if outcome.termination == chess.Termination.CHECKMATE else "resign"
        elif outcome.winner is False:
            winner = "black"
            status = "mate" if outcome.termination == chess.Termination.CHECKMATE else "resign"
        else:
            winner = None
            status = "draw"
    else:
        winner = random.choice(["white", "black", None])
        status = random.choice(["resign", "outoftime"]) if winner else "draw"

    return records, winner, status


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                player_id    VARCHAR PRIMARY KEY,
                title        VARCHAR,
                last_seen_at TIMESTAMP,
                created_at   TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id       VARCHAR PRIMARY KEY,
                white_id      VARCHAR REFERENCES players(player_id),
                black_id      VARCHAR REFERENCES players(player_id),
                white_rating  INTEGER,
                black_rating  INTEGER,
                white_title   VARCHAR,
                black_title   VARCHAR,
                speed         VARCHAR,
                rated         BOOLEAN,
                variant       VARCHAR,
                source        VARCHAR,
                tournament_id VARCHAR,
                opening_eco   VARCHAR,
                opening_name  VARCHAR,
                winner        VARCHAR,
                status        VARCHAR,
                started_at    TIMESTAMP,
                ended_at      TIMESTAMP,
                total_moves   INTEGER
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
    print("Tables ready.")


def seed_players(conn):
    with conn.cursor() as cur:
        for pid, title, _ in PLAYERS:
            cur.execute("""
                INSERT INTO players (player_id, title, last_seen_at, created_at)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (player_id) DO NOTHING
            """, (pid, title))
    conn.commit()
    print(f"Inserted {len(PLAYERS)} players.")


def seed_games(conn, num_games=200):
    player_list = [(p[0], p[1], p[2]) for p in PLAYERS]
    base_time   = datetime(2026, 4, 13, 8, 0, 0)

    games_inserted = 0
    moves_inserted = 0

    with conn.cursor() as cur:
        for i in range(num_games):
            white, black = random.sample(player_list, 2)
            white_id,  white_title,  white_base  = white
            black_id,  black_title,  black_base  = black

            speed       = random.choice(SPEEDS)
            eco, name, opening_moves = random.choice(OPENINGS)
            source      = random.choice(SOURCES)
            target      = random.randint(25, 80)
            started_at  = base_time + timedelta(minutes=i * 4, seconds=random.randint(0, 59))

            white_rating = max(600, min(3300, white_base + random.randint(-30, 30)))
            black_rating = max(600, min(3300, black_base + random.randint(-30, 30)))

            time_limit = {"bullet": 60, "blitz": 300, "rapid": 600}[speed]

            move_records, winner, status = play_game(opening_moves, target)
            total_moves = len(move_records)

            duration_s  = random.randint(total_moves, max(total_moves + 1, time_limit * 2))
            ended_at    = started_at + timedelta(seconds=duration_s)

            game_id = rand_id()

            cur.execute("""
                INSERT INTO games (
                    game_id, white_id, black_id, white_rating, black_rating,
                    white_title, black_title, speed, rated, variant, source,
                    tournament_id, opening_eco, opening_name,
                    winner, status, started_at, ended_at, total_moves
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (game_id) DO NOTHING
            """, (
                game_id, white_id, black_id, white_rating, black_rating,
                white_title, black_title, speed, True, "standard", source,
                None, eco, name,
                winner, status, started_at, ended_at, total_moves,
            ))
            games_inserted += 1

            wc = time_limit
            bc = time_limit
            move_ts = started_at

            for mn, (move_uci, fen, is_check, is_capture) in enumerate(move_records, start=1):
                if mn <= 20:
                    phase = "opening"
                elif mn <= 60:
                    phase = "middlegame"
                else:
                    phase = "endgame"

                if mn % 2 == 1:
                    player_id = white_id
                    think     = random.randint(1, max(1, wc // 8))
                    wc        = max(0, wc - think)
                else:
                    player_id = black_id
                    think     = random.randint(1, max(1, bc // 8))
                    bc        = max(0, bc - think)

                move_ts = move_ts + timedelta(seconds=think)

                cur.execute("""
                    INSERT INTO moves (
                        game_id, move_number, player_id, move_uci, fen_after,
                        white_clock, black_clock, time_spent_s, time_pressure,
                        game_phase, is_check, is_capture, played_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    game_id, mn, player_id, move_uci, fen,
                    wc, bc,
                    think if mn > 1 else None,
                    wc < 10 or bc < 10,
                    phase, is_check, is_capture,
                    move_ts,
                ))
                moves_inserted += 1

        conn.commit()

    print(f"Inserted {games_inserted} games and {moves_inserted} moves.")


if __name__ == "__main__":
    conn = psycopg2.connect(DB_DSN)
    init_db(conn)
    seed_players(conn)
    seed_games(conn, num_games=200)
    conn.close()
    print("Done.")
