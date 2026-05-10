"""StarRocks connection + query helpers."""

import os
import logging
import threading
from contextlib import contextmanager
import mysql.connector
from mysql.connector import pooling

log = logging.getLogger("db")

TABLE = "polaris_catalog.prod.chess_move_events"
EVAL_TABLE = "polaris_catalog.prod.move_evaluations"


class StarRocks:
    _pool: pooling.MySQLConnectionPool | None = None
    _lock = threading.Lock()

    @classmethod
    def init(cls):
        with cls._lock:
            if cls._pool is not None:
                return
            cls._pool = pooling.MySQLConnectionPool(
                pool_name="sr",
                pool_size=4,
                host=os.getenv("STARROCKS_HOST", "starrocks-fe"),
                port=int(os.getenv("STARROCKS_PORT", "9030")),
                user=os.getenv("STARROCKS_USER", "root"),
                password=os.getenv("STARROCKS_PASSWORD", ""),
                connection_timeout=10,
            )
            log.info("starrocks pool initialized")

    @classmethod
    def close(cls):
        cls._pool = None

    @classmethod
    def healthy(cls) -> bool:
        try:
            with cls.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except Exception:
            return False

    @classmethod
    @contextmanager
    def cursor(cls):
        if cls._pool is None:
            cls.init()
        conn = cls._pool.get_connection()
        try:
            # Pooled connections survive FE restarts as stale sockets; ping with reconnect to refresh.
            try:
                conn.ping(reconnect=True, attempts=2, delay=0)
            except Exception:
                pass
            cur = conn.cursor(dictionary=True)
            try:
                yield cur
            finally:
                cur.close()
        finally:
            conn.close()


def _run(sql: str, params: tuple = ()) -> list[dict]:
    with StarRocks.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_game(game_id: str) -> list[dict]:
    return _run(
        f"""
        SELECT move_number, whose_moved, move, fen,
               ROUND(clock_remaining / 100.0, 1) AS clock_s,
               white_id, black_id, white_rating, black_rating,
               opening_eco, opening_name, speed, winner, end_status
        FROM {TABLE}
        WHERE game_id = %s
        ORDER BY move_number
        """,
        (game_id,),
    )


def query_player_profile(username: str) -> dict | None:
    overview = _run(
        f"""
        SELECT speed,
               COUNT(DISTINCT game_id) AS total_games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN winner IS NOT NULL AND winner != CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS losses,
               SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
               ROUND(AVG(CASE WHEN white_id=%s THEN white_rating ELSE black_rating END), 0) AS avg_rating
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY speed ORDER BY total_games DESC
        """,
        (username, username, username, username, username),
    )
    if not overview:
        return None

    color = _run(
        f"""
        SELECT CASE WHEN white_id=%s THEN 'White' ELSE 'Black' END AS color,
               COUNT(DISTINCT game_id) AS games,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY color
        """,
        (username, username, username, username),
    )
    openings = _run(
        f"""
        SELECT opening_eco, opening_name,
               COUNT(DISTINCT game_id) AS games,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1 AND opening_eco IS NOT NULL
        GROUP BY opening_eco, opening_name HAVING games >= 3
        ORDER BY games DESC LIMIT 10
        """,
        (username, username, username),
    )
    clock = _run(
        f"""
        SELECT CASE WHEN move_number<=10 THEN 'Opening' WHEN move_number<=30 THEN 'Middlegame' ELSE 'Endgame' END AS phase,
               ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND clock_remaining IS NOT NULL
        GROUP BY phase ORDER BY phase
        """,
        (username, username),
    )
    vs_rating = _run(
        f"""
        SELECT CASE
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) < (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) - 100 THEN 'Lower rated'
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) > (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) + 100 THEN 'Higher rated'
                 ELSE 'Equal rated' END AS opponent,
               COUNT(DISTINCT game_id) AS games,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY opponent
        """,
        (username, username, username, username, username, username, username),
    )
    recent = _run(
        f"""
        SELECT game_id,
               CASE WHEN white_id=%s THEN black_id ELSE white_id END AS opponent,
               CASE WHEN white_id=%s THEN white_rating ELSE black_rating END AS my_rating,
               CASE WHEN white_id=%s THEN black_rating ELSE white_rating END AS opp_rating,
               opening_eco, opening_name, speed,
               CASE WHEN winner IS NULL THEN 'Draw'
                    WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 'Win'
                    ELSE 'Loss' END AS result,
               date
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ORDER BY date DESC LIMIT 15
        """,
        (username, username, username, username, username, username),
    )

    total_games = sum(r["total_games"] for r in overview)
    total_wins = sum(r["wins"] for r in overview)
    total_losses = sum(r["losses"] for r in overview)
    total_draws = sum(r["draws"] for r in overview)
    avg_rating = round(sum(r["avg_rating"] * r["total_games"] for r in overview) / total_games)
    win_pct = round(total_wins * 100 / total_games, 1) if total_games else 0

    return {
        "username": username,
        "totals": {
            "games": total_games,
            "wins": total_wins,
            "losses": total_losses,
            "draws": total_draws,
            "win_pct": win_pct,
            "avg_rating": avg_rating,
        },
        "by_speed": overview,
        "by_color": color,
        "openings": openings,
        "clock_by_phase": clock,
        "vs_rating": vs_rating,
        "recent_games": recent,
    }


def query_exercise(username: str) -> dict | None:
    # Pick a random blunder/mistake the user committed and join the source position.
    rows = _run(
        f"""
        SELECT e.game_id, e.ply, e.fen, e.played_move, e.best_move,
               e.eval_cp, e.eval_swing_cp_from_prev, e.classification,
               m.clock_remaining, m.whose_moved, m.move_number,
               g.opening_name, g.opening_eco, g.speed
        FROM {EVAL_TABLE} e
        JOIN {TABLE} m
          ON e.game_id = m.game_id AND e.ply = m.move_number
        JOIN (
          SELECT DISTINCT game_id, opening_name, opening_eco, speed,
                 white_id, black_id
          FROM {TABLE} WHERE move_number = 1
        ) g ON g.game_id = e.game_id
        WHERE e.classification IN ('blunder', 'mistake')
          AND ((m.whose_moved='white' AND g.white_id=%s)
               OR (m.whose_moved='black' AND g.black_id=%s))
        ORDER BY RAND()
        LIMIT 1
        """,
        (username, username),
    )
    if not rows:
        return None
    r = rows[0]
    return {
        "game_id": r["game_id"],
        "ply": r["ply"],
        "fen_before": r["fen"],
        "played_move": r["played_move"],
        "best_move": r["best_move"],
        "eval_cp": r["eval_cp"],
        "eval_swing_cp": r["eval_swing_cp_from_prev"],
        "classification": r["classification"],
        "clock_remaining_s": round((r["clock_remaining"] or 0) / 100.0, 1),
        "side_to_move": r["whose_moved"],
        "move_number": r["move_number"],
        "opening_name": r["opening_name"],
        "opening_eco": r["opening_eco"],
        "speed": r["speed"],
    }
