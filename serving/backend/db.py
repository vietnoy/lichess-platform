"""StarRocks connection + query helpers."""

import os
import time
import logging
import threading
from contextlib import contextmanager
import mysql.connector
from mysql.connector import pooling

log = logging.getLogger("db")

TABLE = "polaris_catalog.prod.chess_move_events"
EVAL_TABLE = "polaris_catalog.prod.move_evaluations"


# Tiny TTL cache so the slow profile query doesn't hit StarRocks on every page load.
_PROFILE_TTL = 120
_profile_cache: dict[str, tuple[float, dict | None]] = {}
_profile_lock = threading.Lock()


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
    # Same upstream-duplicates issue: dedupe at (game_id, move_number) granularity.
    return _run(
        f"""
        SELECT move_number,
               MAX(whose_moved)        AS whose_moved,
               MAX(move)               AS move,
               MAX(fen)                AS fen,
               ROUND(MAX(clock_remaining) / 100.0, 1) AS clock_s,
               MAX(white_id)           AS white_id,
               MAX(black_id)           AS black_id,
               MAX(white_rating)       AS white_rating,
               MAX(black_rating)       AS black_rating,
               MAX(opening_eco)        AS opening_eco,
               MAX(opening_name)       AS opening_name,
               MAX(speed)              AS speed,
               MAX(winner)             AS winner,
               MAX(end_status)         AS end_status
        FROM {TABLE}
        WHERE game_id = %s
        GROUP BY move_number
        ORDER BY move_number
        """,
        (game_id,),
    )


def query_game_evaluations(game_id: str) -> list[dict]:
    """Return per-ply evaluation timeline for a game."""
    return _run(
        f"""
        SELECT ply,
               played_move,
               best_move,
               eval_cp,
               mate,
               eval_swing_cp_from_prev,
               classification
        FROM {EVAL_TABLE}
        WHERE game_id = %s
        ORDER BY ply
        """,
        (game_id,),
    )


def query_player_profile(username: str) -> dict | None:
    now = time.time()
    with _profile_lock:
        cached = _profile_cache.get(username)
        if cached and now - cached[0] < _PROFILE_TTL:
            return cached[1]
    profile = _query_player_profile_uncached(username)
    with _profile_lock:
        _profile_cache[username] = (now, profile)
    return profile


def _query_player_profile_uncached(username: str) -> dict | None:
    # chess_move_events has duplicate rows per (game_id, move_number) due to upstream retries;
    # GROUP BY game_id collapses each game to one record. The 60-day bound prunes Iceberg partitions
    # so we don't scan all historical move events on every page load (no index on white_id/black_id).
    games = _run(
        f"""
        SELECT
          game_id,
          MAX(white_id)      AS white_id,
          MAX(black_id)      AS black_id,
          MAX(white_rating)  AS white_rating,
          MAX(black_rating)  AS black_rating,
          MAX(speed)         AS speed,
          MAX(opening_eco)   AS opening_eco,
          MAX(opening_name)  AS opening_name,
          MAX(winner)        AS winner,
          MAX(end_status)    AS end_status,
          MAX(date)          AS date
        FROM {TABLE}
        WHERE move_number=1
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          AND (white_id=%s OR black_id=%s)
        GROUP BY game_id
        """,
        (username, username),
    )
    if not games:
        return None

    # Clock-by-phase: scope to this user's known game IDs to avoid a full-table scan.
    game_ids = [g["game_id"] for g in games]
    placeholders = ",".join(["%s"] * len(game_ids))
    clock_rows = _run(
        f"""
        SELECT phase, ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s
        FROM (
          SELECT
            CASE WHEN move_number<=10 THEN 'Opening'
                 WHEN move_number<=30 THEN 'Middlegame'
                 ELSE 'Endgame' END AS phase,
            MAX(clock_remaining) AS clock_remaining
          FROM {TABLE}
          WHERE game_id IN ({placeholders}) AND clock_remaining IS NOT NULL
          GROUP BY game_id, move_number
        ) t
        GROUP BY phase
        ORDER BY phase
        """,
        tuple(game_ids),
    )

    # ── aggregate in Python ──────────────────────────────────────────────────
    def my_color(g):     return "white" if g["white_id"] == username else "black"
    def my_color_cap(g): return my_color(g).capitalize()
    def my_rating(g):    return g["white_rating"] if my_color(g) == "white" else g["black_rating"]
    def opp_rating(g):   return g["black_rating"] if my_color(g) == "white" else g["white_rating"]
    def opponent(g):     return g["black_id"]    if my_color(g) == "white" else g["white_id"]
    def result(g):
        if g["winner"] is None: return "Draw"
        return "Win" if g["winner"] == my_color(g) else "Loss"

    by_speed: dict[str, dict] = {}
    for g in games:
        s = g["speed"]
        b = by_speed.setdefault(s, {"speed": s, "total_games": 0, "wins": 0, "losses": 0, "draws": 0, "rating_sum": 0})
        b["total_games"] += 1
        b["rating_sum"] += my_rating(g) or 0
        r = result(g)
        if r == "Win": b["wins"] += 1
        elif r == "Loss": b["losses"] += 1
        else: b["draws"] += 1
    overview = []
    for b in sorted(by_speed.values(), key=lambda x: -x["total_games"]):
        b["avg_rating"] = round(b["rating_sum"] / b["total_games"]) if b["total_games"] else 0
        b.pop("rating_sum", None)
        overview.append(b)

    by_color: dict[str, dict] = {"White": {"color": "White", "games": 0, "wins": 0}, "Black": {"color": "Black", "games": 0, "wins": 0}}
    for g in games:
        c = my_color_cap(g)
        by_color[c]["games"] += 1
        if result(g) == "Win": by_color[c]["wins"] += 1
    color = []
    for c in ("White", "Black"):
        e = by_color[c]
        if e["games"]:
            color.append({"color": c, "games": e["games"], "win_pct": round(e["wins"] * 100 / e["games"], 1)})

    open_acc: dict[tuple, dict] = {}
    for g in games:
        if not g["opening_eco"]: continue
        k = (g["opening_eco"], g["opening_name"])
        e = open_acc.setdefault(k, {"opening_eco": k[0], "opening_name": k[1], "games": 0, "wins": 0})
        e["games"] += 1
        if result(g) == "Win": e["wins"] += 1
    openings = sorted(
        ({**v, "win_pct": round(v["wins"] * 100 / v["games"], 1)} for v in open_acc.values() if v["games"] >= 3),
        key=lambda x: -x["games"],
    )[:10]

    vs_acc: dict[str, dict] = {}
    for g in games:
        my_r, opp_r = my_rating(g), opp_rating(g)
        if my_r is None or opp_r is None: continue
        if   opp_r < my_r - 100: bucket = "Lower rated"
        elif opp_r > my_r + 100: bucket = "Higher rated"
        else: bucket = "Equal rated"
        e = vs_acc.setdefault(bucket, {"opponent": bucket, "games": 0, "wins": 0})
        e["games"] += 1
        if result(g) == "Win": e["wins"] += 1
    vs_rating = [{**v, "win_pct": round(v["wins"] * 100 / v["games"], 1)} for v in vs_acc.values()]

    recent = sorted(games, key=lambda g: g["date"] or "", reverse=True)[:15]
    recent_out = [{
        "game_id":      g["game_id"],
        "opponent":     opponent(g),
        "my_rating":    my_rating(g),
        "opp_rating":   opp_rating(g),
        "opening_eco":  g["opening_eco"],
        "opening_name": g["opening_name"],
        "speed":        g["speed"],
        "result":       result(g),
        "date":         g["date"],
    } for g in recent]

    total_games  = len(games)
    total_wins   = sum(1 for g in games if result(g) == "Win")
    total_losses = sum(1 for g in games if result(g) == "Loss")
    total_draws  = total_games - total_wins - total_losses
    ratings      = [my_rating(g) for g in games if my_rating(g) is not None]
    avg_rating   = round(sum(ratings) / len(ratings)) if ratings else 0
    win_pct      = round(total_wins * 100 / total_games, 1) if total_games else 0

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
        "clock_by_phase": clock_rows,
        "vs_rating": vs_rating,
        "recent_games": recent_out,
    }


def query_player_patterns(username: str) -> dict | None:
    # Two-pass to avoid scanning the entire 17M-row chess_move_events:
    # 1) find this user's games in the last 60d (small set, indexed-ish via date partition)
    # 2) join evals + per-move metadata only for those game IDs
    games = _run(
        f"""
        SELECT DISTINCT game_id, opening_name, opening_eco, white_id, black_id, date
        FROM {TABLE}
        WHERE move_number = 1
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          AND (white_id=%s OR black_id=%s)
        """,
        (username, username),
    )
    if not games:
        return None

    game_ids = [g["game_id"] for g in games]
    placeholders = ",".join(["%s"] * len(game_ids))
    games_by_id = {g["game_id"]: g for g in games}

    rows = _run(
        f"""
        SELECT e.game_id, e.ply, e.classification, m.whose_moved, m.clock_remaining
        FROM {EVAL_TABLE} e
        JOIN (
          SELECT game_id, move_number,
                 MAX(whose_moved)     AS whose_moved,
                 MAX(clock_remaining) AS clock_remaining
          FROM {TABLE}
          WHERE game_id IN ({placeholders})
          GROUP BY game_id, move_number
        ) m
          ON e.game_id = m.game_id AND e.ply = m.move_number
        WHERE e.game_id IN ({placeholders})
        """,
        tuple(game_ids) + tuple(game_ids),
    )
    if not rows:
        return None

    # Stitch in opening + side info from the games map.
    for r in rows:
        g = games_by_id.get(r["game_id"], {})
        r["opening_eco"] = g.get("opening_eco")
        r["opening_name"] = g.get("opening_name")
        r["white_id"] = g.get("white_id")
        r["black_id"] = g.get("black_id")
        r["date"] = g.get("date")
        r["move_number"] = r["ply"]
        # Filter to the player's own moves only.
    rows = [
        r for r in rows
        if (r["whose_moved"] == "white" and r["white_id"] == username)
        or (r["whose_moved"] == "black" and r["black_id"] == username)
    ]
    if not rows:
        return None
    if not rows:
        return None

    totals = {"games_analyzed": 0, "blunders": 0, "mistakes": 0, "inaccuracies": 0}
    phase_acc = {
        "opening": {"phase": "opening", "blunders": 0, "mistakes": 0, "inaccuracies": 0},
        "middlegame": {"phase": "middlegame", "blunders": 0, "mistakes": 0, "inaccuracies": 0},
        "endgame": {"phase": "endgame", "blunders": 0, "mistakes": 0, "inaccuracies": 0},
    }
    move_bucket_acc = {
        "1-10": {"bucket": "1-10", "blunders": 0, "mistakes": 0},
        "11-20": {"bucket": "11-20", "blunders": 0, "mistakes": 0},
        "21-30": {"bucket": "21-30", "blunders": 0, "mistakes": 0},
        "31-40": {"bucket": "31-40", "blunders": 0, "mistakes": 0},
        "41-50": {"bucket": "41-50", "blunders": 0, "mistakes": 0},
        "51+": {"bucket": "51+", "blunders": 0, "mistakes": 0},
    }
    clock_acc = {
        "under_10s": {"pressure": "under_10s", "blunders": 0, "mistakes": 0},
        "under_30s": {"pressure": "under_30s", "blunders": 0, "mistakes": 0},
        "normal": {"pressure": "normal", "blunders": 0, "mistakes": 0},
    }
    class_key = {"blunder": "blunders", "mistake": "mistakes", "inaccuracy": "inaccuracies"}
    opening_acc: dict[tuple[str, str], dict] = {}
    worst_games_acc: dict[str, dict] = {}
    game_ids: set[str] = set()

    for r in rows:
        classification = r["classification"]
        game_id = r["game_id"]
        ply = int(r["ply"] or 0)
        move_number = int(r["move_number"] or ply)
        clock_remaining = r["clock_remaining"]

        game_ids.add(game_id)
        if classification == "blunder":
            totals["blunders"] += 1
        elif classification == "mistake":
            totals["mistakes"] += 1
        elif classification == "inaccuracy":
            totals["inaccuracies"] += 1
        else:
            classification = None

        if classification is not None:
            if ply <= 20:
                phase = "opening"
            elif ply <= 60:
                phase = "middlegame"
            else:
                phase = "endgame"
            phase_acc[phase][class_key[classification]] += 1

        if move_number <= 10:
            bucket = "1-10"
        elif move_number <= 20:
            bucket = "11-20"
        elif move_number <= 30:
            bucket = "21-30"
        elif move_number <= 40:
            bucket = "31-40"
        elif move_number <= 50:
            bucket = "41-50"
        else:
            bucket = "51+"
        if classification in ("blunder", "mistake"):
            move_bucket_acc[bucket][class_key[classification]] += 1

        if clock_remaining is not None and classification in ("blunder", "mistake"):
            if clock_remaining < 1000:
                pressure = "under_10s"
            elif clock_remaining < 3000:
                pressure = "under_30s"
            else:
                pressure = "normal"
            clock_acc[pressure][class_key[classification]] += 1

        opening_key = (r["opening_eco"] or "", r["opening_name"] or "")
        opening_entry = opening_acc.setdefault(
            opening_key,
            {
                "opening_eco": opening_key[0],
                "opening_name": opening_key[1],
                "blunders": 0,
                "mistakes": 0,
                "games": set(),
            },
        )
        opening_entry["games"].add(game_id)
        if classification == "blunder":
            opening_entry["blunders"] += 1
        elif classification == "mistake":
            opening_entry["mistakes"] += 1

        worst = worst_games_acc.setdefault(
            game_id,
            {
                "game_id": game_id,
                "blunders": 0,
                "mistakes": 0,
                "date": str(r["date"]) if r["date"] else None,
                "opponent": r["black_id"] if r["white_id"] == username else r["white_id"],
                "opening_name": r["opening_name"],
            },
        )
        if classification == "blunder":
            worst["blunders"] += 1
        elif classification == "mistake":
            worst["mistakes"] += 1
        worst["score"] = worst["blunders"] * 3 + worst["mistakes"]

    totals["games_analyzed"] = len(game_ids)

    by_opening = sorted(
        (
            {
                "opening_eco": v["opening_eco"],
                "opening_name": v["opening_name"],
                "blunders": v["blunders"],
                "mistakes": v["mistakes"],
                "games": len(v["games"]),
            }
            for v in opening_acc.values()
        ),
        key=lambda x: (-x["blunders"], -x["mistakes"], -x["games"], x["opening_name"] or ""),
    )[:5]

    worst_games = sorted(
        worst_games_acc.values(),
        key=lambda x: (x["score"], x["date"] or "", x["game_id"]),
        reverse=True,
    )[:5]
    for g in worst_games:
        g.pop("score", None)

    return {
        "username": username,
        "totals": totals,
        "by_phase": [phase_acc["opening"], phase_acc["middlegame"], phase_acc["endgame"]],
        "by_move_bucket": [
            move_bucket_acc["1-10"],
            move_bucket_acc["11-20"],
            move_bucket_acc["21-30"],
            move_bucket_acc["31-40"],
            move_bucket_acc["41-50"],
            move_bucket_acc["51+"],
        ],
        "by_clock": [clock_acc["under_10s"], clock_acc["under_30s"], clock_acc["normal"]],
        "by_opening": by_opening,
        "worst_games": worst_games,
    }


def query_exercise(username: str) -> dict | None:
    # Scope to 60 days so the chess_move_events scan prunes partitions; without this
    # the move_number=1 subquery touches the entire 17M-row table and times out at 300s.
    rows = _run(
        f"""
        SELECT e.game_id, e.ply, e.fen, e.played_move, e.best_move,
               e.eval_cp, e.eval_swing_cp_from_prev, e.classification,
               MAX(m.clock_remaining) AS clock_remaining,
               MAX(m.whose_moved) AS whose_moved,
               m.move_number,
               MAX(g.opening_name) AS opening_name,
               MAX(g.opening_eco) AS opening_eco,
               MAX(g.speed) AS speed
        FROM {EVAL_TABLE} e
        JOIN (
          SELECT game_id, opening_name, opening_eco, speed, white_id, black_id
          FROM {TABLE}
          WHERE move_number = 1
            AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
            AND (white_id = %s OR black_id = %s)
          GROUP BY game_id, opening_name, opening_eco, speed, white_id, black_id
        ) g ON g.game_id = e.game_id
        JOIN {TABLE} m
          ON e.game_id = m.game_id
         AND e.ply = m.move_number
         AND m.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        WHERE e.classification IN ('blunder', 'mistake')
          AND e.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
          AND ((m.whose_moved = 'white' AND g.white_id = %s)
               OR (m.whose_moved = 'black' AND g.black_id = %s))
        GROUP BY e.game_id, e.ply, e.fen, e.played_move, e.best_move,
                 e.eval_cp, e.eval_swing_cp_from_prev, e.classification, m.move_number
        ORDER BY RAND()
        LIMIT 1
        """,
        (username, username, username, username),
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
