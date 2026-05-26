"""StarRocks connection + query helpers."""

import os
import random
import time
import logging
import threading
from contextlib import contextmanager
import mysql.connector
from mysql.connector import pooling

log = logging.getLogger("db")

TABLE = "polaris_catalog.prod.chess_move_events"
PLAYER_GAMES = "polaris_catalog.prod.player_games"
EVAL_TABLE = "polaris_catalog.prod.move_evaluations"
EVAL_TABLE_ONDEMAND = "polaris_catalog.prod.move_evaluations_ondemand"
CRITICAL_POSITIONS = "polaris_catalog.prod.critical_positions"
PLAYER_WEAKNESS_SUMMARY = "polaris_catalog.prod.player_weakness_summary"
PLAYER_OPENING_STATS = "polaris_catalog.prod.player_opening_stats"
PLAYER_PHASE_STATS = "polaris_catalog.prod.player_phase_stats"

PROD_TABLES = [
    ("chess_move_events", TABLE, "Raw move-level fact table"),
    ("player_games", PLAYER_GAMES, "One row per player per game"),
    ("move_evaluations_ondemand", EVAL_TABLE_ONDEMAND, "Stockfish evaluations from analyzer"),
    ("critical_positions", CRITICAL_POSITIONS, "Teachable mistakes and swings"),
    ("player_weakness_summary", PLAYER_WEAKNESS_SUMMARY, "Daily player weakness aggregate"),
    ("player_opening_stats", PLAYER_OPENING_STATS, "Daily player opening aggregate"),
    ("player_phase_stats", PLAYER_PHASE_STATS, "Daily player phase aggregate"),
]


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


def query_system_summary() -> dict:
    tables = []
    total_rows = 0
    for name, full_name, description in PROD_TABLES:
        latest_rows = _run(
            f"""
            SELECT MAX(date) AS latest_date
            FROM {full_name}
            """
        )
        latest_row = latest_rows[0] if latest_rows else {}
        latest_date = latest_row.get("latest_date")
        if hasattr(latest_date, "isoformat"):
            latest_date = latest_date.isoformat()
        row_count = 0
        if latest_date:
            count_rows = _run(
                f"""
                SELECT COUNT(*) AS row_count
                FROM {full_name}
                WHERE date = %s
                """,
                (str(latest_date),),
            )
            row_count = int((count_rows[0] if count_rows else {}).get("row_count") or 0)
        tables.append(
            {
                "name": name,
                "full_name": full_name,
                "description": description,
                "latest_partition_rows": row_count,
                "latest_date": str(latest_date) if latest_date else None,
            }
        )
        total_rows += row_count
    latest_dates = [t["latest_date"] for t in tables if t["latest_date"]]
    return {
        "tables": tables,
        "totals": {
            "latest_partition_rows": total_rows,
            "tables": len(tables),
            "latest_date": max(latest_dates) if latest_dates else None,
        },
    }


def _to_iso_date(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def query_platform_overview() -> dict:
    latest = _run(
        f"""
        SELECT MAX(date) AS date
        FROM {PLAYER_GAMES}
        """
    )
    date = _to_iso_date((latest[0] if latest else {}).get("date"))
    if not date:
        return {
            "date": None,
            "totals": {"games": 0, "player_game_rows": 0, "players": 0},
            "speed_mix": [],
            "top_openings": [],
            "phase_mistakes": [],
        }

    totals = _run(
        f"""
        SELECT
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS player_game_rows,
            COUNT(DISTINCT player_id) AS players
        FROM {PLAYER_GAMES}
        WHERE date = %s
        """,
        (date,),
    )
    speed_mix = _run(
        f"""
        SELECT
            COALESCE(speed, 'unknown') AS speed,
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS player_game_rows,
            ROUND(AVG(my_rating), 0) AS avg_rating
        FROM {PLAYER_GAMES}
        WHERE date = %s
        GROUP BY speed
        ORDER BY games DESC
        LIMIT 8
        """,
        (date,),
    )
    top_openings = _run(
        f"""
        SELECT
            opening_eco,
            opening_name,
            SUM(games) AS games,
            ROUND(SUM(wins) * 100.0 / NULLIF(SUM(games), 0), 1) AS win_rate_pct,
            SUM(critical_positions) AS critical_positions
        FROM {PLAYER_OPENING_STATS}
        WHERE date = %s
        GROUP BY opening_eco, opening_name
        HAVING SUM(games) >= 20
        ORDER BY games DESC
        LIMIT 10
        """,
        (date,),
    )
    phase_mistakes = _run(
        f"""
        SELECT
            phase,
            SUM(critical_positions) AS critical_positions,
            SUM(blunders) AS blunders,
            SUM(mistakes) AS mistakes,
            SUM(inaccuracies) AS inaccuracies
        FROM {PLAYER_PHASE_STATS}
        WHERE date = %s
        GROUP BY phase
        ORDER BY critical_positions DESC
        """,
        (date,),
    )

    return {
        "date": date,
        "totals": totals[0] if totals else {"games": 0, "player_game_rows": 0, "players": 0},
        "speed_mix": speed_mix,
        "top_openings": top_openings,
        "phase_mistakes": phase_mistakes,
    }


def query_game(game_id: str) -> list[dict]:
    # chess_move_events.date is the partition column; without a `date =` predicate
    # this scans every partition. Look it up via player_games (one row per game) first.
    date_rows = _run(
        f"""
        SELECT MIN(date) AS date
        FROM {PLAYER_GAMES}
        WHERE game_id = %s
        """,
        (game_id,),
    )
    if not date_rows or date_rows[0]["date"] is None:
        return []
    game_date = date_rows[0]["date"]
    if hasattr(game_date, "isoformat"):
        game_date = game_date.isoformat()

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
          AND date = %s
        GROUP BY move_number
        ORDER BY move_number
        """,
        (game_id, game_date),
    )


def query_game_evaluations(game_id: str) -> list[dict]:
    """Return per-ply evaluation timeline for a game."""
    date_rows = _run(
        f"""
        SELECT MIN(date) AS date
        FROM {PLAYER_GAMES}
        WHERE game_id = %s
        """,
        (game_id,),
    )
    if not date_rows or date_rows[0]["date"] is None:
        return []
    game_date = date_rows[0]["date"]
    if hasattr(game_date, "isoformat"):
        game_date = game_date.isoformat()

    return _run(
        f"""
        SELECT ply, played_move, best_move, eval_cp, mate, eval_swing_cp_from_prev, classification
        FROM (
          SELECT ply, played_move, best_move, eval_cp, mate, eval_swing_cp_from_prev, classification,
                 ROW_NUMBER() OVER (PARTITION BY ply ORDER BY source_priority) AS rn
          FROM (
            SELECT ply, played_move, best_move, eval_cp, mate, eval_swing_cp AS eval_swing_cp_from_prev,
                   classification, 1 AS source_priority
            FROM {EVAL_TABLE_ONDEMAND}
            WHERE game_id = %s
              AND date = DATE %s
            UNION ALL
            SELECT ply, played_move, best_move, eval_cp, mate, eval_swing_cp_from_prev,
                   classification, 2 AS source_priority
            FROM {EVAL_TABLE}
            WHERE game_id = %s
              AND date = DATE %s
          ) src
        ) ranked
        WHERE rn = 1
        ORDER BY ply
        """,
        (game_id, game_date, game_id, game_date),
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
    # player_games is the denormalized projection (one row per game-side) built by
    # processing/build_player_games.py. Sorted by player_id within each partition so
    # parquet min/max stats let StarRocks skip whole files for WHERE player_id=?.
    games = _run(
        f"""
        SELECT game_id, color, opponent_id, my_rating, opp_rating,
               speed, opening_eco, opening_name, winner, end_status, date
        FROM {PLAYER_GAMES}
        WHERE player_id=%s
        """,
        (username,),
    )
    if not games:
        return None

    # Keep profile fast: detailed phase weakness now comes from the aggregate
    # player_phase_stats endpoint instead of scanning raw move events here.
    clock_rows = []

    def result(g):
        if g["winner"] is None: return "Draw"
        return "Win" if g["winner"] == g["color"] else "Loss"

    by_speed: dict[str, dict] = {}
    for g in games:
        s = g["speed"]
        b = by_speed.setdefault(s, {"speed": s, "total_games": 0, "wins": 0, "losses": 0, "draws": 0, "rating_sum": 0})
        b["total_games"] += 1
        b["rating_sum"] += g["my_rating"] or 0
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
        c = g["color"].capitalize()
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
        my_r, opp_r = g["my_rating"], g["opp_rating"]
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
        "opponent":     g["opponent_id"],
        "my_rating":    g["my_rating"],
        "opp_rating":   g["opp_rating"],
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
    ratings      = [g["my_rating"] for g in games if g["my_rating"] is not None]
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
    rows = _run(
        f"""
        SELECT
            game_id,
            ply,
            classification,
            clock_remaining,
            opening_eco,
            opening_name,
            opponent_id,
            date
        FROM {CRITICAL_POSITIONS}
        WHERE player_id = %s
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        """,
        (username,),
    )
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
        move_number = ply
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
                "opponent": r["opponent_id"],
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


def clamp_int(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, int(value)))


def query_weakness_summary(username: str, days: int = 60) -> dict:
    days = clamp_int(days, 1, 365)
    rows = _run(
        f"""
        SELECT
            player_id,
            COUNT(*) AS days,
            SUM(critical_positions) AS critical_positions,
            SUM(games_with_critical_positions) AS games_with_critical_positions,
            SUM(blunders) AS blunders,
            SUM(mistakes) AS mistakes,
            SUM(inaccuracies) AS inaccuracies,
            ROUND(AVG(avg_eval_swing_cp), 1) AS avg_eval_swing_cp,
            SUM(time_pressure_positions) AS time_pressure_positions,
            MAX(top_phase) AS top_phase,
            MAX(top_time_pressure) AS top_time_pressure,
            MAX(top_classification) AS top_classification
        FROM {PLAYER_WEAKNESS_SUMMARY}
        WHERE player_id = %s
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)
        GROUP BY player_id
        """,
        (username, days),
    )
    if rows:
        return rows[0]
    return {
        "player_id": username,
        "days": days,
        "critical_positions": 0,
        "games_with_critical_positions": 0,
        "blunders": 0,
        "mistakes": 0,
        "inaccuracies": 0,
        "avg_eval_swing_cp": None,
        "time_pressure_positions": 0,
        "top_phase": None,
        "top_time_pressure": None,
        "top_classification": None,
    }


def query_blunder_examples(
    username: str,
    limit: int = 5,
    phase: str | None = None,
    time_pressure: str | None = None,
) -> list[dict]:
    valid_phases = {"opening", "middlegame", "endgame"}
    valid_pressures = {"unknown", "under_10s", "under_30s", "normal"}
    if phase is not None and phase not in valid_phases:
        return []
    if time_pressure is not None and time_pressure not in valid_pressures:
        return []

    filters = ["player_id = %s", "classification IN ('blunder', 'mistake')"]
    params: list = [username]
    if phase is not None:
        filters.append("phase = %s")
        params.append(phase)
    if time_pressure is not None:
        filters.append("time_pressure = %s")
        params.append(time_pressure)
    params.append(clamp_int(limit, 1, 20))

    return _run(
        f"""
        SELECT
            game_id,
            ply,
            date,
            fen,
            played_move,
            best_move,
            eval_cp,
            mate,
            eval_swing_cp,
            classification,
            phase,
            time_pressure,
            clock_remaining,
            color,
            opponent_id,
            opening_eco,
            opening_name,
            speed,
            perf
        FROM {CRITICAL_POSITIONS}
        WHERE {" AND ".join(filters)}
        ORDER BY date DESC, ABS(eval_swing_cp) DESC
        LIMIT %s
        """,
        tuple(params),
    )


def query_opening_stats(username: str, days: int = 60, top_n: int = 10) -> list[dict]:
    days = clamp_int(days, 1, 365)
    top_n = clamp_int(top_n, 1, 20)
    return _run(
        f"""
        SELECT
            opening_eco,
            opening_name,
            color,
            SUM(games) AS games,
            SUM(wins) AS wins,
            SUM(losses) AS losses,
            SUM(draws) AS draws,
            ROUND(SUM(wins) * 100.0 / NULLIF(SUM(games), 0), 1) AS win_rate_pct,
            SUM(critical_positions) AS critical_positions,
            SUM(blunders) AS blunders,
            SUM(mistakes) AS mistakes,
            SUM(inaccuracies) AS inaccuracies,
            ROUND(AVG(avg_eval_swing_cp), 1) AS avg_eval_swing_cp
        FROM {PLAYER_OPENING_STATS}
        WHERE player_id = %s
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)
        GROUP BY opening_eco, opening_name, color
        HAVING SUM(games) >= 2
        ORDER BY blunders DESC, mistakes DESC, critical_positions DESC, games DESC
        LIMIT %s
        """,
        (username, days, top_n),
    )


def query_phase_stats(username: str, days: int = 60) -> list[dict]:
    days = clamp_int(days, 1, 365)
    return _run(
        f"""
        SELECT
            phase,
            SUM(games_with_positions) AS games_with_positions,
            SUM(critical_positions) AS critical_positions,
            SUM(blunders) AS blunders,
            SUM(mistakes) AS mistakes,
            SUM(inaccuracies) AS inaccuracies,
            SUM(time_pressure_positions) AS time_pressure_positions,
            ROUND(AVG(avg_eval_swing_cp), 1) AS avg_eval_swing_cp,
            MAX(max_eval_swing_cp) AS max_eval_swing_cp
        FROM {PLAYER_PHASE_STATS}
        WHERE player_id = %s
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)
        GROUP BY phase
        ORDER BY critical_positions DESC, blunders DESC
        """,
        (username, days),
    )


def query_exercise(username: str) -> dict | None:
    rows = _run(
        f"""
        SELECT
            game_id,
            ply,
            fen,
            played_move,
            best_move,
            eval_cp,
            eval_swing_cp,
            classification,
            date,
            opening_name,
            opening_eco,
            speed,
            clock_remaining
        FROM {CRITICAL_POSITIONS}
        WHERE player_id = %s
          AND classification IN ('blunder', 'mistake')
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        ORDER BY date DESC, ABS(eval_swing_cp) DESC
        LIMIT 500
        """,
        (username,),
    )
    candidates = []
    for r in rows:
        parts = (r.get("fen") or "").split()
        active = parts[1] if len(parts) >= 2 else ""
        side = "white" if active == "w" else "black" if active == "b" else None
        if not side:
            continue
        candidates.append((r, side))
    if not candidates:
        return None
    r, side = random.choice(candidates)
    clock = r.get("clock_remaining") or 0
    return {
        "game_id": r["game_id"],
        "ply": r["ply"],
        "fen_before": r["fen"],
        "played_move": r["played_move"],
        "best_move": r["best_move"],
        "eval_cp": r["eval_cp"],
        "eval_swing_cp": r["eval_swing_cp"],
        "classification": r["classification"],
        "clock_remaining_s": round(clock / 100.0, 1),
        "side_to_move": side,
        "move_number": r["ply"],
        "opening_name": r["opening_name"],
        "opening_eco": r["opening_eco"],
        "speed": r["speed"],
    }
