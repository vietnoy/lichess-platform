"""StarRocks connection + query helpers."""

import datetime as dt
import os
import random
import time
import logging
import threading
from contextlib import contextmanager
import mysql.connector
from mysql.connector import errors
from mysql.connector import pooling

log = logging.getLogger("db")

STARROCKS_POOL_SIZE = int(os.getenv("STARROCKS_POOL_SIZE", "12"))
STARROCKS_POOL_WAIT_SECONDS = float(os.getenv("STARROCKS_POOL_WAIT_SECONDS", "2.0"))

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
_profile_cache: dict[tuple, tuple[float, dict | None]] = {}
_profile_lock = threading.Lock()
_SYSTEM_TTL = int(os.getenv("SYSTEM_QUERY_CACHE_TTL", "300"))
_PLATFORM_TTL = int(os.getenv("PLATFORM_QUERY_CACHE_TTL", "300"))
_PLAYER_AGG_TTL = int(os.getenv("PLAYER_AGG_QUERY_CACHE_TTL", "180"))
_query_cache: dict[tuple, tuple[float, object]] = {}
_query_cache_lock = threading.Lock()


def _cached(key: tuple, ttl: int, loader):
    now = time.time()
    with _query_cache_lock:
        cached = _query_cache.get(key)
        if cached and now - cached[0] < ttl:
            return cached[1]
    value = loader()
    with _query_cache_lock:
        _query_cache[key] = (now, value)
    return value


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
                pool_size=STARROCKS_POOL_SIZE,
                host=os.getenv("STARROCKS_HOST", "starrocks-fe"),
                port=int(os.getenv("STARROCKS_PORT", "9030")),
                user=os.getenv("STARROCKS_USER", "root"),
                password=os.getenv("STARROCKS_PASSWORD", ""),
                connection_timeout=10,
            )
            log.info("starrocks pool initialized size=%s", STARROCKS_POOL_SIZE)

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
        deadline = time.monotonic() + STARROCKS_POOL_WAIT_SECONDS
        while True:
            try:
                conn = cls._pool.get_connection()
                break
            except errors.PoolError as exc:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
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
    return _cached(("system_summary",), _SYSTEM_TTL, _query_system_summary_uncached)


def _query_system_summary_uncached() -> dict:
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
    rating_histogram = _run(
        f"""
        SELECT
            CAST(FLOOR(my_rating / 200) * 200 AS INT) AS bucket_floor,
            COUNT(DISTINCT player_id) AS players,
            COUNT(*) AS player_game_rows
        FROM {PLAYER_GAMES}
        WHERE my_rating IS NOT NULL
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        GROUP BY bucket_floor
        ORDER BY bucket_floor
        """
    )
    return {
        "tables": tables,
        "totals": {
            "latest_partition_rows": total_rows,
            "tables": len(tables),
            "latest_date": max(latest_dates) if latest_dates else None,
        },
        "rating_histogram": rating_histogram,
    }


def _to_iso_date(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _parse_iso_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD") from exc


def _date_range_filter(column: str, start_date: str | None, end_date: str | None) -> tuple[str, tuple]:
    if start_date and end_date:
        return f"WHERE {column} BETWEEN %s AND %s", (start_date, end_date)
    return "", ()


def query_platform_overview(days: int | None = 30, date: str | None = None, all_time: bool = False) -> dict:
    key = ("platform_overview", int(days or 0), date, bool(all_time))
    return _cached(key, _PLATFORM_TTL, lambda: _query_platform_overview_uncached(days=days, date=date, all_time=all_time))


def _query_platform_overview_uncached(days: int | None = 30, date: str | None = None, all_time: bool = False) -> dict:
    latest = _run(
        f"""
        SELECT MAX(date) AS date
        FROM {PLAYER_GAMES}
        """
    )
    latest_date = _to_iso_date((latest[0] if latest else {}).get("date"))
    if not latest_date:
        return {
            "date": None,
            "start_date": None,
            "end_date": None,
            "range": "empty",
            "totals": {"games": 0, "player_game_rows": 0, "players": 0},
            "speed_mix": [],
            "top_openings": [],
            "phase_mistakes": [],
        }

    if date:
        selected = _parse_iso_date(date).isoformat()
        start_date = selected
        end_date = selected
        range_label = "date"
    elif all_time:
        start_date = None
        end_date = None
        range_label = "all"
    else:
        window_days = max(1, min(int(days or 30), 365))
        end = _parse_iso_date(latest_date)
        start = end - dt.timedelta(days=window_days - 1)
        start_date = start.isoformat()
        end_date = end.isoformat()
        range_label = f"{window_days}d"

    player_filter, player_params = _date_range_filter("date", start_date, end_date)
    opening_filter, opening_params = _date_range_filter("date", start_date, end_date)
    phase_filter, phase_params = _date_range_filter("date", start_date, end_date)

    totals = _run(
        f"""
        SELECT
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS player_game_rows,
            COUNT(DISTINCT player_id) AS players
        FROM {PLAYER_GAMES}
        {player_filter}
        """,
        player_params,
    )
    speed_mix = _run(
        f"""
        SELECT
            COALESCE(speed, 'unknown') AS speed,
            COUNT(DISTINCT game_id) AS games,
            COUNT(*) AS player_game_rows,
            ROUND(AVG(my_rating), 0) AS avg_rating
        FROM {PLAYER_GAMES}
        {player_filter}
        GROUP BY speed
        ORDER BY games DESC
        LIMIT 8
        """,
        player_params,
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
        {opening_filter}
        GROUP BY opening_eco, opening_name
        HAVING SUM(games) >= 20
        ORDER BY games DESC
        LIMIT 10
        """,
        opening_params,
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
        {phase_filter}
        GROUP BY phase
        ORDER BY critical_positions DESC
        """,
        phase_params,
    )

    return {
        "date": end_date or latest_date,
        "start_date": start_date,
        "end_date": end_date,
        "range": range_label,
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


def query_player_profile(
    username: str,
    days: int | None = 60,
    date: str | None = None,
    all_time: bool = False,
) -> dict | None:
    if date:
        _parse_iso_date(date)
    window_days = None if all_time or date else clamp_int(days or 60, 1, 365)
    key = ("profile", username, int(window_days or 0), date, bool(all_time))
    now = time.time()
    with _profile_lock:
        cached = _profile_cache.get(key)
        if cached and now - cached[0] < _PROFILE_TTL:
            return cached[1]
    profile = _query_player_profile_uncached(username, days=window_days, date=date, all_time=all_time)
    with _profile_lock:
        _profile_cache[key] = (now, profile)
    return profile


def _query_player_profile_uncached(
    username: str,
    days: int | None = 60,
    date: str | None = None,
    all_time: bool = False,
) -> dict | None:
    # player_games is the denormalized projection (one row per game-side) built by
    # processing/build_player_games.py. Sorted by player_id within each partition so
    # parquet min/max stats let StarRocks skip whole files for WHERE player_id=?.
    filters = ["player_id=%s"]
    params: list = [username]
    if date:
        selected = _parse_iso_date(date).isoformat()
        filters.append("date = DATE %s")
        params.append(selected)
        range_label = "date"
        start_date = selected
        end_date = selected
    elif all_time:
        range_label = "all"
        start_date = None
        end_date = None
    else:
        window_days = clamp_int(days or 60, 1, 365)
        filters.append("date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)")
        params.append(window_days)
        range_label = f"{window_days}d"
        start_date = None
        end_date = None

    games = _run(
        f"""
        SELECT game_id, color, opponent_id, my_rating, opp_rating,
               speed, opening_eco, opening_name, winner, end_status, date
        FROM {PLAYER_GAMES}
        WHERE {" AND ".join(filters)}
        """,
        tuple(params),
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

    def date_key(g):
        return _to_iso_date(g.get("date")) or ""

    recent = sorted(games, key=date_key, reverse=True)[:15]
    recent_out = [{
        "game_id":      g["game_id"],
        "opponent":     g["opponent_id"],
        "my_rating":    g["my_rating"],
        "opp_rating":   g["opp_rating"],
        "opening_eco":  g["opening_eco"],
        "opening_name": g["opening_name"],
        "speed":        g["speed"],
        "result":       result(g),
        "date":         _to_iso_date(g["date"]),
    } for g in recent]

    rating_acc: dict[str, dict[str, int]] = {}
    for g in games:
        rating = g["my_rating"]
        game_date = _to_iso_date(g["date"])
        if rating is None or not game_date:
            continue
        entry = rating_acc.setdefault(game_date, {"rating_sum": 0, "games": 0})
        entry["rating_sum"] += int(rating)
        entry["games"] += 1
    rating_history = [
        {
            "date": game_date,
            "avg_rating": round(entry["rating_sum"] / entry["games"]),
            "games": entry["games"],
        }
        for game_date, entry in sorted(rating_acc.items())
    ]

    if range_label.endswith("d") and games:
        dates = sorted(date_key(g) for g in games if date_key(g))
        start_date = dates[0] if dates else None
        end_date = dates[-1] if dates else None

    total_games  = len(games)
    total_wins   = sum(1 for g in games if result(g) == "Win")
    total_losses = sum(1 for g in games if result(g) == "Loss")
    total_draws  = total_games - total_wins - total_losses
    ratings      = [g["my_rating"] for g in games if g["my_rating"] is not None]
    avg_rating   = round(sum(ratings) / len(ratings)) if ratings else 0
    win_pct      = round(total_wins * 100 / total_games, 1) if total_games else 0

    return {
        "username": username,
        "range": {
            "label": range_label,
            "start_date": start_date,
            "end_date": end_date,
        },
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
        "rating_history": rating_history,
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


def _player_aggregate_date_filter(
    days: int = 60,
    date: str | None = None,
    all_time: bool = False,
) -> tuple[str, tuple, int]:
    if date:
        selected = _parse_iso_date(date).isoformat()
        return "AND date = DATE %s", (selected,), 1
    if all_time:
        return "", (), 0
    clamped_days = clamp_int(days, 1, 365)
    return "AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)", (clamped_days,), clamped_days


def query_weakness_summary(
    username: str,
    days: int = 60,
    date: str | None = None,
    all_time: bool = False,
) -> dict:
    date_filter, date_params, effective_days = _player_aggregate_date_filter(days, date, all_time)
    key = ("weakness_summary", username, effective_days, date, bool(all_time))
    return _cached(
        key,
        _PLAYER_AGG_TTL,
        lambda: _query_weakness_summary_uncached(username, effective_days, date_filter, date_params),
    )


def _query_weakness_summary_uncached(
    username: str,
    effective_days: int,
    date_filter: str,
    date_params: tuple,
) -> dict:
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
          {date_filter}
        GROUP BY player_id
        """,
        (username,) + date_params,
    )
    if rows:
        return rows[0]
    return {
        "player_id": username,
        "days": effective_days,
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


def query_opening_stats(
    username: str,
    days: int = 60,
    top_n: int = 10,
    date: str | None = None,
    all_time: bool = False,
) -> list[dict]:
    date_filter, date_params, effective_days = _player_aggregate_date_filter(days, date, all_time)
    top_n = clamp_int(top_n, 1, 20)
    key = ("opening_stats", username, effective_days, top_n, date, bool(all_time))
    return _cached(
        key,
        _PLAYER_AGG_TTL,
        lambda: _query_opening_stats_uncached(username, top_n, date_filter, date_params),
    )


def _query_opening_stats_uncached(
    username: str,
    top_n: int,
    date_filter: str,
    date_params: tuple,
) -> list[dict]:
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
          {date_filter}
        GROUP BY opening_eco, opening_name, color
        HAVING SUM(games) >= 2
        ORDER BY blunders DESC, mistakes DESC, critical_positions DESC, games DESC
        LIMIT %s
        """,
        (username,) + date_params + (top_n,),
    )


def query_phase_stats(
    username: str,
    days: int = 60,
    date: str | None = None,
    all_time: bool = False,
) -> list[dict]:
    date_filter, date_params, effective_days = _player_aggregate_date_filter(days, date, all_time)
    key = ("phase_stats", username, effective_days, date, bool(all_time))
    return _cached(
        key,
        _PLAYER_AGG_TTL,
        lambda: _query_phase_stats_uncached(username, date_filter, date_params),
    )


def _query_phase_stats_uncached(username: str, date_filter: str, date_params: tuple) -> list[dict]:
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
          {date_filter}
        GROUP BY phase
        ORDER BY critical_positions DESC, blunders DESC
        """,
        (username,) + date_params,
    )


def _num(value, default=0):
    return default if value is None else value


def _insight_window_label(days: int, date: str | None, all_time: bool) -> str:
    if date:
        return f"ngày {date}"
    if all_time:
        return "toàn bộ dữ liệu"
    return f"{days} ngày gần đây"


def query_player_insights(
    username: str,
    days: int = 60,
    date: str | None = None,
    all_time: bool = False,
) -> dict:
    _, _, effective_days = _player_aggregate_date_filter(days, date, all_time)
    key = ("player_insights", username, effective_days, date, bool(all_time))
    return _cached(
        key,
        _PLAYER_AGG_TTL,
        lambda: _query_player_insights_uncached(username, effective_days, date, all_time),
    )


def _query_player_insights_uncached(
    username: str,
    days: int = 60,
    date: str | None = None,
    all_time: bool = False,
) -> dict:
    weakness = query_weakness_summary(username, days=days or 60, date=date, all_time=all_time)
    phase_stats = query_phase_stats(username, days=days or 60, date=date, all_time=all_time)
    opening_stats = query_opening_stats(username, days=days or 60, top_n=10, date=date, all_time=all_time)
    profile = query_player_profile(username, days=days or 60, date=date, all_time=all_time) or {}
    window_label = _insight_window_label(days, date, all_time)

    insights: list[dict] = []

    if phase_stats:
        phase = phase_stats[0]
        critical = int(_num(phase.get("critical_positions")))
        blunders = int(_num(phase.get("blunders")))
        mistakes = int(_num(phase.get("mistakes")))
        if critical > 0:
            score = min(100, 35 + critical * 2 + blunders * 5 + mistakes * 2)
            insights.append(
                {
                    "type": "phase_weakness",
                    "score": score,
                    "title": f"Bạn đang mất điểm nhiều nhất ở {phase.get('phase')}",
                    "evidence": (
                        f"{critical} critical positions, {blunders} blunders và {mistakes} mistakes "
                        f"trong {window_label}."
                    ),
                    "action": "Ưu tiên drill theo phase này trước khi học thêm opening mới.",
                    "data": phase,
                }
            )

    for opening in opening_stats[:3]:
        games = int(_num(opening.get("games")))
        win_rate = _num(opening.get("win_rate_pct"), None)
        critical = int(_num(opening.get("critical_positions")))
        blunders = int(_num(opening.get("blunders")))
        mistakes = int(_num(opening.get("mistakes")))
        if games < 2:
            continue
        weak_win_rate = win_rate is not None and win_rate < 45
        if critical == 0 and not weak_win_rate:
            continue
        score = min(100, 25 + games * 2 + critical * 3 + blunders * 6 + mistakes * 2 + (15 if weak_win_rate else 0))
        insights.append(
            {
                "type": "opening_leak",
                "score": score,
                "title": f"{opening.get('opening_eco') or '-'} · {opening.get('opening_name') or 'Unknown'} cần review",
                "evidence": (
                    f"{games} games, win rate {win_rate if win_rate is not None else 'n/a'}%, "
                    f"{critical} critical positions."
                ),
                "action": "Review 3-5 game gần nhất trong opening này rồi tạo drill từ các critical positions.",
                "data": opening,
            }
        )

    time_pressure_positions = int(_num(weakness.get("time_pressure_positions") if weakness else 0))
    critical_total = int(_num(weakness.get("critical_positions") if weakness else 0))
    if time_pressure_positions > 0 and critical_total > 0:
        pressure_share = round(time_pressure_positions * 100.0 / critical_total, 1)
        if pressure_share >= 20 or time_pressure_positions >= 5:
            insights.append(
                {
                    "type": "time_pressure",
                    "score": min(100, 30 + time_pressure_positions * 4),
                    "title": "Time pressure đang tạo lỗi đáng kể",
                    "evidence": f"{time_pressure_positions}/{critical_total} critical positions xảy ra dưới áp lực thời gian.",
                    "action": "Tập drill có timer và ưu tiên quyết định candidate moves nhanh hơn.",
                    "data": {"time_pressure_positions": time_pressure_positions, "critical_positions": critical_total, "share_pct": pressure_share},
                }
            )

    by_color = profile.get("by_color") or []
    if len(by_color) >= 2:
        best = max(by_color, key=lambda row: row.get("win_pct") or 0)
        worst = min(by_color, key=lambda row: row.get("win_pct") or 0)
        gap = round((best.get("win_pct") or 0) - (worst.get("win_pct") or 0), 1)
        worst_games = int(_num(worst.get("games")))
        if gap >= 15 and worst_games >= 5:
            insights.append(
                {
                    "type": "color_gap",
                    "score": min(100, 25 + int(gap) + worst_games),
                    "title": f"Hiệu suất cầm {worst.get('color')} thấp hơn rõ rệt",
                    "evidence": f"Win rate lệch {gap} điểm phần trăm giữa {best.get('color')} và {worst.get('color')}.",
                    "action": "So sánh repertoire và chọn một opening ổn định hơn cho màu quân yếu.",
                    "data": {"best": best, "worst": worst, "gap_pct": gap},
                }
            )

    insights.sort(key=lambda item: (-item["score"], item["type"]))
    return {"player_id": username, "days": days, "insights": insights[:6]}


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
