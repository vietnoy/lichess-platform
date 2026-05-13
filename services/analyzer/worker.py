"""Continuous on-demand chess move analyzer. See services/analyzer/PLAN.md."""

from __future__ import annotations

import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from typing import Any, TYPE_CHECKING

from services.analyzer.stockfish import eval_fen

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Connection

log = logging.getLogger(__name__)


def _is_connection_error(exc: BaseException) -> bool:
    # Lazy-imported so the worker module imports cleanly without psycopg2 /
    # mysql.connector installed (the tests' fake-mysql fixture relies on this).
    connection_errors: list[type[BaseException]] = []
    try:
        import psycopg2  # noqa: PLC0415

        connection_errors.extend([psycopg2.OperationalError, psycopg2.InterfaceError])
    except ImportError:
        pass
    try:
        from mysql.connector import errors as mysql_errors  # noqa: PLC0415

        connection_errors.extend([mysql_errors.OperationalError, mysql_errors.InterfaceError])
    except ImportError:
        pass
    return bool(connection_errors) and isinstance(exc, tuple(connection_errors))


_EVAL_DEPTH = 12
_STOCKFISH_PARALLELISM = 8
BATCH_GAMES = 20
THROTTLE_HOURS = 24
BATCH_USERS = 5
SLEEP_S = 5


def _call_stockfish(fen: str) -> dict | None:
    return eval_fen(fen, depth=_EVAL_DEPTH)


def eval_with_cache(pg: "Connection", fen: str) -> dict | None:
    with pg.cursor() as c:
        c.execute("SELECT cp, mate, best_move FROM position_evals WHERE fen = %s", (fen,))
        row = c.fetchone()
    # Treat a row with both cp and mate NULL as a poisoned cache entry (a prior
    # malformed Stockfish response). Re-evaluate it instead of returning useless data.
    if row is not None and (row[0] is not None or row[1] is not None):
        return {"cp": row[0], "mate": row[1], "best_move": row[2]}

    result = eval_fen(fen, depth=_EVAL_DEPTH)
    if result is None:
        return None
    cp, mate, best_move = result.get("cp"), result.get("mate"), result.get("best_move")
    # Don't write a poisoned entry; the next caller will retry.
    if cp is None and mate is None:
        return None

    with pg.cursor() as c:
        c.execute(
            "INSERT INTO position_evals (fen, cp, mate, best_move, depth)"
            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (fen) DO NOTHING",
            (fen, cp, mate, best_move, _EVAL_DEPTH),
        )
    return {"cp": cp, "mate": mate, "best_move": best_move}


def eval_plies_batch(pg: "Connection", plies: list[dict]) -> list[dict | None]:
    """Evaluate plies with one cache read, parallel Stockfish misses, and one insert."""
    fens = [p["fen"] for p in plies]
    unique_fens = list(dict.fromkeys(f for f in fens if f))

    cache: dict[str, dict | None] = {}
    if unique_fens:
        with pg.cursor() as c:
            c.execute(
                "SELECT fen, cp, mate, best_move FROM position_evals"
                " WHERE fen = ANY(%s)",
                (unique_fens,),
            )
            for fen, cp, mate, best_move in c.fetchall():
                # Treat a row with both cp and mate NULL as poisoned; retry it.
                if cp is None and mate is None:
                    continue
                cache[fen] = {"cp": cp, "mate": mate, "best_move": best_move}

    misses = [fen for fen in unique_fens if fen not in cache]

    fresh: dict[str, dict | None] = {}
    if misses:
        with ThreadPoolExecutor(max_workers=_STOCKFISH_PARALLELISM) as ex:
            for fen, result in zip(misses, ex.map(_call_stockfish, misses)):
                fresh[fen] = result

    insertable = [
        (fen, r.get("cp"), r.get("mate"), r.get("best_move"), _EVAL_DEPTH)
        for fen, r in fresh.items()
        if r is not None and (r.get("cp") is not None or r.get("mate") is not None)
    ]
    if insertable:
        with pg.cursor() as c:
            c.executemany(
                "INSERT INTO position_evals (fen, cp, mate, best_move, depth)"
                " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (fen) DO NOTHING",
                insertable,
            )

    def normalize(r: dict | None) -> dict | None:
        if r is None:
            return None
        if r.get("cp") is None and r.get("mate") is None:
            return None
        return {"cp": r.get("cp"), "mate": r.get("mate"), "best_move": r.get("best_move")}

    combined: dict[str, dict | None] = dict(cache)
    for fen, result in fresh.items():
        combined[fen] = normalize(result)

    return [combined.get(p["fen"]) for p in plies]


def fetch_eligible_players(
    pg: "Connection", limit: int
) -> list[tuple[str, str | None, datetime.date | None]]:
    with pg.cursor() as c:
        c.execute(
            "SELECT player_id, last_game_id, last_game_date"
            " FROM analyzer_cursor"
            " WHERE throttle_until IS NULL OR throttle_until < now()"
            " ORDER BY throttle_until ASC NULLS FIRST"
            " LIMIT %s",
            (limit,),
        )
        return c.fetchall()  # type: ignore[return-value]


def update_cursor(
    pg: "Connection",
    player_id: str,
    last_game_id: str,
    last_game_date: datetime.date | None,
    games_delta: int,
    throttle_hours: int,
) -> None:
    # (%s || ' hours')::interval lets us bind the hour count as a parameter
    # instead of f-string-interpolating it into the SQL.
    with pg.cursor() as c:
        c.execute(
            "UPDATE analyzer_cursor"
            " SET last_game_id = %s,"
            "     last_game_date = %s,"
            "     last_processed_at = now(),"
            "     games_processed = games_processed + %s,"
            "     throttle_until = now() + (%s || ' hours')::interval"
            " WHERE player_id = %s",
            (last_game_id, last_game_date, games_delta, f"{throttle_hours}", player_id),
        )


def insert_evaluations(pg: "Connection", rows: list[tuple]) -> None:
    if not rows:
        return
    with pg.cursor() as c:
        c.executemany(
            "INSERT INTO move_evaluations_ondemand"
            " (game_id, ply, player_id, fen, played_move, best_move,"
            "  eval_cp, mate, eval_swing_cp, classification)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (game_id, ply, player_id) DO NOTHING",
            rows,
        )


def fetch_player_games(
    sr: Any,
    player_id: str,
    last_date: datetime.date | None,
    last_game_id: str | None,
    limit: int,
) -> list[dict]:
    # Sentinel 1900-01-01 matches everything (lichess games postdate ~2007). The
    # WHERE/ORDER BY both walk strictly forward in time so the cursor stored by
    # update_cursor (the last row of the batch = newest in batch under ASC) always
    # advances; pairing `date > cursor` with `ORDER BY date DESC` would loop.
    sentinel_date = last_date if last_date is not None else datetime.date(1900, 1, 1)
    sentinel_game_id = last_game_id if last_game_id is not None else ""
    with closing(sr.cursor(dictionary=True)) as c:
        c.execute(
            "SELECT game_id, date"
            " FROM polaris_catalog.prod.player_games"
            " WHERE player_id = %s"
            "   AND (date > %s OR (date = %s AND game_id > %s))"
            " ORDER BY date ASC, game_id ASC"
            " LIMIT %s",
            (player_id, sentinel_date, sentinel_date, sentinel_game_id, limit),
        )
        return c.fetchall()


def fetch_plies(sr: Any, game_id: str, date: datetime.date | None = None) -> list[dict]:
    # chess_move_events has upstream duplicate rows per (game_id, move_number);
    # GROUP BY deduplicates without a subquery. When the caller knows the game's
    # date (it's in the player_games row we fetched a moment ago), passing it
    # prunes the partition scan on a 17M-row table.
    if date is not None:
        sql = (
            "SELECT move_number, fen, whose_moved, move"
            " FROM polaris_catalog.prod.chess_move_events"
            " WHERE game_id = %s AND date = %s"
            " GROUP BY move_number, fen, whose_moved, move"
            " ORDER BY move_number"
        )
        params: tuple = (game_id, date)
    else:
        sql = (
            "SELECT move_number, fen, whose_moved, move"
            " FROM polaris_catalog.prod.chess_move_events"
            " WHERE game_id = %s"
            " GROUP BY move_number, fen, whose_moved, move"
            " ORDER BY move_number"
        )
        params = (game_id,)
    with closing(sr.cursor(dictionary=True)) as c:
        c.execute(sql, params)
        return c.fetchall()


def fetch_plies_batch(
    sr: Any,
    games: list[dict],
) -> dict[str, list[dict]]:
    """Fetch plies for many games in one StarRocks query.

    Returns a dict keyed by game_id, value = list of ply dicts in move_number order.
    Each game must have `game_id` and `date`; date prunes partitions while game_id
    filters within those partitions. GROUP BY dedupes upstream duplicate move rows.
    """
    if not games:
        return {}

    dates = list(dict.fromkeys(g["date"] for g in games))
    game_ids = list(dict.fromkeys(g["game_id"] for g in games))

    with closing(sr.cursor(dictionary=True)) as c:
        placeholders_d = ",".join(["%s"] * len(dates))
        placeholders_g = ",".join(["%s"] * len(game_ids))
        c.execute(
            f"SELECT game_id, move_number, fen, whose_moved, move"
            f" FROM polaris_catalog.prod.chess_move_events"
            f" WHERE date IN ({placeholders_d})"
            f"   AND game_id IN ({placeholders_g})"
            f" GROUP BY game_id, move_number, fen, whose_moved, move"
            f" ORDER BY game_id, move_number",
            (*dates, *game_ids),
        )
        rows = c.fetchall()

    out: dict[str, list[dict]] = {gid: [] for gid in game_ids}
    for r in sorted(rows, key=lambda row: (row["game_id"], row["move_number"])):
        out[r["game_id"]].append(r)
    return out


def process_player(
    pg: "Connection",
    sr: Any,
    player_id: str,
    last_game_id: str | None,
    last_game_date: datetime.date | None,
) -> int:
    """Process up to BATCH_GAMES new games for one player.

    Commits after each game so partial progress survives a crash.
    Returns the number of games fetched (not rows written).
    """
    games = fetch_player_games(sr, player_id, last_game_date, last_game_id, BATCH_GAMES)

    if games:
        plies_by_game = fetch_plies_batch(sr, games)

        for g in games:
            plies = plies_by_game.get(g["game_id"], [])
            if not plies:
                continue

            evals = eval_plies_batch(pg, plies)

            rows: list[tuple] = []
            for i, p in enumerate(plies):
                cp_before = (evals[i] or {}).get("cp")
                cp_after = (evals[i + 1] or {}).get("cp") if i + 1 < len(evals) else None
                swing, classification = classify_move(cp_before, cp_after, p["whose_moved"])
                rows.append((
                    g["game_id"],
                    p["move_number"],
                    player_id,
                    p["fen"],
                    p["move"],
                    (evals[i] or {}).get("best_move"),
                    cp_before,
                    (evals[i] or {}).get("mate"),
                    swing,
                    classification,
                ))

            insert_evaluations(pg, rows)
            pg.commit()

        newest = games[-1]  # ASC ordering -> newest is last
        update_cursor(
            pg, player_id, newest["game_id"], newest["date"],
            games_delta=len(games), throttle_hours=THROTTLE_HOURS,
        )
    else:
        # No new games — still throttle so we don't keep selecting this user.
        # Preserve existing cursor position; fall back to epoch for new users.
        update_cursor(
            pg, player_id, last_game_id or "", last_game_date or datetime.date(1900, 1, 1),
            games_delta=0, throttle_hours=THROTTLE_HOURS,
        )
    pg.commit()

    return len(games)


def classify_move(
    cp_before: int | None,
    cp_after: int | None,
    mover: str,
) -> tuple[int | None, str | None]:
    if cp_before is None or cp_after is None:
        return None, None
    if mover == "white":
        drop = cp_before - cp_after
        swing = cp_after - cp_before
    else:
        drop = cp_after - cp_before
        swing = cp_before - cp_after

    if drop >= 200:
        classification = "blunder"
    elif drop >= 100:
        classification = "mistake"
    elif drop >= 50:
        classification = "inaccuracy"
    else:
        classification = "good"
    return swing, classification


def cycle(pg_pool: Any, sr_pool: Any, batch_users: int = BATCH_USERS) -> int:
    """One pass: fetch up to batch_users eligible players and process each.

    Per-player failures are caught and logged so one bad player doesn't kill the loop.
    Returns the number of players attempted (regardless of per-player success/failure).
    """
    main_pg = pg_pool.getconn()
    try:
        targets = fetch_eligible_players(main_pg, batch_users)
        # Release the SELECT's transaction so per-player work starts clean.
        main_pg.commit()
    finally:
        pg_pool.putconn(main_pg)

    if not targets:
        return 0

    def _run_one(target: tuple[str, str | None, datetime.date | None]) -> None:
        player_id, last_game_id, last_game_date = target
        pg = pg_pool.getconn()
        try:
            sr = sr_pool.get_connection()
        except Exception:
            pg_pool.putconn(pg)
            raise
        try:
            n = process_player(pg, sr, player_id, last_game_id, last_game_date)
            log.info("processed player=%s games=%s", player_id, n)
        except Exception as exc:
            try:
                pg.rollback()
            except Exception:
                pass
            if _is_connection_error(exc):
                log.exception("connection error while processing player=%s", player_id)
                raise
            log.exception("process_player failed for player=%s", player_id)
        finally:
            # Wrap each cleanup independently so a putconn failure doesn't
            # short-circuit sr.close() and leak the StarRocks connection.
            try:
                pg_pool.putconn(pg)
            except Exception:
                log.exception("pg_pool.putconn failed for player=%s", player_id)
            try:
                sr.close()
            except Exception:
                pass

    with ThreadPoolExecutor(max_workers=batch_users) as ex:
        list(ex.map(_run_one, targets))

    return len(targets)


# Touched after each successful cycle so a k8s liveness probe (or operator) can
# spot a zombie worker with a dead connection that the outer try/except keeps masking.
LIVENESS_FILE = "/tmp/analyzer-last-cycle"


def _touch_liveness() -> None:
    try:
        with open(LIVENESS_FILE, "w") as f:
            f.write(str(int(time.time())))
    except OSError:
        log.warning("could not touch liveness file %s", LIVENESS_FILE)


def main() -> None:
    """Entry point. Connects to Postgres + StarRocks, loops cycle() forever."""
    import mysql.connector
    import mysql.connector.pooling
    import psycopg2
    import psycopg2.pool

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    pg_dsn = os.environ["ANALYZER_PG_DSN"]
    sr_host = os.environ["STARROCKS_HOST"]
    sr_port = int(os.getenv("STARROCKS_PORT", "9030"))
    sr_user = os.getenv("STARROCKS_USER", "root")
    sr_password = os.getenv("STARROCKS_PASSWORD", "")

    sr_kwargs = {
        "host": sr_host,
        "port": sr_port,
        "user": sr_user,
        "password": sr_password,
        # autocommit=True: StarRocks doesn't honor MySQL transaction semantics, and
        # the connector otherwise tracks fake txn state that desyncs over time.
        "autocommit": True,
    }

    pg_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=BATCH_USERS + 1, dsn=pg_dsn,
    )
    sr_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="analyzer", pool_size=BATCH_USERS + 1, **sr_kwargs,
    )
    # Touch liveness file at startup so the probe has something to read before
    # the first cycle completes (a slow first cycle can take ~3-4 minutes).
    _touch_liveness()
    log.info("analyzer worker starting")

    while True:
        try:
            cycle(pg_pool, sr_pool)
            _touch_liveness()
        except psycopg2.OperationalError:
            log.exception("postgres pool issue; recreating pool")
            try:
                pg_pool.closeall()
            except Exception:
                pass
            pg_pool = psycopg2.pool.ThreadedConnectionPool(
                1, BATCH_USERS + 1, dsn=pg_dsn,
            )
        except mysql.connector.Error:
            log.exception("starrocks pool issue; recreating pool")
            sr_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="analyzer", pool_size=BATCH_USERS + 1, **sr_kwargs,
            )
        except Exception:
            log.exception("cycle failed")
        time.sleep(SLEEP_S)


if __name__ == "__main__":
    main()
