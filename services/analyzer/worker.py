"""Continuous on-demand chess move analyzer. See services/analyzer/PLAN.md."""

from __future__ import annotations

import datetime
import logging
import os
import time
from contextlib import closing
from typing import Any, TYPE_CHECKING

from services.analyzer.stockfish import eval_fen

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Connection

log = logging.getLogger(__name__)

_EVAL_DEPTH = 12
BATCH_GAMES = 20
THROTTLE_HOURS = 24
BATCH_USERS = 5
SLEEP_S = 30


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

    for g in games:
        plies = fetch_plies(sr, g["game_id"], g["date"])
        if not plies:
            continue

        evals = [eval_with_cache(pg, p["fen"]) for p in plies]

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

    if games:
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


def cycle(pg: "Connection", sr: Any, batch_users: int = BATCH_USERS) -> int:
    """One pass: fetch up to batch_users eligible players and process each.

    Per-player failures are caught and logged so one bad player doesn't kill the loop.
    Returns the number of players attempted (regardless of per-player success/failure).
    """
    targets = fetch_eligible_players(pg, batch_users)
    # Release the SELECT's transaction so per-player work starts clean.
    pg.commit()
    for player_id, last_game_id, last_game_date in targets:
        try:
            n = process_player(pg, sr, player_id, last_game_id, last_game_date)
            log.info("processed player=%s games=%s", player_id, n)
        except Exception:
            pg.rollback()
            log.exception("process_player failed for player=%s", player_id)
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
    import psycopg2

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

    def _connect_pg():
        return psycopg2.connect(pg_dsn)

    def _connect_sr():
        # autocommit=True: StarRocks doesn't honor MySQL transaction semantics, and
        # the connector otherwise tracks fake txn state that desyncs over time.
        return mysql.connector.connect(
            host=sr_host, port=sr_port, user=sr_user, password=sr_password,
            autocommit=True,
        )

    pg = _connect_pg()
    sr = _connect_sr()
    # Touch liveness file at startup so the probe has something to read before
    # the first cycle completes (a slow first cycle can take ~3-4 minutes).
    _touch_liveness()
    log.info("analyzer worker starting")

    while True:
        try:
            cycle(pg, sr)
            _touch_liveness()
        except psycopg2.OperationalError:
            log.exception("postgres connection broken; reconnecting")
            try:
                pg.close()
            except Exception:
                pass
            pg = _connect_pg()
        except mysql.connector.Error:
            log.exception("starrocks connection broken; reconnecting")
            try:
                sr.close()
            except Exception:
                pass
            sr = _connect_sr()
        except Exception:
            log.exception("cycle failed")
        time.sleep(SLEEP_S)


if __name__ == "__main__":
    main()
