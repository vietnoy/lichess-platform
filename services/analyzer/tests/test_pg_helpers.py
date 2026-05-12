"""Tests for the four Postgres helper functions in services/analyzer/worker.py."""

from unittest.mock import MagicMock, call, patch

from services.analyzer.worker import (
    eval_with_cache,
    fetch_eligible_players,
    insert_evaluations,
    update_cursor,
)


def _make_pg(fetchone=None, fetchall=None):
    """Return a mock psycopg2 connection whose cursor() context manager works."""
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone
    cursor.fetchall.return_value = fetchall if fetchall is not None else []

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=cursor)
    cm.__exit__ = MagicMock(return_value=False)

    pg = MagicMock()
    pg.cursor.return_value = cm
    return pg, cursor


# ---------------------------------------------------------------------------
# eval_with_cache
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.eval_fen")
def test_eval_with_cache_hit(mock_eval_fen):
    pg, cursor = _make_pg(fetchone=(50, None, "e2e4"))
    result = eval_with_cache(pg, "some/fen")

    assert result == {"cp": 50, "mate": None, "best_move": "e2e4"}
    mock_eval_fen.assert_not_called()
    # no INSERT should have been issued
    for c in cursor.execute.call_args_list:
        sql = c.args[0].upper()
        assert "INSERT" not in sql


@patch("services.analyzer.worker.eval_fen")
def test_eval_with_cache_miss_stockfish_ok(mock_eval_fen):
    mock_eval_fen.return_value = {"cp": 10, "mate": None, "best_move": "d2d4"}
    pg, cursor = _make_pg(fetchone=None)

    result = eval_with_cache(pg, "miss/fen")

    assert result == {"cp": 10, "mate": None, "best_move": "d2d4"}
    # INSERT must have been called with the correct params
    insert_calls = [
        c for c in cursor.execute.call_args_list if "INSERT" in c.args[0].upper()
    ]
    assert len(insert_calls) == 1
    params = insert_calls[0].args[1]
    # (fen, cp, mate, best_move, depth)
    assert params == ("miss/fen", 10, None, "d2d4", 12)


@patch("services.analyzer.worker.eval_fen")
def test_eval_with_cache_miss_stockfish_none(mock_eval_fen):
    mock_eval_fen.return_value = None
    pg, cursor = _make_pg(fetchone=None)

    result = eval_with_cache(pg, "miss/fen")

    assert result is None
    for c in cursor.execute.call_args_list:
        assert "INSERT" not in c.args[0].upper()


# ---------------------------------------------------------------------------
# fetch_eligible_players
# ---------------------------------------------------------------------------


def test_fetch_eligible_players_returns_rows():
    fake_rows = [("alice", "g1", None), ("bob", "g2", None), ("carol", "g3", None)]
    pg, cursor = _make_pg(fetchall=fake_rows)

    result = fetch_eligible_players(pg, 10)

    assert result == fake_rows


def test_fetch_eligible_players_sql_shape():
    pg, cursor = _make_pg(fetchall=[])
    fetch_eligible_players(pg, 5)

    sql: str = cursor.execute.call_args.args[0]
    assert "throttle_until IS NULL" in sql
    assert "ORDER BY THROTTLE_UNTIL" in sql.upper().replace("\n", " ")


def test_fetch_eligible_players_limit_as_parameter():
    pg, cursor = _make_pg(fetchall=[])
    fetch_eligible_players(pg, 7)

    params = cursor.execute.call_args.args[1]
    assert 7 in params


# ---------------------------------------------------------------------------
# update_cursor
# ---------------------------------------------------------------------------


def test_update_cursor_binding_count():
    pg, cursor = _make_pg()
    update_cursor(pg, "alice", "game99", None, 5, 24)

    sql, params = cursor.execute.call_args.args
    placeholders = sql.count("%s")
    assert placeholders == len(params), (
        f"SQL has {placeholders} placeholders but {len(params)} params were passed"
    )


def test_update_cursor_where_player_id():
    pg, cursor = _make_pg()
    update_cursor(pg, "alice", "game99", None, 5, 24)

    sql: str = cursor.execute.call_args.args[0].upper()
    assert "WHERE PLAYER_ID = %S" in sql.replace("\n", " ")


# ---------------------------------------------------------------------------
# insert_evaluations
# ---------------------------------------------------------------------------


def test_insert_evaluations_executemany():
    rows = [
        ("g1", 1, "alice", "fen1", "e2e4", "d2d4", 50, None, -30, "good"),
        ("g1", 2, "alice", "fen2", "d7d5", "g1f3", -10, None, 5, "good"),
    ]
    pg, cursor = _make_pg()

    insert_evaluations(pg, rows)

    cursor.executemany.assert_called_once()
    call_args = cursor.executemany.call_args
    sql: str = call_args.args[0]
    passed_rows = call_args.args[1]

    assert "ON CONFLICT (game_id, ply, player_id) DO NOTHING" in sql
    assert passed_rows == rows


def test_insert_evaluations_empty_rows():
    pg, cursor = _make_pg()

    insert_evaluations(pg, [])

    # early-return path: executemany must not be called
    cursor.executemany.assert_not_called()
