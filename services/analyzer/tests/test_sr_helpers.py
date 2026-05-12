"""Tests for the two StarRocks helper functions in services/analyzer/worker.py."""

import datetime
from unittest.mock import MagicMock, call

from services.analyzer.worker import fetch_player_games, fetch_plies


def _make_sr(fetchall=None):
    """Return a mock mysql.connector connection whose cursor(dictionary=True) works."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall if fetchall is not None else []

    sr = MagicMock()
    # closing() calls .close() on the object returned by sr.cursor(...);
    # the cursor itself must be returned directly (not via a context manager).
    sr.cursor.return_value = cursor
    return sr, cursor


# ---------------------------------------------------------------------------
# fetch_player_games
# ---------------------------------------------------------------------------


def test_fetch_player_games_returns_rows():
    rows = [
        {"game_id": "g1", "date": datetime.date(2026, 5, 1)},
        {"game_id": "g2", "date": datetime.date(2026, 4, 30)},
        {"game_id": "g3", "date": datetime.date(2026, 4, 29)},
    ]
    sr, cursor = _make_sr(fetchall=rows)

    result = fetch_player_games(sr, "alice", datetime.date(2026, 4, 1), "g0", 20)

    assert result == rows


def test_fetch_player_games_sql_shape():
    sr, cursor = _make_sr()
    fetch_player_games(sr, "alice", datetime.date(2026, 4, 1), "g0", 20)

    sql: str = cursor.execute.call_args.args[0]
    assert "polaris_catalog.prod.player_games" in sql
    assert "player_id = %s" in sql
    sql_upper = sql.upper().replace("\n", " ")
    assert "ORDER BY DATE DESC" in sql_upper
    assert "LIMIT %S" in sql_upper


def test_fetch_player_games_none_defaults():
    sr, cursor = _make_sr()
    fetch_player_games(sr, "alice", None, None, 10)

    params = cursor.execute.call_args.args[1]
    assert datetime.date(1900, 1, 1) in params
    assert "" in params


def test_fetch_player_games_binding_count():
    sr, cursor = _make_sr()
    fetch_player_games(sr, "alice", datetime.date(2026, 1, 1), "g0", 5)

    sql, params = cursor.execute.call_args.args
    placeholders = sql.count("%s")
    assert placeholders == len(params), (
        f"SQL has {placeholders} placeholders but {len(params)} params were passed"
    )


# ---------------------------------------------------------------------------
# fetch_plies
# ---------------------------------------------------------------------------


def test_fetch_plies_returns_rows():
    rows = [
        {"move_number": 1, "fen": "fen1", "whose_moved": "white", "move": "e2e4"},
        {"move_number": 2, "fen": "fen2", "whose_moved": "black", "move": "e7e5"},
    ]
    sr, cursor = _make_sr(fetchall=rows)

    result = fetch_plies(sr, "game42")

    assert result == rows


def test_fetch_plies_sql_shape():
    sr, cursor = _make_sr()
    fetch_plies(sr, "game42")

    sql: str = cursor.execute.call_args.args[0]
    assert "polaris_catalog.prod.chess_move_events" in sql
    assert "WHERE game_id = %s" in sql
    sql_upper = sql.upper().replace("\n", " ")
    assert "GROUP BY" in sql_upper
    assert "ORDER BY MOVE_NUMBER" in sql_upper


def test_fetch_plies_binding_count():
    sr, cursor = _make_sr()
    fetch_plies(sr, "game42")

    sql, params = cursor.execute.call_args.args
    assert sql.count("%s") == 1
    assert len(params) == 1


def test_fetch_plies_uses_dictionary_cursor():
    sr, cursor = _make_sr()
    fetch_plies(sr, "game42")

    sr.cursor.assert_called_with(dictionary=True)
