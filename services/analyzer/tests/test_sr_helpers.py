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
    sql_compact = " ".join(sql.split())
    assert "polaris_catalog.prod.player_games" in sql_compact
    assert "player_id = %s" in sql_compact
    # Cursor pagination requires forward-only walk: WHERE uses strict-greater on
    # (date, game_id) tiebreaker AND ORDER BY both ASC, otherwise cursor loops.
    assert "(date > %s OR (date = %s AND game_id > %s))" in sql_compact
    sql_upper = sql_compact.upper()
    assert "ORDER BY DATE ASC, GAME_ID ASC" in sql_upper
    assert "LIMIT %S" in sql_upper


def test_fetch_player_games_none_defaults_exact_tuple():
    sr, cursor = _make_sr()
    fetch_player_games(sr, "alice", None, None, 10)

    params = cursor.execute.call_args.args[1]
    # Positional order must be: player_id, sentinel_date, sentinel_date, sentinel_gid, limit
    assert params == ("alice", datetime.date(1900, 1, 1), datetime.date(1900, 1, 1), "", 10)


def test_fetch_player_games_uses_dictionary_cursor():
    sr, cursor = _make_sr()
    fetch_player_games(sr, "alice", None, None, 10)
    sr.cursor.assert_called_with(dictionary=True)


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


def test_fetch_plies_sql_shape_no_date():
    sr, cursor = _make_sr()
    fetch_plies(sr, "game42")

    sql: str = cursor.execute.call_args.args[0]
    sql_compact = " ".join(sql.split())
    assert "polaris_catalog.prod.chess_move_events" in sql_compact
    assert "WHERE game_id = %s" in sql_compact
    # GROUP BY must include all four columns or dedup semantics shift.
    assert "GROUP BY move_number, fen, whose_moved, move" in sql_compact
    assert "ORDER BY move_number" in sql_compact


def test_fetch_plies_with_date_prunes_partition():
    sr, cursor = _make_sr()
    fetch_plies(sr, "game42", datetime.date(2026, 4, 18))

    sql, params = cursor.execute.call_args.args
    sql_compact = " ".join(sql.split())
    assert "WHERE game_id = %s AND date = %s" in sql_compact
    assert params == ("game42", datetime.date(2026, 4, 18))


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
