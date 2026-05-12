"""Tests for bulk StarRocks ply fetching."""

import datetime
import re
from unittest.mock import MagicMock

from services.analyzer.worker import fetch_plies_batch


def _make_sr(fetchall=None):
    """Return a mock mysql.connector connection whose cursor(dictionary=True) works."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall if fetchall is not None else []

    sr = MagicMock()
    # closing() calls .close() on the object returned by sr.cursor(...);
    # the cursor itself must be returned directly (not via a context manager).
    sr.cursor.return_value = cursor
    return sr, cursor


def _compact(sql: str) -> str:
    return " ".join(sql.split())


def _in_placeholder_count(sql: str, column: str) -> int:
    match = re.search(rf"{column} IN \(([^)]*)\)", _compact(sql))
    assert match is not None
    return match.group(1).count("%s")


def test_empty_games_input_returns_empty_dict_without_sql():
    sr, cursor = _make_sr()

    assert fetch_plies_batch(sr, []) == {}

    sr.cursor.assert_not_called()
    cursor.execute.assert_not_called()


def test_single_game_uses_one_date_and_game_placeholder_and_sorts_rows():
    game_date = datetime.date(2026, 4, 18)
    rows = [
        {"game_id": "g1", "move_number": 2, "fen": "f2", "whose_moved": "black", "move": "e7e5"},
        {"game_id": "g1", "move_number": 1, "fen": "f1", "whose_moved": "white", "move": "e2e4"},
    ]
    sr, cursor = _make_sr(fetchall=rows)

    result = fetch_plies_batch(sr, [{"game_id": "g1", "date": game_date}])

    sql, params = cursor.execute.call_args.args
    assert _in_placeholder_count(sql, "date") == 1
    assert _in_placeholder_count(sql, "game_id") == 1
    assert params == (game_date, "g1")
    assert list(result) == ["g1"]
    assert [r["move_number"] for r in result["g1"]] == [1, 2]


def test_multiple_games_multiple_dates_buckets_rows_correctly():
    d1 = datetime.date(2026, 4, 18)
    d2 = datetime.date(2026, 4, 19)
    rows = [
        {"game_id": "g2", "move_number": 1, "fen": "g2f1", "whose_moved": "white", "move": "d2d4"},
        {"game_id": "g1", "move_number": 1, "fen": "g1f1", "whose_moved": "white", "move": "e2e4"},
        {"game_id": "g2", "move_number": 2, "fen": "g2f2", "whose_moved": "black", "move": "d7d5"},
    ]
    sr, cursor = _make_sr(fetchall=rows)

    result = fetch_plies_batch(
        sr,
        [
            {"game_id": "g1", "date": d1},
            {"game_id": "g2", "date": d2},
        ],
    )

    sql, params = cursor.execute.call_args.args
    assert _in_placeholder_count(sql, "date") == 2
    assert _in_placeholder_count(sql, "game_id") == 2
    assert params == (d1, d2, "g1", "g2")
    assert [r["fen"] for r in result["g1"]] == ["g1f1"]
    assert [r["fen"] for r in result["g2"]] == ["g2f1", "g2f2"]


def test_distinct_dates_are_deduped():
    game_date = datetime.date(2026, 4, 18)
    sr, cursor = _make_sr()

    fetch_plies_batch(
        sr,
        [
            {"game_id": "g1", "date": game_date},
            {"game_id": "g2", "date": game_date},
        ],
    )

    sql, params = cursor.execute.call_args.args
    assert _in_placeholder_count(sql, "date") == 1
    assert params == (game_date, "g1", "g2")


def test_sql_shape():
    sr, cursor = _make_sr()

    fetch_plies_batch(sr, [{"game_id": "g1", "date": datetime.date(2026, 4, 18)}])

    sql: str = cursor.execute.call_args.args[0]
    sql_compact = _compact(sql)
    assert "polaris_catalog.prod.chess_move_events" in sql_compact
    assert "WHERE date IN" in sql_compact
    assert "AND game_id IN" in sql_compact
    assert "GROUP BY game_id, move_number, fen, whose_moved, move" in sql_compact
    assert "ORDER BY game_id, move_number" in sql_compact


def test_missing_game_in_result_keeps_empty_bucket():
    game_date = datetime.date(2026, 4, 18)
    rows = [
        {"game_id": "g1", "move_number": 1, "fen": "f1", "whose_moved": "white", "move": "e2e4"},
    ]
    sr, cursor = _make_sr(fetchall=rows)

    result = fetch_plies_batch(
        sr,
        [
            {"game_id": "g1", "date": game_date},
            {"game_id": "g2", "date": game_date},
        ],
    )

    assert [r["move_number"] for r in result["g1"]] == [1]
    assert result["g2"] == []


def test_uses_dictionary_cursor():
    sr, cursor = _make_sr()

    fetch_plies_batch(sr, [{"game_id": "g1", "date": datetime.date(2026, 4, 18)}])

    sr.cursor.assert_called_with(dictionary=True)
