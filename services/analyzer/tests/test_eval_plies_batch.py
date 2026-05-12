"""Tests for batched position evaluation in services/analyzer/worker.py."""

from unittest.mock import MagicMock, patch

from services.analyzer.worker import eval_plies_batch


def _make_pg(fetchall=None):
    """Return a mock psycopg2 connection whose cursor() context manager works."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall if fetchall is not None else []

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=cursor)
    cm.__exit__ = MagicMock(return_value=False)

    pg = MagicMock()
    pg.cursor.return_value = cm
    return pg, cursor


def _plies(*fens: str) -> list[dict]:
    return [{"fen": fen} for fen in fens]


def _eval_for_fen(fen: str, depth: int | None = None) -> dict:
    values = {
        "f1": 10,
        "f2": 20,
        "f3": 30,
        "f4": 40,
        "f5": 50,
    }
    return {"cp": values[fen], "mate": None, "best_move": f"{fen}-best"}


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_all_hits(mock_eval_fen):
    rows = [
        ("f1", 10, None, "m1"),
        ("f2", 20, None, "m2"),
        ("f3", 30, None, "m3"),
        ("f4", 40, None, "m4"),
        ("f5", 50, None, "m5"),
    ]
    pg, cursor = _make_pg(fetchall=rows)

    result = eval_plies_batch(pg, _plies("f1", "f2", "f3", "f4", "f5"))

    assert result == [
        {"cp": 10, "mate": None, "best_move": "m1"},
        {"cp": 20, "mate": None, "best_move": "m2"},
        {"cp": 30, "mate": None, "best_move": "m3"},
        {"cp": 40, "mate": None, "best_move": "m4"},
        {"cp": 50, "mate": None, "best_move": "m5"},
    ]
    mock_eval_fen.assert_not_called()
    cursor.executemany.assert_not_called()


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_all_misses(mock_eval_fen):
    mock_eval_fen.side_effect = _eval_for_fen
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, _plies("f1", "f2", "f3", "f4", "f5"))

    assert [r["cp"] for r in result if r is not None] == [10, 20, 30, 40, 50]
    assert mock_eval_fen.call_count == 5
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 5


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_mixed_hits_and_misses(mock_eval_fen):
    mock_eval_fen.side_effect = _eval_for_fen
    pg, cursor = _make_pg(fetchall=[
        ("f1", 10, None, "m1"),
        ("f3", 30, None, "m3"),
    ])

    result = eval_plies_batch(pg, _plies("f1", "f2", "f3", "f4", "f5"))

    assert result == [
        {"cp": 10, "mate": None, "best_move": "m1"},
        {"cp": 20, "mate": None, "best_move": "f2-best"},
        {"cp": 30, "mate": None, "best_move": "m3"},
        {"cp": 40, "mate": None, "best_move": "f4-best"},
        {"cp": 50, "mate": None, "best_move": "f5-best"},
    ]
    assert mock_eval_fen.call_count == 3
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 3


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_duplicate_fens_dedupes_stockfish_calls(mock_eval_fen):
    mock_eval_fen.side_effect = _eval_for_fen
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, _plies("f1", "f2", "f1", "f3", "f2"))

    assert mock_eval_fen.call_count == 3
    assert len(result) == 5
    assert result[0] == result[2]
    assert result[1] == result[4]
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 3


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_stockfish_none_skips_insert_for_failed_fen(mock_eval_fen):
    def eval_or_none(fen: str, depth: int | None = None) -> dict | None:
        if fen == "f3":
            return None
        return _eval_for_fen(fen, depth)

    mock_eval_fen.side_effect = eval_or_none
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, _plies("f1", "f2", "f3", "f4", "f5"))

    assert result[2] is None
    assert result[0] == {"cp": 10, "mate": None, "best_move": "f1-best"}
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 4
    assert all(row[0] != "f3" for row in insert_rows)


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_stockfish_poisoned_response_is_none_and_not_cached(mock_eval_fen):
    def eval_or_poison(fen: str, depth: int | None = None) -> dict | None:
        if fen == "f2":
            return {"cp": None, "mate": None, "best_move": None}
        return _eval_for_fen(fen, depth)

    mock_eval_fen.side_effect = eval_or_poison
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, _plies("f1", "f2", "f3"))

    assert result[1] is None
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert len(insert_rows) == 2
    assert all(row[0] != "f2" for row in insert_rows)


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_poisoned_cached_row_is_treated_as_miss(mock_eval_fen):
    mock_eval_fen.return_value = {"cp": 25, "mate": None, "best_move": "fresh"}
    pg, cursor = _make_pg(fetchall=[("f1", None, None, None)])

    result = eval_plies_batch(pg, _plies("f1"))

    assert result == [{"cp": 25, "mate": None, "best_move": "fresh"}]
    mock_eval_fen.assert_called_once_with("f1", depth=12)
    cursor.executemany.assert_called_once()
    insert_rows = cursor.executemany.call_args.args[1]
    assert insert_rows == [("f1", 25, None, "fresh", 12)]


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_empty_plies_does_no_io(mock_eval_fen):
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, [])

    assert result == []
    pg.cursor.assert_not_called()
    cursor.execute.assert_not_called()
    cursor.executemany.assert_not_called()
    mock_eval_fen.assert_not_called()


@patch("services.analyzer.worker.eval_fen")
def test_eval_plies_batch_preserves_input_order(mock_eval_fen):
    mock_eval_fen.side_effect = _eval_for_fen
    pg, cursor = _make_pg(fetchall=[])

    result = eval_plies_batch(pg, _plies("f5", "f2", "f4", "f1", "f3"))

    assert [r["cp"] for r in result if r is not None] == [50, 20, 40, 10, 30]
    assert result[0] == {"cp": 50, "mate": None, "best_move": "f5-best"}
    assert result[3] == {"cp": 10, "mate": None, "best_move": "f1-best"}
    cursor.executemany.assert_called_once()
