"""Tests for services/analyzer/bootstrap.py."""

from unittest.mock import MagicMock, call, patch

from services.analyzer.bootstrap import fetch_eligible_player_ids, seed_analyzer_cursor


def _make_sr(fetchall=None):
    """Return a mock mysql.connector connection whose cursor(dictionary=True) works."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall if fetchall is not None else []

    sr = MagicMock()
    sr.cursor.return_value = cursor
    return sr, cursor


def _make_pg():
    """Return a mock psycopg2 connection whose cursor() context manager works."""
    cursor = MagicMock()

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=cursor)
    cm.__exit__ = MagicMock(return_value=False)

    pg = MagicMock()
    pg.cursor.return_value = cm
    return pg, cursor


# ---------------------------------------------------------------------------
# fetch_eligible_player_ids
# ---------------------------------------------------------------------------


def test_fetch_eligible_player_ids_returns_flat_list():
    sr, cursor = _make_sr(fetchall=[{"player_id": "alice"}, {"player_id": "bob"}])

    result = fetch_eligible_player_ids(sr)

    assert result == ["alice", "bob"]


def test_fetch_eligible_player_ids_sql_shape():
    sr, cursor = _make_sr()

    fetch_eligible_player_ids(sr)

    sql: str = cursor.execute.call_args.args[0]
    sql_lower = sql.lower()
    assert "polaris_catalog.prod.player_games" in sql_lower
    assert "not regexp" in sql_lower
    assert "bot|stockfish|maia|leela" in sql_lower
    assert "having count(*) >= 5" in sql_lower
    assert "max(date) >=" in sql_lower


# ---------------------------------------------------------------------------
# seed_analyzer_cursor
# ---------------------------------------------------------------------------


def test_seed_analyzer_cursor_calls_executemany():
    pg, cursor = _make_pg()

    seed_analyzer_cursor(pg, ["alice", "bob", "carol"])

    cursor.executemany.assert_called_once()
    call_args = cursor.executemany.call_args
    sql: str = call_args.args[0]
    assert "analyzer_cursor" in sql
    assert "ON CONFLICT (player_id) DO NOTHING" in sql
    pg.commit.assert_called_once()


def test_seed_analyzer_cursor_empty_list():
    pg, cursor = _make_pg()

    result = seed_analyzer_cursor(pg, [])

    cursor.executemany.assert_not_called()
    pg.commit.assert_not_called()
    assert result == 0


def test_seed_analyzer_cursor_return_value():
    pg, cursor = _make_pg()

    result = seed_analyzer_cursor(pg, ["alice", "bob", "carol"])

    assert result == 3


def test_seed_analyzer_cursor_batching():
    """Explicit batch slicing: 2500 ids with batch_size=1000 -> 3 executemany calls."""
    pg, cursor = _make_pg()
    player_ids = [f"player_{i}" for i in range(2500)]

    seed_analyzer_cursor(pg, player_ids, batch_size=1000)

    assert cursor.executemany.call_count == 3
    # Verify each batch has the right size: 1000, 1000, 500
    batch_sizes = [len(c.args[1]) for c in cursor.executemany.call_args_list]
    assert batch_sizes == [1000, 1000, 500]
