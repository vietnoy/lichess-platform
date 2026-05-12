"""Tests for process_player in services/analyzer/worker.py."""

import datetime
from unittest.mock import MagicMock, call, patch

from services.analyzer.worker import process_player, BATCH_GAMES, THROTTLE_HOURS


def _make_pg() -> MagicMock:
    pg = MagicMock()
    return pg


# ---------------------------------------------------------------------------
# 1. Happy path: one game, two plies
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_happy_path_one_game_two_plies(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    game_date = datetime.date(2026, 4, 18)
    mock_fetch_games.return_value = [{"game_id": "g1", "date": game_date}]
    mock_fetch_plies.return_value = [
        {"move_number": 1, "fen": "f1", "whose_moved": "white", "move": "e2e4"},
        {"move_number": 2, "fen": "f2", "whose_moved": "black", "move": "e7e5"},
    ]
    mock_eval.side_effect = [
        {"cp": 10, "mate": None, "best_move": "d2d4"},
        {"cp": 20, "mate": None, "best_move": "g8f6"},
    ]

    pg = _make_pg()
    sr = MagicMock()

    result = process_player(pg, sr, "alice", None, None)

    assert result == 1

    # insert_evaluations called once with 2 rows
    mock_insert.assert_called_once()
    rows_arg = mock_insert.call_args.args[1]
    assert len(rows_arg) == 2

    # fetch_plies must receive the date for partition pruning on the 17M-row table.
    mock_fetch_plies.assert_called_once_with(sr, "g1", game_date)

    # fetch_player_games receives the cursor values + batch limit.
    mock_fetch_games.assert_called_once_with(sr, "alice", None, None, BATCH_GAMES)

    # update_cursor advances to the newest game in the batch
    mock_update_cursor.assert_called_once_with(
        pg, "alice", "g1", game_date,
        games_delta=1, throttle_hours=THROTTLE_HOURS,
    )

    # pg.commit called at least twice: after insert and after cursor update
    assert pg.commit.call_count >= 2


# ---------------------------------------------------------------------------
# 2. No games returned
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_no_games_returned(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    mock_fetch_games.return_value = []

    pg = _make_pg()
    sr = MagicMock()

    result = process_player(pg, sr, "alice", None, None)

    assert result == 0
    mock_insert.assert_not_called()
    mock_update_cursor.assert_called_once_with(
        pg, "alice", "", datetime.date(1900, 1, 1),
        games_delta=0, throttle_hours=THROTTLE_HOURS,
    )
    pg.commit.assert_called()


# ---------------------------------------------------------------------------
# 3. No games, existing cursor preserved
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_no_games_existing_cursor_preserved(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    mock_fetch_games.return_value = []
    last_date = datetime.date(2026, 4, 1)

    pg = _make_pg()
    sr = MagicMock()

    result = process_player(pg, sr, "bob", "prev", last_date)

    assert result == 0
    mock_update_cursor.assert_called_once_with(
        pg, "bob", "prev", last_date,
        games_delta=0, throttle_hours=THROTTLE_HOURS,
    )


# ---------------------------------------------------------------------------
# 4. Game with empty ply list is skipped; cursor still advances
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_empty_ply_list_skipped_cursor_advances(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    game_date = datetime.date(2026, 4, 18)
    mock_fetch_games.return_value = [{"game_id": "g1", "date": game_date}]
    mock_fetch_plies.return_value = []

    pg = _make_pg()
    sr = MagicMock()

    result = process_player(pg, sr, "carol", None, None)

    # insert not called because plies list was empty
    mock_insert.assert_not_called()
    # cursor still advances past the empty-ply game (len(games)==1)
    mock_update_cursor.assert_called_once_with(
        pg, "carol", "g1", game_date,
        games_delta=1, throttle_hours=THROTTLE_HOURS,
    )
    # return value counts games fetched, not rows written
    assert result == 1


# ---------------------------------------------------------------------------
# 5. eval returns None for one ply — no crash, rows still inserted
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_eval_none_for_one_ply_no_crash(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    game_date = datetime.date(2026, 4, 18)
    mock_fetch_games.return_value = [{"game_id": "g1", "date": game_date}]
    mock_fetch_plies.return_value = [
        {"move_number": 1, "fen": "f1", "whose_moved": "white", "move": "e2e4"},
        {"move_number": 2, "fen": "f2", "whose_moved": "black", "move": "e7e5"},
    ]
    # evals[0] is None; evals[1] has data
    mock_eval.side_effect = [None, {"cp": 10, "mate": None, "best_move": "d2d4"}]

    pg = _make_pg()
    sr = MagicMock()

    # must not raise
    result = process_player(pg, sr, "dave", None, None)

    assert result == 1
    mock_insert.assert_called_once()
    rows_arg = mock_insert.call_args.args[1]
    assert len(rows_arg) == 2
    # ply 0: cp_before is None so classification is None
    assert rows_arg[0][6] is None   # eval_cp
    assert rows_arg[0][9] is None   # classification
    # ply 1: cp_before=10, cp_after=None (no evals[2]) -> classification None
    assert rows_arg[1][6] == 10
    assert rows_arg[1][9] is None


# ---------------------------------------------------------------------------
# 6. Batch limit is forwarded to fetch_player_games
# ---------------------------------------------------------------------------


@patch("services.analyzer.worker.update_cursor")
@patch("services.analyzer.worker.insert_evaluations")
@patch("services.analyzer.worker.fetch_plies")
@patch("services.analyzer.worker.eval_with_cache")
@patch("services.analyzer.worker.fetch_player_games")
def test_batch_limit_honored(
    mock_fetch_games,
    mock_eval,
    mock_fetch_plies,
    mock_insert,
    mock_update_cursor,
):
    mock_fetch_games.return_value = []

    pg = _make_pg()
    sr = MagicMock()

    process_player(pg, sr, "eve", None, None)

    mock_fetch_games.assert_called_once()
    _, kwargs = mock_fetch_games.call_args
    # limit may be positional or keyword — check both
    all_args = mock_fetch_games.call_args.args
    assert all_args[-1] == BATCH_GAMES or kwargs.get("limit") == BATCH_GAMES
