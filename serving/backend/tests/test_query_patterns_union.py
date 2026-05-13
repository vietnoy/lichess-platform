from unittest.mock import patch

from serving.backend import db


def _game(game_id="game-1", username="alice", opponent="bob"):
    return {
        "game_id": game_id,
        "opening_name": "Queen's Pawn Game",
        "opening_eco": "D00",
        "white_id": username,
        "black_id": opponent,
        "date": "2026-05-12",
    }


def test_query_player_patterns_eval_query_unions_legacy_and_ondemand_tables():
    with patch("serving.backend.db._run", side_effect=[[_game()], []]) as run:
        result = db.query_player_patterns("alice")

    assert result is None
    sql, params = run.call_args_list[1].args
    assert db.EVAL_TABLE in sql
    assert db.EVAL_TABLE_ONDEMAND in sql
    # UNION (not UNION ALL) so games present in both tables don't double-count.
    assert "UNION" in sql
    assert "UNION ALL" not in sql
    assert "e.player_id = %s" in sql
    assert sql.count(db.TABLE) == 2
    assert len(params) == 5
    assert params[-1] == "alice"


def test_query_player_patterns_returns_none_when_no_games():
    with patch("serving.backend.db._run", return_value=[]) as run:
        result = db.query_player_patterns("alice")

    assert result is None
    assert run.call_count == 1


def test_query_player_patterns_returns_none_when_no_eval_rows_after_filtering():
    with patch("serving.backend.db._run", side_effect=[[_game()], []]):
        assert db.query_player_patterns("alice") is None


def test_query_player_patterns_drops_opponent_moves_via_post_filter():
    # Simulates the legacy half returning bob's plies in a game where alice
    # was white. The Python post-filter must drop them.
    rows = [
        {"game_id": "game-1", "ply": 7,  "classification": "blunder",
         "whose_moved": "white", "clock_remaining": 4200},  # alice
        {"game_id": "game-1", "ply": 8,  "classification": "blunder",
         "whose_moved": "black", "clock_remaining": 3900},  # bob — drop
    ]
    with patch("serving.backend.db._run", side_effect=[[_game()], rows]):
        result = db.query_player_patterns("alice")
    assert result["totals"]["blunders"] == 1, "bob's blunder must not be counted"


def test_query_player_patterns_happy_path_aggregates_user_rows():
    rows = [
        {
            "game_id": "game-1",
            "ply": 7,
            "classification": "blunder",
            "whose_moved": "white",
            "clock_remaining": 4200,
        },
        {
            "game_id": "game-1",
            "ply": 12,
            "classification": "mistake",
            "whose_moved": "white",
            "clock_remaining": 900,
        },
        {
            "game_id": "game-1",
            "ply": 18,
            "classification": "good",
            "whose_moved": "white",
            "clock_remaining": 2400,
        },
    ]
    with patch("serving.backend.db._run", side_effect=[[_game()], rows]):
        result = db.query_player_patterns("alice")

    assert result["totals"]["blunders"] == 1
    assert result["totals"]["mistakes"] == 1
    assert result["totals"]["inaccuracies"] == 0
