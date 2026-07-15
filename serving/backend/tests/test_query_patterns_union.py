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


def test_query_player_patterns_uses_critical_positions_table():
    with patch("serving.backend.db._run", side_effect=[[{"date": "2026-05-12"}], []]) as run:
        result = db.query_player_patterns("alice")

    assert result is None
    sql, params = run.call_args_list[1].args
    assert db.CRITICAL_POSITIONS in sql
    assert db.EVAL_TABLE not in sql
    assert db.EVAL_TABLE_ONDEMAND not in sql
    assert db.TABLE not in sql
    assert "player_id = %s" in sql
    assert "date BETWEEN DATE %s AND DATE %s" in sql
    assert params == ("alice", "2026-03-14", "2026-05-12")


def test_query_player_patterns_returns_none_when_no_games():
    with patch("serving.backend.db._run", side_effect=[[{"date": None}], []]) as run:
        result = db.query_player_patterns("alice")

    assert result is None
    assert run.call_count == 2


def test_query_player_patterns_supports_single_date_filter():
    with patch("serving.backend.db._run", return_value=[]) as run:
        result = db.query_player_patterns("alice", date="2026-05-12")

    assert result is None
    sql, params = run.call_args.args
    assert "date = DATE %s" in sql
    assert params == ("alice", "2026-05-12")


def test_query_player_patterns_supports_all_time_filter():
    with patch("serving.backend.db._run", return_value=[]) as run:
        result = db.query_player_patterns("alice", all_time=True)

    assert result is None
    sql, params = run.call_args.args
    assert "date BETWEEN" not in sql
    assert "date = DATE" not in sql
    assert params == ("alice",)


def test_query_player_patterns_happy_path_aggregates_user_rows():
    rows = [
        {
            "game_id": "game-1",
            "ply": 7,
            "classification": "blunder",
            "clock_remaining": 4200,
            "opening_eco": "D00",
            "opening_name": "Queen's Pawn Game",
            "opponent_id": "bob",
            "date": "2026-05-12",
        },
        {
            "game_id": "game-1",
            "ply": 12,
            "classification": "mistake",
            "clock_remaining": 900,
            "opening_eco": "D00",
            "opening_name": "Queen's Pawn Game",
            "opponent_id": "bob",
            "date": "2026-05-12",
        },
        {
            "game_id": "game-1",
            "ply": 18,
            "classification": "inaccuracy",
            "clock_remaining": 2400,
            "opening_eco": "D00",
            "opening_name": "Queen's Pawn Game",
            "opponent_id": "bob",
            "date": "2026-05-12",
        },
    ]
    with patch("serving.backend.db._run", side_effect=[[{"date": "2026-05-12"}], rows]):
        result = db.query_player_patterns("alice")

    assert result["totals"]["blunders"] == 1
    assert result["totals"]["mistakes"] == 1
    assert result["totals"]["inaccuracies"] == 1
    assert result["worst_games"][0]["opponent"] == "bob"
