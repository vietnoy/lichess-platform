from unittest.mock import patch

from serving.backend import db


def test_query_exercise_uses_critical_positions_table():
    with patch("serving.backend.db._run", return_value=[]) as run:
        result = db.query_exercise("alice")

    assert result is None
    sql, params = run.call_args.args
    assert db.CRITICAL_POSITIONS in sql
    assert db.EVAL_TABLE not in sql
    assert db.EVAL_TABLE_ONDEMAND not in sql
    assert db.TABLE not in sql
    assert "player_id = %s" in sql
    assert "classification IN ('blunder', 'mistake')" in sql
    assert params == ("alice",)


def test_query_exercise_returns_normalized_eval_swing_cp():
    row = {
        "game_id": "game-1",
        "ply": 17,
        "fen": "8/8/8/8/8/8/8/8 w - - 0 1",
        "played_move": "e2e4",
        "best_move": "d2d4",
        "eval_cp": -120,
        "eval_swing_cp": 340,
        "classification": "blunder",
        "date": "2026-05-12",
        "opening_name": "Queen's Pawn Game",
        "opening_eco": "D00",
        "speed": "rapid",
        "clock_remaining": 1234,
    }
    with (
        patch("serving.backend.db._run", return_value=[row]),
        patch("serving.backend.db.random.choice", side_effect=lambda candidates: candidates[0]),
    ):
        result = db.query_exercise("alice")

    assert result["eval_swing_cp"] == 340
    assert result["clock_remaining_s"] == 12.3
    assert "eval_swing_cp_from_prev" not in result


def test_query_exercise_returns_none_when_no_candidates():
    with patch("serving.backend.db._run", return_value=[]):
        assert db.query_exercise("alice") is None
