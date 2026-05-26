from unittest.mock import patch

from serving.backend import db


def _game(**overrides):
    row = {
        "game_id": "game-1",
        "color": "white",
        "opponent_id": "bob",
        "my_rating": 1800,
        "opp_rating": 1750,
        "speed": "rapid",
        "opening_eco": "D00",
        "opening_name": "Queen's Pawn Game",
        "winner": "white",
        "end_status": "resign",
        "date": "2026-05-25",
    }
    row.update(overrides)
    return row


def test_query_player_profile_avoids_raw_move_event_scan():
    with patch("serving.backend.db._run", return_value=[_game()]) as run:
        profile = db._query_player_profile_uncached("alice")

    assert profile["totals"]["games"] == 1
    assert profile["clock_by_phase"] == []
    assert run.call_count == 1
    sql, params = run.call_args.args
    assert db.PLAYER_GAMES in sql
    assert db.TABLE not in sql
    assert params == ("alice",)
