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
        profile = db._query_player_profile_uncached("alice", days=60)

    assert profile["totals"]["games"] == 1
    assert profile["clock_by_phase"] == []
    assert profile["range"]["label"] == "60d"
    assert profile["rating_history"] == [{"date": "2026-05-25", "avg_rating": 1800, "games": 1}]
    assert run.call_count == 1
    sql, params = run.call_args.args
    assert db.PLAYER_GAMES in sql
    assert db.TABLE not in sql
    assert "date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)" in sql
    assert params == ("alice", 60)


def test_query_player_profile_can_filter_custom_date():
    with patch("serving.backend.db._run", return_value=[_game()]) as run:
        profile = db._query_player_profile_uncached("alice", date="2026-05-25")

    assert profile["range"] == {"label": "date", "start_date": "2026-05-25", "end_date": "2026-05-25"}
    sql, params = run.call_args.args
    assert "date = DATE %s" in sql
    assert params == ("alice", "2026-05-25")
