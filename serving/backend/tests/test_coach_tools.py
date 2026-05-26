from unittest.mock import patch

from serving.backend import db
from serving.backend import coach


def test_query_weakness_summary_aggregates_daily_summary_with_bound_params():
    rows = [{
        "player_id": "alice",
        "days": 60,
        "critical_positions": 7,
        "games_with_critical_positions": 3,
        "blunders": 2,
        "mistakes": 4,
        "inaccuracies": 1,
        "avg_eval_swing_cp": 142.5,
        "time_pressure_positions": 2,
        "top_phase": "middlegame",
        "top_time_pressure": "normal",
        "top_classification": "mistake",
    }]
    with patch("serving.backend.db._run", return_value=rows) as run:
        result = db.query_weakness_summary("alice", days=600)

    sql, params = run.call_args.args
    assert db.PLAYER_WEAKNESS_SUMMARY in sql
    assert "%s" in sql
    assert "player_id = %s" in sql
    assert params == ("alice", 365)
    assert result == rows[0]


def test_query_weakness_summary_returns_empty_shape_when_no_rows():
    with patch("serving.backend.db._run", return_value=[]):
        result = db.query_weakness_summary("alice")

    assert result["player_id"] == "alice"
    assert result["critical_positions"] == 0
    assert result["top_phase"] is None


def test_query_blunder_examples_uses_allowlisted_filters_and_clamped_limit():
    with patch("serving.backend.db._run", return_value=[]) as run:
        db.query_blunder_examples(
            "alice",
            limit=100,
            phase="endgame",
            time_pressure="under_10s",
        )

    sql, params = run.call_args.args
    assert db.CRITICAL_POSITIONS in sql
    assert "classification IN ('blunder', 'mistake')" in sql
    assert "phase = %s" in sql
    assert "time_pressure = %s" in sql
    assert params == ("alice", "endgame", "under_10s", 20)


def test_query_blunder_examples_rejects_unknown_filter_values():
    with patch("serving.backend.db._run") as run:
        result = db.query_blunder_examples("alice", phase="DROP TABLE", time_pressure="fast")

    assert result == []
    run.assert_not_called()


def test_query_opening_stats_uses_derived_table_and_clamps_inputs():
    rows = [{
        "opening_eco": "B01",
        "opening_name": "Scandinavian Defense",
        "color": "black",
        "games": 12,
        "wins": 4,
        "losses": 7,
        "draws": 1,
        "win_rate_pct": 33.3,
        "critical_positions": 9,
        "blunders": 3,
        "mistakes": 4,
        "avg_eval_swing_cp": 184.0,
    }]
    with patch("serving.backend.db._run", return_value=rows) as run:
        result = db.query_opening_stats("alice", days=600, top_n=100)

    sql, params = run.call_args.args
    assert db.PLAYER_OPENING_STATS in sql
    assert db.PLAYER_GAMES not in sql
    assert "player_id = %s" in sql
    assert "date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)" in sql
    assert "HAVING SUM(games) >= 2" in sql
    assert "HAVING games >= 2" not in sql
    assert "ORDER BY blunders DESC, mistakes DESC, critical_positions DESC, games DESC" in sql
    assert params == ("alice", 365, 20)
    assert result == rows


def test_query_phase_stats_uses_derived_table_and_clamps_days():
    rows = [{
        "phase": "middlegame",
        "games_with_positions": 8,
        "critical_positions": 17,
        "blunders": 5,
        "mistakes": 7,
        "inaccuracies": 5,
        "time_pressure_positions": 4,
        "avg_eval_swing_cp": 151.2,
    }]
    with patch("serving.backend.db._run", return_value=rows) as run:
        result = db.query_phase_stats("alice", days=600)

    sql, params = run.call_args.args
    assert db.PLAYER_PHASE_STATS in sql
    assert "player_id = %s" in sql
    assert "date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)" in sql
    assert "ORDER BY critical_positions DESC, blunders DESC" in sql
    assert params == ("alice", 365)
    assert result == rows


def test_coach_dispatch_exposes_new_safe_tools(monkeypatch):
    monkeypatch.setattr(
        coach,
        "get_weakness_summary",
        lambda player_id, days=60: {"player_id": player_id, "days": days},
    )
    session = coach.CoachSession()

    result = session._dispatch("get_weakness_summary", {"player_id": "alice", "days": 30})

    assert '"player_id": "alice"' in result
    assert '"days": 30' in result
    assert "get_weakness_summary" in coach._TOOL_FNS
    assert "get_blunder_examples" in coach._TOOL_FNS


def test_coach_system_prompt_is_vietnamese_and_action_oriented():
    assert "Trả lời bằng tiếng Việt" in coach._SYSTEM_PROMPT
    assert "Chẩn đoán" in coach._SYSTEM_PROMPT
    assert "Bài tập" in coach._SYSTEM_PROMPT
    assert "không được bịa số liệu" in coach._SYSTEM_PROMPT
