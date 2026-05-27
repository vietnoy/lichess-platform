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
    assert "inspect_student_style" in coach._TOOL_FNS


def test_inspect_student_style_combines_coaching_evidence(monkeypatch):
    monkeypatch.setattr(
        coach,
        "query_player_profile",
        lambda player_id, days=60: {
            "range": {"label": f"{days}d", "start_date": "2026-05-01", "end_date": "2026-05-25"},
            "totals": {"games": 10, "win_pct": 40.0, "avg_rating": 1800},
            "by_speed": [{"speed": "rapid", "total_games": 10}],
            "by_color": [{"color": "White", "games": 5, "win_pct": 60.0}],
            "vs_rating": [{"opponent": "Higher rated", "games": 3, "win_pct": 33.3}],
            "rating_history": [{"date": "2026-05-25", "avg_rating": 1800, "games": 2}],
            "recent_games": [{"game_id": "g1"}],
        },
    )
    monkeypatch.setattr(
        coach,
        "query_weakness_summary",
        lambda player_id, days=60: {"player_id": player_id, "top_phase": "middlegame", "critical_positions": 7},
    )
    monkeypatch.setattr(
        coach,
        "query_phase_stats",
        lambda player_id, days=60: [{"phase": "middlegame", "critical_positions": 7}],
    )
    monkeypatch.setattr(
        coach,
        "query_opening_stats",
        lambda player_id, days=60, top_n=8: [{"opening_eco": "B01", "critical_positions": 3}],
    )
    monkeypatch.setattr(
        coach,
        "query_blunder_examples",
        lambda player_id, limit=5, phase=None, time_pressure=None: [{"game_id": "g1", "phase": phase}],
    )

    result = coach.inspect_student_style("alice", days=600)

    assert result["days"] == 365
    assert result["profile"]["totals"]["games"] == 10
    assert result["weakness"]["top_phase"] == "middlegame"
    assert result["coach_brief"]["diagnosis"]
    assert result["coach_brief"]["drills"]
    assert result["critical_examples"] == [{"game_id": "g1", "phase": "middlegame"}]


def test_get_time_pressure_stats_binds_params_in_sql_order(monkeypatch):
    calls = []

    class Cursor:
        def execute(self, sql, params):
            calls.append((sql, params))

        def fetchall(self):
            if len(calls) == 1:
                import datetime as dt

                return [{"date": dt.date(2026, 5, 25)}]
            return []

    class CursorContext:
        def __enter__(self):
            return Cursor()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(coach.StarRocks, "cursor", lambda: CursorContext())

    coach.get_time_pressure_stats("alice")

    assert calls[1][1] == ("alice", "alice", "alice", "alice", "2026-05-25", "alice", "alice")


def test_gemini_final_answer_uses_tool_evidence(monkeypatch):
    captured = {}

    class Response:
        status_code = 200
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "candidates": [
                    {"content": {"parts": [{"text": "Chẩn đoán chính: dữ liệu đủ."}]}}
                ]
            }

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(coach, "GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(coach, "GEMINI_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(coach.requests, "post", fake_post)

    text = coach._gemini_final_answer([
        {"role": "system", "content": coach._SYSTEM_PROMPT},
        {"role": "user", "content": "Coach alice"},
        {"role": "tool", "name": "inspect_student_style", "content": '{"player_id":"alice"}'},
    ])

    assert text == "Chẩn đoán chính: dữ liệu đủ."
    assert captured["url"].endswith("/models/gemini-2.5-flash:generateContent")
    assert captured["headers"]["x-goog-api-key"] == "test-key"
    body_text = captured["json"]["contents"][0]["parts"][0]["text"]
    assert "Coach alice" in body_text
    assert "inspect_student_style" in body_text


def test_coach_system_prompt_is_vietnamese_and_action_oriented():
    assert "Trả lời bằng tiếng Việt" in coach._SYSTEM_PROMPT
    assert "inspect_student_style" in coach._SYSTEM_PROMPT
    assert "coach_brief" in coach._SYSTEM_PROMPT
    assert "Không nhắc tên tool" in coach._SYSTEM_PROMPT
    assert "Chẩn đoán" in coach._SYSTEM_PROMPT
    assert "Bài tập" in coach._SYSTEM_PROMPT
    assert "không được bịa số liệu" in coach._SYSTEM_PROMPT
