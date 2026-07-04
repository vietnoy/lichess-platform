from unittest.mock import patch

from serving.backend import db
from serving.backend import coach


def test_query_weakness_summary_aggregates_daily_summary_with_bound_params():
    db._query_cache.clear()
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
    with patch("serving.backend.db._run", side_effect=[[{"date": "2026-05-26"}], rows]) as run:
        result = db.query_weakness_summary("alice", days=600)

    sql, params = run.call_args_list[1].args
    assert db.PLAYER_WEAKNESS_SUMMARY in sql
    assert "%s" in sql
    assert "player_id = %s" in sql
    assert "date BETWEEN DATE %s AND DATE %s" in sql
    assert params == ("alice", "2025-05-27", "2026-05-26")
    assert result == rows[0]


def test_query_weakness_summary_returns_empty_shape_when_no_rows():
    with patch("serving.backend.db._run", return_value=[]):
        result = db.query_weakness_summary("alice")

    assert result["player_id"] == "alice"
    assert result["critical_positions"] == 0
    assert result["top_phase"] is None


def test_query_weakness_summary_supports_single_date_filter():
    with patch("serving.backend.db._run", return_value=[]) as run:
        db.query_weakness_summary("alice", date="2026-05-26")

    sql, params = run.call_args.args
    assert "date = DATE %s" in sql
    assert "DATE_SUB" not in sql
    assert params == ("alice", "2026-05-26")


def test_query_phase_stats_supports_all_time_filter():
    with patch("serving.backend.db._run", return_value=[]) as run:
        db.query_phase_stats("alice", all_time=True)

    sql, params = run.call_args.args
    assert "DATE_SUB" not in sql
    assert "date = DATE %s" not in sql
    assert params == ("alice",)


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
    db._query_cache.clear()
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
    with patch("serving.backend.db._run", side_effect=[[{"date": "2026-05-26"}], rows]) as run:
        result = db.query_opening_stats("alice", days=600, top_n=100)

    sql, params = run.call_args_list[1].args
    assert db.PLAYER_OPENING_STATS in sql
    assert db.PLAYER_GAMES not in sql
    assert "player_id = %s" in sql
    assert "date BETWEEN DATE %s AND DATE %s" in sql
    assert "HAVING SUM(games) >= 2" in sql
    assert "HAVING games >= 2" not in sql
    assert "ORDER BY blunders DESC, mistakes DESC, critical_positions DESC, games DESC" in sql
    assert params == ("alice", "2025-05-27", "2026-05-26", 20)
    assert result == rows


def test_query_phase_stats_uses_derived_table_and_clamps_days():
    db._query_cache.clear()
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
    with patch("serving.backend.db._run", side_effect=[[{"date": "2026-05-26"}], rows]) as run:
        result = db.query_phase_stats("alice", days=600)

    sql, params = run.call_args_list[1].args
    assert db.PLAYER_PHASE_STATS in sql
    assert "player_id = %s" in sql
    assert "date BETWEEN DATE %s AND DATE %s" in sql
    assert "ORDER BY critical_positions DESC, blunders DESC" in sql
    assert params == ("alice", "2025-05-27", "2026-05-26")
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


def test_coach_exposes_vertex_function_declarations_for_all_tools():
    declared = {tool["name"] for tool in coach._TOOL_DECLARATIONS}

    assert declared == set(coach._TOOL_FNS)
    assert "inspect_student_style" in declared
    assert "analyze_game" in declared
    opening_tool = next(tool for tool in coach._TOOL_DECLARATIONS if tool["name"] == "get_opening_stats")
    assert "player_id" in opening_tool["parameters"]["required"]
    assert "days" in opening_tool["parameters"]["properties"]


def test_vertex_agent_loop_executes_model_selected_tool(monkeypatch):
    calls = []

    responses = [
        {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "get_opening_stats",
                                    "args": {"player_id": "alice", "days": 30, "top_n": 5},
                                }
                            }
                        ],
                    }
                }
            ]
        },
        {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [{"text": "Alice cần ổn định repertoire khai cuộc."}],
                    }
                }
            ]
        },
    ]

    def fake_generate(payload, timeout=45):
        calls.append(payload)
        return responses.pop(0)

    monkeypatch.setattr(coach, "_vertex_generate_content_payload", fake_generate)
    monkeypatch.setattr(
        coach,
        "get_opening_stats",
        lambda player_id, top_n=10, days=60: [{"player_id": player_id, "days": days, "top_n": top_n}],
    )

    session = coach.CoachSession()
    events = list(session._run_vertex_agent("Hãy phân tích opening của [Player: alice] trong 30 ngày"))

    assert events[0]["type"] == "tool_start"
    assert events[0]["name"] == "get_opening_stats"
    assert events[1]["type"] == "tool_result"
    assert events[-2] == {"type": "token", "text": "Alice cần ổn định repertoire khai cuộc."}
    assert events[-1] == {"type": "done"}
    assert calls[0]["tools"][0]["functionDeclarations"]
    assert calls[0]["toolConfig"]["functionCallingConfig"]["mode"] == "AUTO"
    assert "allowedFunctionNames" not in calls[0]["toolConfig"]["functionCallingConfig"]
    second_contents = calls[1]["contents"]
    assert any("functionResponse" in part for content in second_contents for part in content.get("parts", []))


def test_vertex_agent_loop_groups_multiple_function_responses(monkeypatch):
    calls = []
    responses = [
        {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "get_time_pressure_stats",
                                    "args": {"player_id": "alice", "days": 30},
                                }
                            },
                            {
                                "functionCall": {
                                    "name": "get_performance_by_color",
                                    "args": {"player_id": "alice"},
                                }
                            },
                        ],
                    }
                }
            ]
        },
        {
            "candidates": [
                {"content": {"role": "model", "parts": [{"text": "Alice cần tập trung khi cầm Đen."}]}}
            ]
        },
    ]

    def fake_generate(payload, timeout=45):
        calls.append(payload)
        return responses.pop(0)

    monkeypatch.setattr(coach, "_vertex_generate_content_payload", fake_generate)
    monkeypatch.setattr(
        coach,
        "get_time_pressure_stats",
        lambda player_id, days=60: {"player_id": player_id, "days": days},
    )
    monkeypatch.setattr(
        coach,
        "get_performance_by_color",
        lambda player_id: {"player_id": player_id, "by_color": []},
    )

    events = list(coach.CoachSession()._run_vertex_agent("Coach [Player: alice]"))

    assert [event["name"] for event in events if event["type"] == "tool_start"] == [
        "get_time_pressure_stats",
        "get_performance_by_color",
    ]
    response_contents = calls[1]["contents"]
    response_messages = [
        content for content in response_contents
        if any("functionResponse" in part for part in content.get("parts", []))
    ]
    assert len(response_messages) == 1
    assert len(response_messages[0]["parts"]) == 2
    assert all("functionResponse" in part for part in response_messages[0]["parts"])


def test_vertex_agent_includes_recent_chat_context_for_followups(monkeypatch):
    calls = []
    responses = [
        {
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            {
                                "functionCall": {
                                    "name": "analyze_game",
                                    "args": {"game_id": "8IzoF2q3"},
                                }
                            }
                        ],
                    }
                }
            ]
        },
        {"candidates": [{"content": {"role": "model", "parts": [{"text": "Đây là phân tích cụ thể."}]}}]},
    ]

    def fake_generate(payload, timeout=45):
        calls.append(payload)
        return responses.pop(0)

    monkeypatch.setattr(coach, "_vertex_generate_content_payload", fake_generate)
    monkeypatch.setattr(coach, "analyze_game", lambda game_id: {"game_id": game_id, "moves": []})

    session = coach.CoachSession()
    session.messages.extend(
        [
            {"role": "user", "content": "[Player: benirks] bạn thấy tôi có phong cách chơi như nào?"},
            {
                "role": "assistant",
                "content": "Bạn có muốn tôi đi sâu hơn vào game 8IzoF2q3 trong Philidor Defense không?",
            },
            {"role": "user", "content": "okay phân tích cụ thể thử đi xem nào"},
        ]
    )

    events = list(session._run_vertex_agent("okay phân tích cụ thể thử đi xem nào"))

    first_prompt = calls[0]["contents"][0]["parts"][0]["text"]
    assert "Bối cảnh hội thoại gần đây" in first_prompt
    assert "8IzoF2q3" in first_prompt
    assert "okay phân tích cụ thể" in first_prompt
    assert events[0]["name"] == "analyze_game"


def test_analyze_game_uses_stored_evaluations_without_live_stockfish(monkeypatch):
    monkeypatch.setattr(
        coach,
        "query_game",
        lambda game_id: [
            {
                "move_number": 1,
                "whose_moved": "white",
                "move": "e2e4",
                "clock_s": 180.0,
                "white_id": "alice",
                "black_id": "bob",
                "white_rating": 1500,
                "black_rating": 1510,
                "opening_name": "King's Pawn Game",
                "speed": "blitz",
                "winner": "white",
                "end_status": "mate",
            },
            {
                "move_number": 2,
                "whose_moved": "black",
                "move": "e7e5",
                "clock_s": 179.0,
                "white_id": "alice",
                "black_id": "bob",
                "white_rating": 1500,
                "black_rating": 1510,
                "opening_name": "King's Pawn Game",
                "speed": "blitz",
                "winner": "white",
                "end_status": "mate",
            },
        ],
    )
    monkeypatch.setattr(
        coach,
        "query_game_evaluations",
        lambda game_id: [
            {
                "ply": 1,
                "played_move": "e2e4",
                "best_move": "e2e4",
                "eval_cp": 20,
                "mate": None,
                "eval_swing_cp_from_prev": 0,
                "classification": "good",
            },
            {
                "ply": 2,
                "played_move": "e7e5",
                "best_move": "c7c5",
                "eval_cp": 80,
                "mate": None,
                "eval_swing_cp_from_prev": 120,
                "classification": "mistake",
            },
        ],
    )

    result = coach.analyze_game("game1234")

    assert result["evaluated_positions"] == 2
    assert result["moves"][1]["classification"] == "mistake"
    assert result["moves"][1]["best_move"] == "c7c5"
    assert result["mistakes"] == 1


def test_get_time_pressure_stats_uses_aggregate_tables(monkeypatch):
    monkeypatch.setattr(
        coach,
        "query_weakness_summary",
        lambda player_id, days=60: {
            "player_id": player_id,
            "critical_positions": 10,
            "time_pressure_positions": 4,
            "top_time_pressure": "under_30s",
        },
    )
    monkeypatch.setattr(
        coach,
        "query_phase_stats",
        lambda player_id, days=60: [
            {"phase": "middlegame", "critical_positions": 8, "time_pressure_positions": 3}
        ],
    )

    result = coach.get_time_pressure_stats("alice", days=600)

    assert result["days"] == 365
    assert result["time_pressure"]["share_pct"] == 40.0
    assert result["by_phase"][0]["share_pct"] == 37.5


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


def test_vertex_final_answer_uses_adc_token_and_project(monkeypatch):
    captured = {}

    class Response:
        status_code = 200
        text = ""

        def json(self):
            return {
                "candidates": [
                    {"content": {"parts": [{"text": "Kế hoạch: tập trung endgame."}]}}
                ]
            }

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(coach, "GCP_PROJECT", "chess-platform-497604")
    monkeypatch.setattr(coach, "GCP_LOCATION", "us-central1")
    monkeypatch.setattr(coach, "VERTEX_MODEL", "gemini-2.5-flash")
    monkeypatch.setattr(coach, "_vertex_access_token", lambda: "access-token")
    monkeypatch.setattr(coach.requests, "post", fake_post)

    text = coach._vertex_final_answer([
        {"role": "system", "content": coach._SYSTEM_PROMPT},
        {"role": "user", "content": "Coach alice"},
        {"role": "tool", "name": "inspect_student_style", "content": '{"player_id":"alice"}'},
    ])

    assert text == "Kế hoạch: tập trung endgame."
    assert captured["url"].endswith(
        "/projects/chess-platform-497604/locations/us-central1/publishers/google/models/gemini-2.5-flash:generateContent"
    )
    assert captured["headers"]["Authorization"] == "Bearer access-token"
    body_text = captured["json"]["contents"][0]["parts"][0]["text"]
    assert "Coach alice" in body_text
    assert "inspect_student_style" in body_text


def test_coach_system_prompt_is_vietnamese_and_action_oriented():
    assert "Trả lời bằng tiếng Việt" in coach._SYSTEM_PROMPT
    assert "coach_brief" in coach._SYSTEM_PROMPT
    assert "Không nhắc tên tool" in coach._SYSTEM_PROMPT
    assert "username hoặc game id" in coach._SYSTEM_PROMPT
    assert "Chẩn đoán" in coach._SYSTEM_PROMPT
    assert "Bài tập tiếp theo" not in coach._SYSTEM_PROMPT
    assert "Câu trả lời ngắn" not in coach._SYSTEM_PROMPT
    assert "không được bịa số liệu" in coach._SYSTEM_PROMPT
