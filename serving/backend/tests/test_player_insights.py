from unittest.mock import patch

from serving.backend import db


def test_player_insights_endpoint_returns_ranked_insights(client):
    expected = {
        "player_id": "alice",
        "days": 60,
        "insights": [{"type": "phase_weakness", "score": 90}],
    }
    with patch("main.query_player_insights", return_value=expected) as query:
        response = client.get("/api/players/alice/insights?days=60")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with("alice", days=60, date=None, all_time=False)


def test_query_player_insights_scores_actionable_patterns(monkeypatch):
    monkeypatch.setattr(
        db,
        "query_weakness_summary",
        lambda username, days=60, date=None, all_time=False: {
            "player_id": username,
            "critical_positions": 30,
            "blunders": 8,
            "mistakes": 12,
            "avg_eval_swing_cp": 170,
            "time_pressure_positions": 10,
            "top_phase": "middlegame",
        },
    )
    monkeypatch.setattr(
        db,
        "query_phase_stats",
        lambda username, days=60, date=None, all_time=False: [
            {
                "phase": "middlegame",
                "critical_positions": 20,
                "blunders": 6,
                "mistakes": 8,
                "time_pressure_positions": 7,
                "avg_eval_swing_cp": 180,
            }
        ],
    )
    monkeypatch.setattr(
        db,
        "query_opening_stats",
        lambda username, days=60, top_n=10, date=None, all_time=False: [
            {
                "opening_eco": "B01",
                "opening_name": "Scandinavian Defense",
                "color": "black",
                "games": 12,
                "win_rate_pct": 33.3,
                "critical_positions": 9,
                "blunders": 3,
                "mistakes": 4,
            }
        ],
    )
    monkeypatch.setattr(
        db,
        "query_player_profile",
        lambda username, days=60, date=None, all_time=False: {
            "by_color": [
                {"color": "White", "games": 20, "win_pct": 60.0},
                {"color": "Black", "games": 20, "win_pct": 35.0},
            ],
            "vs_rating": [],
        },
    )

    result = db.query_player_insights("alice", days=60)

    assert result["player_id"] == "alice"
    assert result["insights"][0]["score"] >= result["insights"][-1]["score"]
    assert {item["type"] for item in result["insights"]} >= {
        "phase_weakness",
        "opening_leak",
        "color_gap",
        "time_pressure",
    }
    assert all(item["action"] for item in result["insights"])


def test_query_player_insights_uses_precomputed_cards(monkeypatch):
    db._query_cache.clear()

    def fake_run(sql, params=()):
        assert "player_insight_cards" in sql
        assert params == ("alice", 60, "alice", 60, 6)
        return [
            {
                "insight_type": "phase_weakness",
                "score": 92,
                "title": "Middlegame needs work",
                "evidence": "12 blunders in 60 days.",
                "action": "Train middlegame critical positions.",
                "data_json": '{"phase":"middlegame"}',
            }
        ]

    monkeypatch.setattr(db, "_run", fake_run)

    result = db.query_player_insights("alice", days=60)

    assert result == {
        "player_id": "alice",
        "days": 60,
        "insights": [
            {
                "type": "phase_weakness",
                "score": 92,
                "title": "Middlegame needs work",
                "evidence": "12 blunders in 60 days.",
                "action": "Train middlegame critical positions.",
                "data": {"phase": "middlegame"},
            }
        ],
    }


def test_query_player_insights_falls_back_when_precomputed_table_missing(monkeypatch):
    db._query_cache.clear()

    def missing_precomputed(sql, params=()):
        if "player_insight_cards" in sql:
            raise db.errors.ProgrammingError("missing table")
        return []

    monkeypatch.setattr(db, "_run", missing_precomputed)
    monkeypatch.setattr(
        db,
        "query_weakness_summary",
        lambda username, days=60, date=None, all_time=False: {
            "player_id": username,
            "critical_positions": 0,
            "time_pressure_positions": 0,
        },
    )
    monkeypatch.setattr(db, "query_phase_stats", lambda username, days=60, date=None, all_time=False: [])
    monkeypatch.setattr(db, "query_opening_stats", lambda username, days=60, top_n=10, date=None, all_time=False: [])
    monkeypatch.setattr(db, "query_player_profile", lambda username, days=60, date=None, all_time=False: {"by_color": []})

    result = db.query_player_insights("alice", days=60)

    assert result == {"player_id": "alice", "days": 60, "insights": []}
