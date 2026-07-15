from unittest.mock import patch


def test_patterns_endpoint_forwards_date_range_options(client):
    expected = {"username": "alice", "totals": {"games_analyzed": 3}}
    with patch("main.query_player_patterns", return_value=expected) as query:
        response = client.get("/api/players/alice/patterns?days=30")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with("alice", days=30, date=None, all_time=False)


def test_patterns_endpoint_forwards_all_time_filter(client):
    expected = {"username": "alice", "totals": {"games_analyzed": 3}}
    with patch("main.query_player_patterns", return_value=expected) as query:
        response = client.get("/api/players/alice/patterns?all_time=true")

    assert response.status_code == 200
    query.assert_called_once_with("alice", days=60, date=None, all_time=True)


def test_weakness_summary_endpoint_uses_aggregate_query(client):
    expected = {
        "player_id": "alice",
        "critical_positions": 7,
        "blunders": 2,
    }
    with patch("main.query_weakness_summary", return_value=expected) as query:
        response = client.get("/api/players/alice/weakness-summary?days=90")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with("alice", days=90, date=None, all_time=False)


def test_weakness_summary_endpoint_forwards_date_filter(client):
    with patch("main.query_weakness_summary", return_value={"player_id": "alice"}) as query:
        response = client.get("/api/players/alice/weakness-summary?date=2026-05-26")

    assert response.status_code == 200
    query.assert_called_once_with("alice", days=60, date="2026-05-26", all_time=False)


def test_opening_stats_endpoint_uses_aggregate_query(client):
    rows = [
        {
            "opening_eco": "B01",
            "opening_name": "Scandinavian Defense",
            "color": "black",
            "games": 12,
            "critical_positions": 9,
        }
    ]
    with patch("main.query_opening_stats", return_value=rows) as query:
        response = client.get("/api/players/alice/opening-stats?days=30&top_n=5")

    assert response.status_code == 200
    assert response.json() == {"player_id": "alice", "opening_stats": rows}
    query.assert_called_once_with("alice", days=30, top_n=5, date=None, all_time=False)


def test_opening_stats_endpoint_forwards_all_time_filter(client):
    with patch("main.query_opening_stats", return_value=[]) as query:
        response = client.get("/api/players/alice/opening-stats?all_time=true&top_n=5")

    assert response.status_code == 200
    query.assert_called_once_with("alice", days=60, top_n=5, date=None, all_time=True)


def test_phase_stats_endpoint_uses_aggregate_query(client):
    rows = [
        {
            "phase": "middlegame",
            "critical_positions": 17,
            "blunders": 5,
        }
    ]
    with patch("main.query_phase_stats", return_value=rows) as query:
        response = client.get("/api/players/alice/phase-stats?days=14")

    assert response.status_code == 200
    assert response.json() == {"player_id": "alice", "phase_stats": rows}
    query.assert_called_once_with("alice", days=14, date=None, all_time=False)
