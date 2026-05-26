from unittest.mock import patch


def test_platform_overview_endpoint_returns_latest_meta(client):
    expected = {
        "date": "2026-05-25",
        "totals": {"games": 10, "player_game_rows": 20},
        "speed_mix": [],
        "top_openings": [],
        "phase_mistakes": [],
    }
    with patch("main.query_platform_overview", return_value=expected) as query:
        response = client.get("/api/platform/overview")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with()
