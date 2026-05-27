from unittest.mock import patch


def test_platform_overview_endpoint_returns_latest_meta(client):
    expected = {
        "date": "2026-05-25",
        "start_date": "2026-04-26",
        "end_date": "2026-05-25",
        "range": "30d",
        "totals": {"games": 10, "player_game_rows": 20},
        "speed_mix": [],
        "top_openings": [],
        "phase_mistakes": [],
    }
    with patch("main.query_platform_overview", return_value=expected) as query:
        response = client.get("/api/platform/overview")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with(days=30, date=None, all_time=False)


def test_platform_overview_endpoint_accepts_range_filters(client):
    expected = {
        "date": "2026-05-10",
        "start_date": "2026-05-10",
        "end_date": "2026-05-10",
        "range": "date",
        "totals": {"games": 1, "player_game_rows": 2},
        "speed_mix": [],
        "top_openings": [],
        "phase_mistakes": [],
    }
    with patch("main.query_platform_overview", return_value=expected) as query:
        response = client.get("/api/platform/overview?days=60&date=2026-05-10&all_time=true")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with(days=60, date="2026-05-10", all_time=True)
