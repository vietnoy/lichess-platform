from unittest.mock import patch


def test_system_summary_endpoint_returns_table_health(client):
    expected = {
        "tables": [
            {
                "name": "player_games",
                "full_name": "polaris_catalog.prod.player_games",
                "rows": 42,
                "latest_date": "2026-05-25",
            }
        ],
        "totals": {"rows": 42, "tables": 1},
    }
    with patch("main.query_system_summary", return_value=expected) as query:
        response = client.get("/api/system/summary")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with()
