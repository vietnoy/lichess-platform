from unittest.mock import patch

import main


def test_system_summary_endpoint_returns_table_health(client):
    expected = {
        "tables": [
            {
                "name": "player_games",
                "full_name": "polaris_catalog.prod.player_games",
                "latest_partition_rows": 42,
                "latest_date": "2026-05-25",
            }
        ],
        "totals": {"latest_partition_rows": 42, "tables": 1, "latest_date": "2026-05-25"},
    }
    with patch("main.query_system_summary", return_value=expected) as query:
        response = client.get("/api/system/summary")

    assert response.status_code == 200
    assert response.json() == expected
    query.assert_called_once_with()


def test_cache_warmup_endpoint_runs_dashboard_queries(client):
    freshness = {"data_through": "2026-05-26", "days_available": 37}
    with (
        patch("main.get_freshness", return_value=freshness) as get_freshness,
        patch("main.query_system_summary", return_value={"tables": []}) as system_summary,
        patch("main.query_platform_overview", return_value={"totals": {}}) as platform_overview,
    ):
        response = client.post("/api/cache/warmup")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert [item["name"] for item in payload["results"]] == [
        "freshness",
        "system_summary",
        "platform_14d",
        "platform_30d",
        "platform_latest_date",
    ]
    get_freshness.assert_called_once_with()
    system_summary.assert_called_once_with()
    assert platform_overview.call_count == 3
    platform_overview.assert_any_call(days=14)
    platform_overview.assert_any_call(days=30)
    platform_overview.assert_any_call(date="2026-05-26")


def test_freshness_failures_are_not_cached(client, monkeypatch):
    main._freshness_cache.clear()
    calls = {"count": 0}

    class FakeCursor:
        def execute(self, sql):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("catalog down")

        def fetchone(self):
            return {"max_date": "2026-06-02", "days": 42}

    class FakeCursorContext:
        def __enter__(self):
            return FakeCursor()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(main.StarRocks, "cursor", classmethod(lambda cls: FakeCursorContext()))

    failed = client.get("/api/freshness")
    recovered = client.get("/api/freshness")

    assert failed.status_code == 200
    assert failed.json()["error"] == "catalog down"
    assert recovered.status_code == 200
    assert recovered.json() == {"data_through": "2026-06-02", "days_available": 42}
    assert calls["count"] == 2
