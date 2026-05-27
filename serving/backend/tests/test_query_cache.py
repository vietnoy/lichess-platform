from serving.backend import db


def test_platform_overview_uses_ttl_cache(monkeypatch):
    db._query_cache.clear()
    calls = {"count": 0}

    def fake_run(sql, params=()):
        calls["count"] += 1
        if "MAX(date)" in sql:
            return [{"date": "2026-05-26"}]
        if "COUNT(DISTINCT game_id)" in sql and "GROUP BY speed" not in sql:
            return [{"games": 1, "player_game_rows": 2, "players": 1}]
        return []

    monkeypatch.setattr(db, "_run", fake_run)

    first = db.query_platform_overview(days=30)
    second = db.query_platform_overview(days=30)

    assert first == second
    assert calls["count"] == 5


def test_player_aggregate_cache_is_keyed_by_arguments(monkeypatch):
    db._query_cache.clear()
    calls = []

    def fake_run(sql, params=()):
        calls.append(params)
        return [{"player_id": params[0], "critical_positions": 1}]

    monkeypatch.setattr(db, "_run", fake_run)

    assert db.query_weakness_summary("benirks", days=60)["player_id"] == "benirks"
    assert db.query_weakness_summary("benirks", days=60)["player_id"] == "benirks"
    assert db.query_weakness_summary("benirks", days=14)["player_id"] == "benirks"

    assert calls == [("benirks", 60), ("benirks", 14)]
