import datetime as dt

from serving.backend import db


def test_query_platform_overview_defaults_to_latest_30_days(monkeypatch):
    calls = []

    def fake_run(sql, params=()):
        calls.append((sql, params))
        if "MAX(date)" in sql:
            return [{"date": dt.date(2026, 5, 25)}]
        if "COUNT(DISTINCT game_id)" in sql and "GROUP BY speed" not in sql:
            return [{"games": 10, "player_game_rows": 20, "players": 7}]
        if "GROUP BY speed" in sql:
            return [{"speed": "blitz", "games": 8, "player_game_rows": 16, "avg_rating": 1540}]
        if "opening_name" in sql:
            return [{"opening_eco": "C20", "opening_name": "King's Pawn Game", "games": 12, "win_rate_pct": 52.5, "critical_positions": 3}]
        if "phase" in sql:
            return [{"phase": "middlegame", "critical_positions": 9, "blunders": 2, "mistakes": 3, "inaccuracies": 4}]
        return []

    monkeypatch.setattr(db, "_run", fake_run)

    overview = db.query_platform_overview()

    assert overview["date"] == "2026-05-25"
    assert overview["start_date"] == "2026-04-26"
    assert overview["end_date"] == "2026-05-25"
    assert overview["range"] == "30d"
    assert overview["totals"] == {"games": 10, "player_game_rows": 20, "players": 7}
    assert overview["speed_mix"][0]["speed"] == "blitz"
    assert overview["top_openings"][0]["opening_eco"] == "C20"
    assert overview["phase_mistakes"][0]["phase"] == "middlegame"
    assert all(params == ("2026-04-26", "2026-05-25") for _, params in calls if params)
    assert any("BETWEEN %s AND %s" in sql for sql, params in calls if params)
    assert any("HAVING SUM(games) >= 20" in sql for sql, _ in calls)


def test_query_platform_overview_supports_custom_date(monkeypatch):
    calls = []

    def fake_run(sql, params=()):
        calls.append((sql, params))
        if "MAX(date)" in sql:
            return [{"date": dt.date(2026, 5, 25)}]
        if "COUNT(DISTINCT game_id)" in sql and "GROUP BY speed" not in sql:
            return [{"games": 2, "player_game_rows": 4, "players": 3}]
        return []

    monkeypatch.setattr(db, "_run", fake_run)

    overview = db.query_platform_overview(date="2026-05-10")

    assert overview["date"] == "2026-05-10"
    assert overview["start_date"] == "2026-05-10"
    assert overview["end_date"] == "2026-05-10"
    assert overview["range"] == "date"
    assert all(params == ("2026-05-10", "2026-05-10") for _, params in calls if params)


def test_query_platform_overview_supports_all_time(monkeypatch):
    calls = []

    def fake_run(sql, params=()):
        calls.append((sql, params))
        if "MAX(date)" in sql:
            return [{"date": dt.date(2026, 5, 25)}]
        if "COUNT(DISTINCT game_id)" in sql and "GROUP BY speed" not in sql:
            return [{"games": 50, "player_game_rows": 100, "players": 40}]
        return []

    monkeypatch.setattr(db, "_run", fake_run)

    overview = db.query_platform_overview(all_time=True)

    assert overview["date"] == "2026-05-25"
    assert overview["start_date"] is None
    assert overview["end_date"] is None
    assert overview["range"] == "all"
    assert all(params == () for _, params in calls)
