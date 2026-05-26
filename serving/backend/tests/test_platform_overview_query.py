import datetime as dt

from serving.backend import db


def test_query_platform_overview_uses_latest_partition(monkeypatch):
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
    assert overview["totals"] == {"games": 10, "player_game_rows": 20, "players": 7}
    assert overview["speed_mix"][0]["speed"] == "blitz"
    assert overview["top_openings"][0]["opening_eco"] == "C20"
    assert overview["phase_mistakes"][0]["phase"] == "middlegame"
    assert all(params == ("2026-05-25",) for _, params in calls if params)
    assert any("HAVING SUM(games) >= 20" in sql for sql, _ in calls)
