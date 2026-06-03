import datetime as dt

from serving.backend import db


def test_query_system_summary_counts_all_prod_tables(monkeypatch):
    calls = []

    def fake_run(sql, params=()):
        calls.append((sql, params))
        if "bucket_floor" in sql:
            return [{"bucket_floor": 1600, "players": 12, "player_game_rows": 34}]
        if "COUNT(*)" in sql:
            return [{"row_count": 3}]
        return [{"latest_date": dt.date(2026, 5, 25)}]

    db._query_cache.clear()
    monkeypatch.setattr(db, "_run", fake_run)

    summary = db.query_system_summary()

    assert summary["totals"] == {
        "latest_partition_rows": 3 * len(db.PROD_TABLES),
        "tables": len(db.PROD_TABLES),
        "latest_date": "2026-05-25",
    }
    assert len(summary["tables"]) == len(db.PROD_TABLES)
    assert summary["tables"][0]["latest_date"] == "2026-05-25"
    assert summary["tables"][0]["latest_partition_rows"] == 3
    assert summary["tables"][0]["description"]
    assert summary["rating_histogram"] == [{"bucket_floor": 1600, "players": 12, "player_game_rows": 34}]
    assert len(calls) == len(db.PROD_TABLES) * 2 + 1
    assert all(params == ("2026-05-25",) for sql, params in calls if "COUNT(*)" in sql and "bucket_floor" not in sql)
    assert any("MAX(as_of_date) AS latest_date" in sql for sql, _params in calls)
    assert any("WHERE as_of_date = %s" in sql for sql, _params in calls)


def test_query_system_summary_tolerates_single_table_failure(monkeypatch):
    db._query_cache.clear()
    failed = {"done": False}

    def fake_run(sql, params=()):
        if "bucket_floor" in sql:
            return [{"bucket_floor": 1600, "players": 12, "player_game_rows": 34}]
        if not failed["done"] and "MAX(date) AS latest_date" in sql:
            failed["done"] = True
            raise db.errors.DatabaseError("catalog temporarily unavailable")
        if "COUNT(*)" in sql:
            return [{"row_count": 3}]
        return [{"latest_date": dt.date(2026, 5, 25)}]

    monkeypatch.setattr(db, "_run", fake_run)

    summary = db.query_system_summary()

    assert summary["totals"]["tables"] == len(db.PROD_TABLES)
    assert summary["tables"][0]["latest_partition_rows"] == 0
    assert "catalog temporarily unavailable" in summary["tables"][0]["error"]
    assert summary["rating_histogram"] == [{"bucket_floor": 1600, "players": 12, "player_game_rows": 34}]
