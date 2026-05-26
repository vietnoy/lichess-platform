import datetime as dt

from serving.backend import db


def test_query_system_summary_counts_all_prod_tables(monkeypatch):
    calls = []

    def fake_run(sql, params=()):
        calls.append(sql)
        return [{"row_count": 3, "latest_date": dt.date(2026, 5, 25)}]

    monkeypatch.setattr(db, "_run", fake_run)

    summary = db.query_system_summary()

    assert summary["totals"] == {
        "rows": 3 * len(db.PROD_TABLES),
        "tables": len(db.PROD_TABLES),
        "latest_date": "2026-05-25",
    }
    assert len(summary["tables"]) == len(db.PROD_TABLES)
    assert summary["tables"][0]["latest_date"] == "2026-05-25"
    assert summary["tables"][0]["description"]
    assert len(calls) == len(db.PROD_TABLES)
    assert all("AS row_count" in sql for sql in calls)
