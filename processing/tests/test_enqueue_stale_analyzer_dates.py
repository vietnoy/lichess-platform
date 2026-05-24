import sys
from importlib import util
from pathlib import Path
from types import SimpleNamespace


sys.modules.setdefault("psycopg2", SimpleNamespace(connect=None))
sys.modules.setdefault(
    "rebuild_changed_analyzer_dates",
    SimpleNamespace(
        ensure_change_table=lambda cur: cur.execute("ensure"),
        starrocks_mysql_command=lambda sql: ["mysql", "-e", sql],
    ),
)
module_path = Path(__file__).resolve().parents[1] / "enqueue_stale_analyzer_dates.py"
spec = util.spec_from_file_location("enqueue_stale_analyzer_dates", module_path)
enqueue = util.module_from_spec(spec)
spec.loader.exec_module(enqueue)


def test_stale_dates_sql_compares_eval_critical_and_summary():
    sql = enqueue.stale_dates_sql(90)

    assert "move_evaluations_ondemand" in sql
    assert "critical_positions" in sql
    assert "player_weakness_summary" in sql
    assert "classification IN ('blunder', 'mistake', 'inaccuracy')" in sql
    assert "eval_teachable_keys <> COALESCE(c.critical_rows, 0)" in sql
    assert "critical_players, 0) <> COALESCE(s.summary_players, 0)" in sql


def test_enqueue_dates_marks_only_requested_dates_pending():
    cursor = FakeCursor()

    enqueue.enqueue_dates(cursor, ["2026-05-21", "2026-05-23"])

    insert_calls = [call for call in cursor.executed if "INSERT INTO analyzer_partition_changes" in call[0]]
    assert len(insert_calls) == 2
    assert insert_calls[0][1] == ("2026-05-21",)
    assert insert_calls[1][1] == ("2026-05-23",)


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
