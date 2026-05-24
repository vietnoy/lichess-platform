from datetime import date
from importlib import util
from pathlib import Path
import sys
from types import SimpleNamespace


sys.modules.setdefault("psycopg2", SimpleNamespace(connect=None))
module_path = Path(__file__).resolve().parents[1] / "rebuild_changed_analyzer_dates.py"
spec = util.spec_from_file_location("rebuild_changed_analyzer_dates", module_path)
rebuild = util.module_from_spec(spec)
spec.loader.exec_module(rebuild)


def test_spark_submit_command_targets_expected_script_and_date():
    command = rebuild.spark_submit_command(
        "/git/repo/processing/build_critical_positions.py",
        "2026-05-23",
    )

    assert command[:3] == ["spark-submit", "--master", "spark://spark-master:7077"]
    assert "/git/repo/processing/build_critical_positions.py" in command
    assert command[-1] == "2026-05-23"
    assert "spark.cores.max=4" in command


def test_claim_pending_dates_marks_dates_processing_in_order():
    cursor = FakeCursor(
        fetchall_rows=[
            [(date(2026, 5, 21),), (date(2026, 5, 23),)],
        ]
    )

    claimed = rebuild.claim_pending_dates(cursor, max_dates=2)

    assert claimed == ["2026-05-21", "2026-05-23"]
    assert "CREATE TABLE IF NOT EXISTS analyzer_partition_changes" in cursor.executed[0][0]
    assert "ORDER BY date" in cursor.executed[1][0]
    assert cursor.executed[2][1] == (["2026-05-21", "2026-05-23"],)


def test_process_date_runs_builders_validates_and_marks_clean(monkeypatch):
    calls = []
    cursor = FakeCursor()
    monkeypatch.setattr(rebuild.subprocess, "run", lambda command, check: calls.append(command))
    monkeypatch.setattr(rebuild, "refresh_starrocks_tables", lambda: None)
    monkeypatch.setattr(rebuild, "validate_date", lambda date_str: None)

    rebuild.process_date(cursor, "2026-05-23")

    scripts = [command[-2] for command in calls]
    assert scripts == [
        "/git/repo/processing/build_critical_positions.py",
        "/git/repo/processing/build_player_weakness_summary.py",
        "/git/repo/processing/build_player_opening_stats.py",
        "/git/repo/processing/build_player_phase_stats.py",
    ]
    assert all(command[-1] == "2026-05-23" for command in calls)
    assert any("status = 'clean'" in sql for sql, _ in cursor.executed)


def test_process_date_marks_failed_when_validation_fails(monkeypatch):
    cursor = FakeCursor()
    monkeypatch.setattr(rebuild.subprocess, "run", lambda command, check: None)
    monkeypatch.setattr(rebuild, "refresh_starrocks_tables", lambda: None)
    monkeypatch.setattr(
        rebuild,
        "validate_date",
        lambda date_str: (_ for _ in ()).throw(
            RuntimeError("critical_positions duplicate keys for 2026-05-23: 1")
        ),
    )

    try:
        rebuild.process_date(cursor, "2026-05-23")
    except RuntimeError as exc:
        assert "critical_positions duplicate keys" in str(exc)
    else:
        raise AssertionError("expected validation failure")

    assert any("status = 'failed'" in sql for sql, _ in cursor.executed)


class FakeCursor:
    def __init__(self, fetchall_rows=None, fetchone_rows=None):
        self.executed = []
        self._fetchall_rows = list(fetchall_rows or [])
        self._fetchone_rows = list(fetchone_rows or [])

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._fetchall_rows.pop(0)

    def fetchone(self):
        return self._fetchone_rows.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False
