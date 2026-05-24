import sys
import types
from importlib import util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


class FakeWriter:
    def __init__(self, frame):
        self.frame = frame

    def overwritePartitions(self):
        self.frame.overwrite_partitions_called = True


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows
        self.write_target = None
        self.overwrite_partitions_called = False

    def count(self):
        return len(self.rows)

    def writeTo(self, target):
        self.write_target = target
        return FakeWriter(self)


class FakeSpark:
    def __init__(self, output):
        self.output = output
        self.sql_calls = []
        self.sparkContext = SimpleNamespace(setLogLevel=MagicMock())
        self.stop = MagicMock()

    def sql(self, query):
        self.sql_calls.append(query)
        if query.lstrip().upper().startswith("SELECT") or "WITH source AS" in query:
            return self.output
        return FakeDataFrame([])


@pytest.fixture
def module(monkeypatch):
    monkeypatch.setitem(
        sys.modules,
        "dotenv",
        SimpleNamespace(
            find_dotenv=lambda **kwargs: "",
            load_dotenv=lambda *args, **kwargs: None,
        ),
    )
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = SimpleNamespace(builder=SimpleNamespace())
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)

    module_path = Path(__file__).resolve().parents[1] / "build_player_phase_stats.py"
    spec = util.spec_from_file_location("build_player_phase_stats", module_path)
    stats_module = util.module_from_spec(spec)
    sys.modules.pop("build_player_phase_stats", None)
    sys.modules["build_player_phase_stats"] = stats_module
    spec.loader.exec_module(stats_module)
    return stats_module


def test_ensure_table_creates_player_phase_stats(module):
    spark = FakeSpark(FakeDataFrame([]))

    module.ensure_table(spark)

    ddl = spark.sql_calls[0]
    assert "CREATE TABLE IF NOT EXISTS polaris.prod.player_phase_stats" in ddl
    assert "player_id" in ddl
    assert "phase" in ddl
    assert "critical_positions" in ddl
    assert "time_pressure_positions" in ddl
    assert "avg_eval_swing_cp" in ddl
    assert "PARTITIONED BY (date)" in ddl


def test_phase_stats_sql_groups_critical_positions_by_phase(module):
    sql = module.build_player_phase_stats_sql("2026-05-10")

    assert "FROM polaris.prod.critical_positions" in sql
    assert "WHERE date = DATE '2026-05-10'" in sql
    assert "GROUP BY player_id, date, phase" in sql
    assert "classification = 'blunder'" in sql
    assert "time_pressure IN ('under_10s', 'under_30s')" in sql
    assert "ROUND(AVG(ABS(eval_swing_cp)), 1) AS avg_eval_swing_cp" in sql


def test_phase_stats_sql_can_run_for_all_dates(module):
    sql = module.build_player_phase_stats_sql(None)

    assert "FROM polaris.prod.critical_positions" in sql
    assert "WHERE date = DATE" not in sql


def test_run_overwrites_partition_when_rows_exist(module, monkeypatch):
    output = FakeDataFrame([{"player_id": "alice", "date": "2026-05-10"}])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-10") == 1

    assert output.write_target == "polaris.prod.player_phase_stats"
    assert output.overwrite_partitions_called is True
    assert spark.stop.called is True


def test_run_clears_empty_date_partition(module, monkeypatch):
    output = FakeDataFrame([])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-10") == 0

    assert output.write_target is None
    assert any(
        "DELETE FROM polaris.prod.player_phase_stats WHERE date = DATE '2026-05-10'" in query
        for query in spark.sql_calls
    )
    assert spark.stop.called is True
