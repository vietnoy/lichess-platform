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
        self.overwrite_partitions_called = False
        self.write_target = None

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

    module_path = Path(__file__).resolve().parents[1] / "build_player_weakness_summary.py"
    spec = util.spec_from_file_location("build_player_weakness_summary", module_path)
    summary_module = util.module_from_spec(spec)
    sys.modules.pop("build_player_weakness_summary", None)
    sys.modules["build_player_weakness_summary"] = summary_module
    spec.loader.exec_module(summary_module)
    return summary_module


def test_ensure_table_creates_daily_player_summary(module):
    spark = FakeSpark(FakeDataFrame([]))

    module.ensure_table(spark)

    ddl = spark.sql_calls[0]
    assert "CREATE TABLE IF NOT EXISTS polaris.prod.player_weakness_summary" in ddl
    assert "player_id" in ddl
    assert "games_with_critical_positions" in ddl
    assert "top_phase" in ddl
    assert "top_time_pressure" in ddl
    assert "PARTITIONED BY (date)" in ddl


def test_summary_sql_aggregates_critical_positions_by_player_and_date(module):
    sql = module.build_player_weakness_summary_sql("2026-05-21")

    assert "FROM polaris.prod.critical_positions" in sql
    assert "WHERE date = DATE '2026-05-21'" in sql
    assert "GROUP BY player_id, date" in sql
    assert "COUNT(DISTINCT game_id) AS games_with_critical_positions" in sql
    assert "classification = 'blunder'" in sql
    assert "phase = 'endgame'" in sql
    assert "time_pressure IN ('under_10s', 'under_30s')" in sql
    assert "row_number() OVER" in sql


def test_summary_sql_can_run_for_all_dates(module):
    sql = module.build_player_weakness_summary_sql(None)

    assert "FROM polaris.prod.critical_positions" in sql
    assert "WHERE date = DATE" not in sql


def test_run_overwrites_partition_when_summary_has_rows(module, monkeypatch):
    output = FakeDataFrame([{"player_id": "alice", "date": "2026-05-21"}])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-21") == 1

    assert output.write_target == "polaris.prod.player_weakness_summary"
    assert output.overwrite_partitions_called is True
    assert spark.stop.called is True


def test_run_clears_empty_date_partition(module, monkeypatch):
    output = FakeDataFrame([])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-21") == 0

    assert output.write_target is None
    assert any(
        "DELETE FROM polaris.prod.player_weakness_summary WHERE date = DATE '2026-05-21'" in query
        for query in spark.sql_calls
    )
    assert spark.stop.called is True
