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
        if query.lstrip().upper().startswith("SELECT") or "WITH games AS" in query:
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

    module_path = Path(__file__).resolve().parents[1] / "build_player_opening_stats.py"
    spec = util.spec_from_file_location("build_player_opening_stats", module_path)
    stats_module = util.module_from_spec(spec)
    sys.modules.pop("build_player_opening_stats", None)
    sys.modules["build_player_opening_stats"] = stats_module
    spec.loader.exec_module(stats_module)
    return stats_module


def test_ensure_table_creates_player_opening_stats(module):
    spark = FakeSpark(FakeDataFrame([]))

    module.ensure_table(spark)

    ddl = spark.sql_calls[0]
    assert "CREATE TABLE IF NOT EXISTS polaris.prod.player_opening_stats" in ddl
    assert "player_id" in ddl
    assert "opening_eco" in ddl
    assert "critical_positions" in ddl
    assert "blunders" in ddl
    assert "win_rate_pct" in ddl
    assert "PARTITIONED BY (date)" in ddl


def test_opening_stats_sql_combines_games_and_critical_positions(module):
    sql = module.build_player_opening_stats_sql("2026-05-11")

    assert "FROM polaris.prod.player_games" in sql
    assert "FROM polaris.prod.critical_positions" in sql
    assert "WHERE date = DATE '2026-05-11'" in sql
    assert "GROUP BY player_id, date, opening_eco, opening_name, color" in sql
    assert "COUNT(DISTINCT game_id) AS games" in sql
    assert "classification = 'blunder'" in sql
    assert "ROUND(g.wins * 100.0 / g.games, 1) AS win_rate_pct" in sql


def test_opening_stats_sql_can_run_for_all_dates(module):
    sql = module.build_player_opening_stats_sql(None)

    assert "FROM polaris.prod.player_games" in sql
    assert "WHERE date = DATE" not in sql


def test_resolve_date_arg_supports_all_dates(module):
    assert module.resolve_date_arg(["build_player_opening_stats.py", "--all"]) is None


def test_run_overwrites_partition_when_rows_exist(module, monkeypatch):
    output = FakeDataFrame([{"player_id": "alice", "date": "2026-05-11"}])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-11") == 1

    assert output.write_target == "polaris.prod.player_opening_stats"
    assert output.overwrite_partitions_called is True
    assert spark.stop.called is True


def test_run_clears_empty_date_partition(module, monkeypatch):
    output = FakeDataFrame([])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-05-11") == 0

    assert output.write_target is None
    assert any(
        "DELETE FROM polaris.prod.player_opening_stats WHERE date = DATE '2026-05-11'" in query
        for query in spark.sql_calls
    )
    assert spark.stop.called is True
