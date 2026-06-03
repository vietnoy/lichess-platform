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
        if "WITH windows AS" in query:
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

    module_path = Path(__file__).resolve().parents[1] / "build_player_insight_cards.py"
    spec = util.spec_from_file_location("build_player_insight_cards", module_path)
    insight_module = util.module_from_spec(spec)
    sys.modules.pop("build_player_insight_cards", None)
    sys.modules["build_player_insight_cards"] = insight_module
    spec.loader.exec_module(insight_module)
    return insight_module


def test_ensure_table_creates_player_insight_cards(module):
    spark = FakeSpark(FakeDataFrame([]))

    module.ensure_table(spark)

    ddl = spark.sql_calls[0]
    assert "CREATE TABLE IF NOT EXISTS polaris.prod.player_insight_cards" in ddl
    assert "as_of_date" in ddl
    assert "window_days" in ddl
    assert "insight_type" in ddl
    assert "data_json" in ddl
    assert "PARTITIONED BY (as_of_date)" in ddl


def test_build_sql_uses_existing_aggregate_tables(module):
    sql = module.build_player_insight_cards_sql("2026-06-02")

    assert "polaris.prod.player_weakness_summary" in sql
    assert "polaris.prod.player_phase_stats" in sql
    assert "polaris.prod.player_opening_stats" in sql
    assert "polaris.prod.player_games" in sql
    assert "DATE '2026-06-02'" in sql
    assert "array(14, 30, 60, 0)" in sql
    assert "to_json(named_struct" in sql


def test_run_overwrites_as_of_partition_when_rows_exist(module, monkeypatch):
    output = FakeDataFrame([{"player_id": "alice", "as_of_date": "2026-06-02"}])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-06-02") == 1

    assert output.write_target == "polaris.prod.player_insight_cards"
    assert output.overwrite_partitions_called is True
    assert spark.stop.called is True


def test_run_clears_empty_as_of_partition(module, monkeypatch):
    output = FakeDataFrame([])
    spark = FakeSpark(output)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    assert module.run("2026-06-02") == 0

    assert output.write_target is None
    assert any(
        "DELETE FROM polaris.prod.player_insight_cards WHERE as_of_date = DATE '2026-06-02'" in query
        for query in spark.sql_calls
    )
    assert spark.stop.called is True
