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

    def append(self):
        self.frame.append_called = True
        if self.frame.append_error:
            raise self.frame.append_error


class FakeDataFrame:
    last_write_frame = None
    last_write_target = None

    def __init__(self, rows, name=None, append_error=None):
        self.rows = rows
        self.name = name
        self.append_error = append_error
        self.append_called = False
        self.write_target = None
        self.cache_called = False
        self.persist_called = False
        self.persist_level = None
        self.unpersist_called = False

    def count(self):
        return len(self.rows)

    def select(self, *columns):
        return FakeDataFrame(
            [{column: row[column] for column in columns} for row in self.rows],
            name=f"{self.name}.select" if self.name else None,
            append_error=self.append_error,
        )

    def distinct(self):
        seen = set()
        unique_rows = []
        for row in self.rows:
            key = tuple(sorted(row.items()))
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        return FakeDataFrame(unique_rows, name=self.name, append_error=self.append_error)

    def where(self, condition):
        self.where_condition = condition
        return self

    def join(self, other, on, how):
        assert how in {"inner", "left_anti"}
        joined = []
        for left in self.rows:
            matches = [
                right for right in other.rows
                if all(left[column] == right[column] for column in on)
            ]
            if how == "left_anti":
                if not matches:
                    joined.append(left)
                continue
            for right in matches:
                joined.append({**left, **right})
        return FakeDataFrame(joined, name="joined", append_error=self.append_error)

    def dropDuplicates(self, columns):
        seen = set()
        unique_rows = []
        for row in self.rows:
            key = tuple(row[column] for column in columns)
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        return FakeDataFrame(unique_rows, name=self.name, append_error=self.append_error)

    def cache(self):
        self.cache_called = True
        return self

    def persist(self, storage_level):
        self.persist_called = True
        self.persist_level = storage_level
        return self

    def unpersist(self):
        self.unpersist_called = True

    def collect(self):
        return [SimpleNamespace(**row) for row in self.rows]

    def toLocalIterator(self):
        return iter(self.collect())

    def createOrReplaceTempView(self, name):
        self.temp_view_name = name

    def writeTo(self, target):
        self.write_target = target
        FakeDataFrame.last_write_frame = self
        FakeDataFrame.last_write_target = target
        return FakeWriter(self)


class FakeReader:
    def __init__(self, frame):
        self.frame = frame
        self.options = {}
        self.format_name = None

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.frame


class FakeSpark:
    def __init__(self, staging, player_games, existing_evals=None):
        self.read = FakeReader(staging)
        self.player_games = player_games
        self.existing_evals = existing_evals or FakeDataFrame([], name="existing_evals")
        self.sparkContext = SimpleNamespace(setLogLevel=MagicMock())
        self.sql = MagicMock(return_value=FakeDataFrame([], name="sql"))
        self.stop = MagicMock()

    def table(self, name):
        if name == "polaris.prod.player_games":
            return self.player_games
        if name == "polaris.prod.move_evaluations_ondemand":
            return self.existing_evals
        raise AssertionError(name)


@pytest.fixture
def module(monkeypatch):
    FakeDataFrame.last_write_frame = None
    FakeDataFrame.last_write_target = None
    monkeypatch.setitem(
        sys.modules,
        "dotenv",
        SimpleNamespace(
            find_dotenv=lambda **kwargs: "",
            load_dotenv=lambda *args, **kwargs: None,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "psycopg2",
        SimpleNamespace(connect=MagicMock()),
    )
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = SimpleNamespace(builder=SimpleNamespace())
    pyspark_storagelevel = types.ModuleType("pyspark.storagelevel")
    pyspark_storagelevel.StorageLevel = SimpleNamespace(DISK_ONLY="DISK_ONLY")
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setitem(sys.modules, "pyspark.storagelevel", pyspark_storagelevel)
    module_path = Path(__file__).resolve().parents[1] / "compact_ondemand_evals.py"
    spec = util.spec_from_file_location("compact_ondemand_evals", module_path)
    compact_module = util.module_from_spec(spec)
    sys.modules.pop("compact_ondemand_evals", None)
    sys.modules["compact_ondemand_evals"] = compact_module
    spec.loader.exec_module(compact_module)
    return compact_module


def test_empty_staging_skips_write_and_delete(module, monkeypatch):
    staging = FakeDataFrame([], name="staging")
    player_games = FakeDataFrame([], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    clear_staging = MagicMock()
    monkeypatch.setattr(module, "clear_staging", clear_staging)

    assert module.run() == 0

    assert staging.write_target is None
    clear_staging.assert_not_called()
    spark.stop.assert_called_once()


def test_non_empty_staging_writes_joined_rows_with_date(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice", "fen": "fen1"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    clear_staging = MagicMock()
    monkeypatch.setattr(module, "clear_staging", clear_staging)
    append_critical_positions = MagicMock(return_value=1)
    monkeypatch.setattr(module, "append_critical_positions", append_critical_positions)

    enriched = module.enrich_with_dates(spark, staging)
    assert enriched.rows == [
        {"game_id": "g1", "ply": 12, "player_id": "alice", "fen": "fen1", "date": "2026-05-11"}
    ]

    assert module.run() == 1
    assert FakeDataFrame.last_write_target == "polaris.prod.move_evaluations_ondemand"
    assert FakeDataFrame.last_write_frame.append_called is True
    assert FakeDataFrame.last_write_frame.persist_called is False
    append_critical_positions.assert_called_once()
    assert list(clear_staging.call_args.args[0])[0].game_id == "g1"


def test_changed_dates_are_distinct_and_sorted(module):
    compacted = FakeDataFrame([
        {"game_id": "g2", "ply": 1, "player_id": "bob", "date": "2026-05-12"},
        {"game_id": "g1", "ply": 1, "player_id": "alice", "date": "2026-05-11"},
        {"game_id": "g1", "ply": 2, "player_id": "alice", "date": "2026-05-11"},
    ])

    assert module.changed_dates(compacted) == ["2026-05-11", "2026-05-12"]


def test_existing_iceberg_rows_are_not_appended_but_staging_is_cleared(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    existing_evals = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="existing_evals")
    spark = FakeSpark(staging, player_games, existing_evals)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    clear_staging = MagicMock()
    monkeypatch.setattr(module, "clear_staging", clear_staging)
    append_critical_positions = MagicMock()
    monkeypatch.setattr(module, "append_critical_positions", append_critical_positions)

    assert module.run() == 1
    assert FakeDataFrame.last_write_target is None
    append_critical_positions.assert_not_called()
    delete_keys = list(clear_staging.call_args.args[0])
    assert [(key.game_id, key.ply, key.player_id) for key in delete_keys] == [
        ("g1", 12, "alice")
    ]


def test_incremental_critical_positions_reads_only_compacted_batch(module):
    compacted = MagicMock()
    critical_rows = MagicMock()
    critical_rows.count.return_value = 3
    spark = MagicMock()
    spark.sql.return_value = critical_rows

    assert module.append_critical_positions(spark, compacted) == 3

    compacted.createOrReplaceTempView.assert_called_once_with("new_move_evaluations_ondemand")
    sql = spark.sql.call_args.args[0]
    assert "FROM new_move_evaluations_ondemand e" in sql
    assert "FROM polaris.prod.move_evaluations e" not in sql
    assert "LEFT ANTI JOIN polaris.prod.critical_positions existing" in sql
    critical_rows.writeTo.assert_called_once_with("polaris.prod.critical_positions")
    critical_rows.writeTo.return_value.append.assert_called_once()


def test_successful_write_deletes_postgres_rows(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(module.psycopg2, "connect", connect)

    assert module.run() == 1

    delete_calls = [
        call for call in cursor.execute.call_args_list
        if "DELETE FROM move_evaluations_ondemand" in call.args[0]
    ]
    assert len(delete_calls) == 1
    sql, params = delete_calls[0].args
    assert "DELETE FROM move_evaluations_ondemand" in sql
    assert params == ["g1", 12, "alice"]


def test_failed_write_does_not_delete_postgres_rows(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="staging", append_error=RuntimeError("iceberg write failed"))
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    connect = MagicMock()
    monkeypatch.setattr(module.psycopg2, "connect", connect)

    with pytest.raises(RuntimeError, match="iceberg write failed"):
        module.run()

    connect.assert_not_called()
    spark.stop.assert_called_once()


def test_unmatched_staging_row_stays_out_of_delete_keys(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice", "fen": "fen1"},
        {"game_id": "g2", "ply": 7, "player_id": "bob", "fen": "fen2"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    clear_staging = MagicMock()
    monkeypatch.setattr(module, "clear_staging", clear_staging)

    compacted = module.enrich_with_dates(spark, staging)
    assert compacted.count() == 1

    assert module.run() == 1

    clear_staging.assert_called_once()
    delete_keys = list(clear_staging.call_args.args[0])
    assert len(delete_keys) == 1
    assert [(key.game_id, key.ply, key.player_id) for key in delete_keys] == [
        ("g1", 12, "alice")
    ]
    assert ("g2", 7, "bob") not in [
        (key.game_id, key.ply, key.player_id) for key in delete_keys
    ]


def test_clear_staging_failure_after_successful_append_is_raised(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)

    cursor_error = RuntimeError("postgres delete failed")
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.execute.side_effect = cursor_error
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(module.psycopg2, "connect", connect)

    # Duplicate-on-retry hazard: Iceberg append succeeded, then staging cleanup failed.
    with pytest.raises(RuntimeError, match="postgres delete failed"):
        module.run()

    assert FakeDataFrame.last_write_target == "polaris.prod.move_evaluations_ondemand"
    assert FakeDataFrame.last_write_frame.append_called is True
    cursor.execute.assert_called_once()
    spark.stop.assert_called_once()
