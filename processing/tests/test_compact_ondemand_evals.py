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


def test_read_staging_uses_bounded_ordered_jdbc_query(module, monkeypatch):
    monkeypatch.setenv("COMPACT_ONDEMAND_BATCH_ROWS", "123")
    staging = FakeDataFrame([], name="staging")
    player_games = FakeDataFrame([], name="player_games")
    spark = FakeSpark(staging, player_games)

    assert module.read_staging(spark) is staging

    dbtable = spark.read.options["dbtable"]
    assert "FROM move_evaluations_ondemand" in dbtable
    assert "ORDER BY evaluated_at NULLS LAST, game_id, ply, player_id" in dbtable
    assert "LIMIT 123" in dbtable


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
    enqueue_dates = MagicMock()
    monkeypatch.setattr(module, "enqueue_critical_rebuild_dates", enqueue_dates)

    enriched = module.enrich_with_dates(spark, staging)
    assert enriched.rows == [
        {"game_id": "g1", "ply": 12, "player_id": "alice", "fen": "fen1", "date": "2026-05-11"}
    ]

    assert module.run() == 1
    assert FakeDataFrame.last_write_target == "polaris.prod.move_evaluations_ondemand"
    assert FakeDataFrame.last_write_frame.append_called is True
    assert FakeDataFrame.last_write_frame.persist_called is False
    enqueue_dates.assert_called_once_with(["2026-05-11"])
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
        {"game_id": "g1", "ply": 12, "player_id": "alice", "date": "2026-05-11"},
    ], name="existing_evals")
    spark = FakeSpark(staging, player_games, existing_evals)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    clear_staging = MagicMock()
    monkeypatch.setattr(module, "clear_staging", clear_staging)
    enqueue_dates = MagicMock()
    monkeypatch.setattr(module, "enqueue_critical_rebuild_dates", enqueue_dates)

    assert module.run() == 1
    assert FakeDataFrame.last_write_target is None
    enqueue_dates.assert_not_called()
    delete_keys = list(clear_staging.call_args.args[0])
    assert [(key.game_id, key.ply, key.player_id) for key in delete_keys] == [
        ("g1", 12, "alice")
    ]


def test_compaction_does_not_touch_critical_positions(module):
    source = Path(module.__file__).read_text()

    assert "critical_positions" not in source
    assert "MERGE INTO" not in source


def test_rebuild_queue_requeues_done_dates_without_overlapping_running_dates(module):
    source = Path(module.__file__).read_text()

    assert "WHEN {CRITICAL_REBUILD_QUEUE_TABLE}.status = 'running'" in source
    assert "rerun_after_running = CASE" in source
    assert "WHERE {CRITICAL_REBUILD_QUEUE_TABLE}.status NOT IN ('done', 'running')" not in source


def test_successful_write_deletes_postgres_rows(module, monkeypatch):
    staging = FakeDataFrame([
        {"game_id": "g1", "ply": 12, "player_id": "alice"},
    ], name="staging")
    player_games = FakeDataFrame([
        {"game_id": "g1", "player_id": "alice", "date": "2026-05-11"},
    ], name="player_games")
    spark = FakeSpark(staging, player_games)
    monkeypatch.setattr(module, "build_spark", lambda: spark)
    enqueue_dates = MagicMock()
    monkeypatch.setattr(module, "enqueue_critical_rebuild_dates", enqueue_dates)

    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(module.psycopg2, "connect", connect)

    assert module.run() == 1
    enqueue_dates.assert_called_once_with(["2026-05-11"])

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
    enqueue_dates = MagicMock()
    monkeypatch.setattr(module, "enqueue_critical_rebuild_dates", enqueue_dates)

    compacted = module.enrich_with_dates(spark, staging)
    assert compacted.count() == 1

    assert module.run() == 1

    enqueue_dates.assert_called_once_with(["2026-05-11"])
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
    monkeypatch.setattr(module, "enqueue_critical_rebuild_dates", MagicMock())

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
