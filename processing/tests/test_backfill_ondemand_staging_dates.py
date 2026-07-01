import sys
import types
from importlib import util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


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

    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.connect = MagicMock()
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_extras.execute_values = MagicMock()
    monkeypatch.setitem(sys.modules, "psycopg2", psycopg2)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", psycopg2_extras)

    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = SimpleNamespace(builder=SimpleNamespace())
    pyspark_sql.functions = SimpleNamespace(col=lambda name: name)
    pyspark_storagelevel = types.ModuleType("pyspark.storagelevel")
    pyspark_storagelevel.StorageLevel = SimpleNamespace(MEMORY_AND_DISK="MEMORY_AND_DISK")
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setitem(sys.modules, "pyspark.storagelevel", pyspark_storagelevel)

    module_path = Path(__file__).resolve().parents[1] / "backfill_ondemand_staging_dates.py"
    spec = util.spec_from_file_location("backfill_ondemand_staging_dates", module_path)
    backfill_module = util.module_from_spec(spec)
    sys.modules.pop("backfill_ondemand_staging_dates", None)
    sys.modules["backfill_ondemand_staging_dates"] = backfill_module
    spec.loader.exec_module(backfill_module)
    return backfill_module


def test_staging_batch_query_reads_null_date_rows_in_stable_order(module):
    query = module.staging_batch_query(1000, None)

    assert "FROM move_evaluations_ondemand" in query
    assert "WHERE date IS NULL" in query
    assert "ORDER BY game_id, ply, player_id" in query
    assert "LIMIT 1000" in query


def test_staging_batch_query_can_resume_after_last_key(module):
    query = module.staging_batch_query(10, ("g'1", 12, "alice'black"))

    assert "AND (game_id, ply, player_id) > ('g''1', 12, 'alice''black')" in query


def test_ensure_pg_date_column_creates_null_date_index(module, monkeypatch):
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    monkeypatch.setattr(module.psycopg2, "connect", MagicMock(return_value=conn))

    module.ensure_pg_date_column()

    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "ALTER TABLE move_evaluations_ondemand ADD COLUMN IF NOT EXISTS date DATE" in statements
    assert "move_eval_ondemand_null_date_order_idx" in statements
    assert "WHERE date IS NULL" in statements
    assert "ANALYZE move_evaluations_ondemand" in statements


def test_update_staging_date_chunk_only_fills_missing_dates(module):
    cursor = MagicMock()
    cursor.rowcount = 2
    rows = [
        ("g1", 12, "alice", "2026-05-30"),
        ("g2", 7, "bob", "2026-05-31"),
    ]

    assert module.update_staging_date_chunk(cursor, rows) == 2

    sql = module.execute_values.call_args.args[1]
    assert "UPDATE move_evaluations_ondemand AS m" in sql
    assert "SET date = v.date" in sql
    assert "AND m.date IS NULL" in sql
    assert module.execute_values.call_args.args[2] == rows
    assert module.execute_values.call_args.kwargs["template"] == "(%s, %s, %s, %s::date)"
