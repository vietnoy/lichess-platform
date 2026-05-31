import sys
import types
from importlib import util
from pathlib import Path
from types import SimpleNamespace

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
    pyspark = types.ModuleType("pyspark")
    pyspark_sql = types.ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = SimpleNamespace(builder=SimpleNamespace())
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)

    module_path = Path(__file__).resolve().parents[1] / "build_move_context_by_ply.py"
    spec = util.spec_from_file_location("build_move_context_by_ply", module_path)
    move_context_module = util.module_from_spec(spec)
    sys.modules.pop("build_move_context_by_ply", None)
    sys.modules["build_move_context_by_ply"] = move_context_module
    spec.loader.exec_module(move_context_module)
    return move_context_module


def test_sql_builds_one_date_partition(module):
    sql = module.build_move_context_sql("2026-05-30")

    assert "FROM polaris.prod.chess_move_events" in sql
    assert "WHERE date = DATE '2026-05-30'" in sql
    assert "GROUP BY date, game_id, move_number" in sql
    assert "move_number AS ply" in sql


def test_sql_can_express_full_history_when_explicitly_requested(module):
    sql = module.build_move_context_sql(None)

    assert "FROM polaris.prod.chess_move_events" in sql
    assert "WHERE date =" not in sql
    assert "GROUP BY date, game_id, move_number" in sql
