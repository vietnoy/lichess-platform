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
    pyspark_sql_utils = types.ModuleType("pyspark.sql.utils")
    pyspark_sql_utils.AnalysisException = Exception
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.utils", pyspark_sql_utils)

    module_path = Path(__file__).resolve().parents[1] / "build_critical_positions.py"
    spec = util.spec_from_file_location("build_critical_positions", module_path)
    critical_module = util.module_from_spec(spec)
    sys.modules.pop("build_critical_positions", None)
    sys.modules["build_critical_positions"] = critical_module
    spec.loader.exec_module(critical_module)
    return critical_module


def test_phase_case_uses_simple_mvp_boundaries(module):
    sql = module.phase_case("e.ply")

    assert "e.ply <= 20 THEN 'opening'" in sql
    assert "e.ply <= 60 THEN 'middlegame'" in sql
    assert "ELSE 'endgame'" in sql


def test_time_pressure_case_uses_clock_centisecond_boundaries(module):
    sql = module.time_pressure_case("m.clock_remaining")

    assert "m.clock_remaining IS NULL THEN 'unknown'" in sql
    assert "m.clock_remaining < 1000 THEN 'under_10s'" in sql
    assert "m.clock_remaining < 3000 THEN 'under_30s'" in sql
    assert "ELSE 'normal'" in sql


def test_sql_uses_ondemand_only_and_dedupes_by_player_game_ply(module):
    sql = module.build_critical_positions_sql("2026-05-20", include_legacy_daily=True)

    assert "SELECT DISTINCT game_id, player_id, color, opponent_id, date" in sql
    assert "FROM polaris.prod.move_evaluations_ondemand e" in sql
    assert "FROM polaris.prod.move_context_by_ply m" in sql
    assert "FROM polaris.prod.chess_move_events m" not in sql
    assert "UNION ALL" not in sql
    assert "FROM polaris.prod.move_evaluations e" not in sql
    assert "eval_swing_cp_from_prev" not in sql
    assert "PARTITION BY game_id, ply, player_id" in sql
    assert "ORDER BY source_priority" in sql
    assert "WHERE e.rn = 1" in sql
    assert "e.date = DATE '2026-05-20'" in sql
    assert "m.date = DATE '2026-05-20'" in sql


def test_sql_can_run_without_legacy_daily_table(module):
    sql = module.build_critical_positions_sql("2026-05-20", include_legacy_daily=False)

    assert "FROM polaris.prod.move_evaluations_ondemand e" in sql
    assert "FROM polaris.prod.move_context_by_ply m" in sql
    assert "FROM polaris.prod.move_evaluations e" not in sql
    assert "WHERE e.rn = 1" in sql


def test_resolve_date_arg_supports_all_dates(module):
    assert module.resolve_date_arg(["build_critical_positions.py", "--all"]) is None
    assert module.resolve_date_arg(["build_critical_positions.py", "2026-05-20"]) == "2026-05-20"
