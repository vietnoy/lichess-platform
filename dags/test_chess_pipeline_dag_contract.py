from pathlib import Path


DAG_SOURCE = Path(__file__).with_name("chess_pipeline_dag.py").read_text()


def test_daily_pipeline_builds_player_weakness_summary_after_critical_positions():
    assert 'task_id="run_build_player_weakness_summary"' in DAG_SOURCE
    assert 'application="/git/repo/processing/build_player_weakness_summary.py"' in DAG_SOURCE
    assert (
        "process >> build_player_games >> compact_ondemand >> build_critical_positions "
        ">> build_player_weakness_summary >> refresh_starrocks_catalog"
    ) in DAG_SOURCE


def test_starrocks_refresh_includes_player_weakness_summary():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_weakness_summary;'
        in DAG_SOURCE
    )
