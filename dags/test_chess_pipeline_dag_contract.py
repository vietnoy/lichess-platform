from pathlib import Path


DAG_SOURCE = Path(__file__).with_name("chess_pipeline_dag.py").read_text()


def test_daily_pipeline_only_builds_raw_tables_before_refresh():
    assert "process >> build_player_games >> refresh_starrocks_catalog" in DAG_SOURCE
    assert "process >> build_player_games >> compact_ondemand" not in DAG_SOURCE


def test_analyzer_maintenance_compacts_eval_staging_before_rebuild():
    assert 'dag_id="analyzer_derived_maintenance"' in DAG_SOURCE
    assert 'task_id="run_compact_ondemand_evals"' in DAG_SOURCE
    assert 'application="/git/repo/processing/compact_ondemand_evals.py"' in DAG_SOURCE
    assert 'task_id="rebuild_changed_analyzer_dates"' in DAG_SOURCE
    assert (
        "compact_ondemand >> rebuild_changed_dates >> refresh_analyzer_tables"
        in DAG_SOURCE
    )


def test_analyzer_refresh_includes_player_opening_stats():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_opening_stats;'
        in DAG_SOURCE
    )


def test_analyzer_refresh_includes_player_phase_stats():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_phase_stats;'
        in DAG_SOURCE
    )


def test_analyzer_rebuilder_script_is_used():
    assert (
        "python /git/repo/processing/rebuild_changed_analyzer_dates.py --max-dates 4"
        in DAG_SOURCE
    )


def test_historical_analyzer_staleness_scan_enqueues_dates_only():
    assert 'dag_id="historical_analyzer_staleness_scan"' in DAG_SOURCE
    assert 'task_id="enqueue_stale_analyzer_dates"' in DAG_SOURCE
    assert (
        "python /git/repo/processing/enqueue_stale_analyzer_dates.py --lookback-days 90"
        in DAG_SOURCE
    )


def test_starrocks_refresh_includes_player_weakness_summary():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_weakness_summary;'
        in DAG_SOURCE
    )


def test_starrocks_refresh_includes_player_opening_stats():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_opening_stats;'
        in DAG_SOURCE
    )


def test_starrocks_refresh_includes_player_phase_stats():
    assert (
        'REFRESH EXTERNAL TABLE polaris_catalog.prod.player_phase_stats;'
        in DAG_SOURCE
    )
