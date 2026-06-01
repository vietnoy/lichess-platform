from pathlib import Path


DAG_SOURCE = Path(__file__).with_name("chess_pipeline_dag.py").read_text()


def test_daily_pipeline_builds_context_before_refresh():
    assert "process >> build_player_games >> build_move_context >> refresh_starrocks_catalog" in DAG_SOURCE
    assert 'application="/git/repo/processing/build_move_context_by_ply.py"' in DAG_SOURCE
    assert "process >> build_player_games >> compact_ondemand" not in DAG_SOURCE


def test_analyzer_maintenance_compacts_eval_staging_without_history_rebuild():
    assert 'dag_id="analyzer_derived_maintenance"' in DAG_SOURCE
    assert 'schedule="15 1-23/2 * * *"' in DAG_SOURCE
    assert 'task_id="run_compact_ondemand_evals"' in DAG_SOURCE
    assert 'application="/git/repo/processing/compact_ondemand_evals.py"' in DAG_SOURCE
    maintenance_block = DAG_SOURCE.split('dag_id="analyzer_derived_maintenance"', 1)[1].split(
        'dag_id="init_catalog_starrocks"', 1
    )[0]
    assert 'task_id="rebuild_changed_analyzer_dates"' not in maintenance_block
    assert "compact_ondemand >> refresh_analyzer_tables" in maintenance_block


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


def test_analyzer_critical_positions_runs_as_separate_date_bounded_dag():
    assert 'dag_id="analyzer_critical_positions"' in DAG_SOURCE
    assert 'schedule="45 1-23/2 * * *"' in DAG_SOURCE
    assert 'task_id="rebuild_critical_positions"' in DAG_SOURCE
    assert 'application="/git/repo/processing/build_critical_positions.py"' in DAG_SOURCE
    assert (
        'application_args=["{{ (dag_run.conf or {}).get(\'date\') or \'--next-queued\' }}"]'
        in DAG_SOURCE
    )
    assert "rebuild_critical_positions >> refresh_critical_positions" in DAG_SOURCE

    compaction_block = DAG_SOURCE.split('dag_id="analyzer_derived_maintenance"', 1)[1].split(
        'dag_id="analyzer_critical_positions"', 1
    )[0]
    assert "build_critical_positions.py" not in compaction_block


def test_historical_analyzer_dags_are_not_registered():
    assert 'dag_id="historical_analyzer_rebuild"' not in DAG_SOURCE
    assert 'dag_id="historical_analyzer_staleness_scan"' not in DAG_SOURCE
    assert 'task_id="rebuild_changed_analyzer_dates"' not in DAG_SOURCE
    assert 'task_id="enqueue_stale_analyzer_dates"' not in DAG_SOURCE


def test_analyzer_summary_maintenance_rebuilds_all_summaries_nightly():
    assert 'dag_id="analyzer_summary_maintenance"' in DAG_SOURCE
    assert 'schedule="15 2 * * *"' in DAG_SOURCE
    assert 'task_id="rebuild_player_weakness_summary"' in DAG_SOURCE
    assert 'task_id="rebuild_player_opening_stats"' in DAG_SOURCE
    assert 'task_id="rebuild_player_phase_stats"' in DAG_SOURCE
    assert 'application_args=["--all"]' in DAG_SOURCE
    assert "analyzer_summary_maintenance pending=${pending}" not in DAG_SOURCE
    assert "rebuild_changed_analyzer_dates.py --max-dates 1" not in DAG_SOURCE
    assert (
        "rebuild_player_weakness_summary\n"
        "        >> rebuild_player_opening_stats\n"
        "        >> rebuild_player_phase_stats\n"
        "        >> refresh_summary_tables"
    ) in DAG_SOURCE


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
