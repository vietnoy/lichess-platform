import pytest

from ops import pipeline_health


def test_airflow_json_from_output_skips_warning_lines():
    output = "\n".join(
        [
            "/home/airflow/.local/lib/python3.13/site-packages warning",
            "[{\"dag_run_id\": \"scheduled__1\", \"state\": \"success\"}]",
        ]
    )

    assert pipeline_health.airflow_json_from_output(output, "kafka_to_minio") == [
        {"dag_run_id": "scheduled__1", "state": "success"}
    ]


def test_airflow_json_from_output_requires_json_array():
    with pytest.raises(RuntimeError, match="airflow returned no JSON"):
        pipeline_health.airflow_json_from_output("no runs found", "kafka_to_minio")


def test_check_airflow_runs_uses_latest_run(monkeypatch):
    def fake_airflow_json_local(dag_id, start_date):
        return [
            {"dag_run_id": f"{dag_id}-old", "state": "failed", "logical_date": "2026-05-19T01:00:00"},
            {"dag_run_id": f"{dag_id}-new", "state": "success", "logical_date": "2026-05-19T02:00:00"},
        ]

    monkeypatch.setattr(pipeline_health, "airflow_json_local", fake_airflow_json_local)

    results = pipeline_health.check_airflow_runs(None, lookback_hours=30, local_cli=True)

    assert [result.status for result in results] == ["OK", "OK"]
    assert all("state=success" in result.detail for result in results)


def test_check_airflow_runs_warns_when_latest_run_is_in_progress(monkeypatch):
    def fake_airflow_json_local(dag_id, start_date):
        return [
            {"dag_run_id": f"{dag_id}-old", "state": "success", "logical_date": "2026-05-19T01:00:00"},
            {"dag_run_id": f"{dag_id}-new", "state": "running", "logical_date": "2026-05-19T02:00:00"},
        ]

    monkeypatch.setattr(pipeline_health, "airflow_json_local", fake_airflow_json_local)

    results = pipeline_health.check_airflow_runs(None, lookback_hours=30, local_cli=True)

    assert [result.status for result in results] == ["WARN", "WARN"]
    assert all("last success=" in result.detail for result in results)


def test_check_kafka_offsets_reports_growth(monkeypatch):
    offsets = iter(
        [
            {0: 10},
            {0: 20},
            {0: 30},
            {0: 15},
            {0: 25},
            {0: 35},
        ]
    )

    monkeypatch.setattr(pipeline_health, "kafka_offsets", lambda topic: next(offsets))
    monkeypatch.setattr(pipeline_health.time, "sleep", lambda seconds: None)

    result = pipeline_health.check_kafka_offsets(offset_wait_s=20)

    assert result.status == "OK"
    assert "lichess.game_start +5" in result.detail
    assert "lichess.game_end +5" in result.detail
    assert "lichess.moves +5" in result.detail


def test_check_spark_workers_warns_on_evicted_pods():
    pods = {
        "items": [
            {
                "metadata": {"name": "spark-worker-good"},
                "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
            },
            {
                "metadata": {"name": "spark-worker-old"},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
        ]
    }

    result = pipeline_health.check_spark_workers(pods)

    assert result.status == "WARN"
    assert result.detail == "ready=1 bad_or_evicted=1"


def test_check_failed_pods_ok_when_none_failed():
    pods = {
        "items": [
            {
                "metadata": {"name": "spark-worker-good", "labels": {"app": "spark-worker"}},
                "status": {"phase": "Running"},
            },
            {
                "metadata": {"name": "analyzer-init-done", "labels": {"app": "analyzer-init"}},
                "status": {"phase": "Succeeded"},
            },
        ]
    }

    result = pipeline_health.check_failed_pods(pods)

    assert result.status == "OK"
    assert result.detail == "none"


def test_check_failed_pods_groups_by_app_and_reason():
    pods = {
        "items": [
            {
                "metadata": {"name": "spark-worker-old", "labels": {"app": "spark-worker"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "spark-worker-older", "labels": {"app": "spark-worker"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "starrocks-fe-old", "labels": {"app": "starrocks-fe"}},
                "status": {"phase": "Failed", "reason": "Error"},
            },
        ]
    }

    result = pipeline_health.check_failed_pods(pods)

    assert result.status == "WARN"
    assert result.detail == "spark-worker=2 (Evicted), starrocks-fe=1 (Error)"


def test_parse_date_counts_reads_mysql_batch_output():
    output = "2026-05-18\t100\n2026-05-19\t250\n"

    assert pipeline_health.parse_date_counts(output) == {
        "2026-05-18": 100,
        "2026-05-19": 250,
    }


def test_check_serving_rows_reports_positive_counts(monkeypatch):
    queries = []

    def fake_starrocks_query(sql, scheduler_pod, local_cli):
        queries.append(sql)
        if "chess_move_events" in sql:
            return "2026-05-19\t12\n"
        return "2026-05-19\t3\n"

    monkeypatch.setattr(pipeline_health, "starrocks_query", fake_starrocks_query)

    results = pipeline_health.check_serving_rows(
        scheduler_pod=None,
        dates=["2026-05-19", "2026-05-18"],
        local_cli=True,
    )

    assert [result.status for result in results] == ["OK", "OK"]
    assert results[0].name == "starrocks_chess_move_events_fresh_rows"
    assert results[1].name == "starrocks_player_games_fresh_rows"
    assert "WHERE date IN ('2026-05-19', '2026-05-18')" in queries[0]


def test_check_serving_rows_fails_when_no_recent_rows(monkeypatch):
    monkeypatch.setattr(pipeline_health, "starrocks_query", lambda sql, scheduler_pod, local_cli: "")

    results = pipeline_health.check_serving_rows(
        scheduler_pod="airflow-scheduler-0",
        dates=["2026-05-19"],
        local_cli=False,
    )

    assert [result.status for result in results] == ["FAIL", "FAIL"]
    assert all("no rows for checked dates" in result.detail for result in results)


def test_starrocks_query_retries_without_password_on_access_denied(monkeypatch):
    commands = []

    def fake_run_starrocks_query_command(command, scheduler_pod, local_cli):
        commands.append(command)
        if len(commands) == 1:
            raise RuntimeError("ERROR 1045 (28000): Access denied for user 'root'")
        return "2026-05-19\t1\n"

    monkeypatch.setattr(
        pipeline_health,
        "run_starrocks_query_command",
        fake_run_starrocks_query_command,
    )

    output = pipeline_health.starrocks_query("SELECT 1", "airflow-scheduler-0", local_cli=False)

    assert output == "2026-05-19\t1\n"
    assert '-p"$STARROCKS_PASSWORD"' in commands[0]
    assert '-p"$STARROCKS_PASSWORD"' not in commands[1]


def test_summarize_error_prefers_database_error_line():
    message = "\n".join(
        [
            "--------------",
            "SELECT date FROM missing",
            "--------------",
            "ERROR 5078 (42000) at line 1: Unknown catalog 'polaris_catalog'.",
            "command terminated with exit code 1",
        ]
    )

    assert pipeline_health.summarize_error(RuntimeError(message)) == (
        "ERROR 5078 (42000) at line 1: Unknown catalog 'polaris_catalog'."
    )
