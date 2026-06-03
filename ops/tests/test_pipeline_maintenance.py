from ops import pipeline_maintenance


def test_failed_pods_for_cleanup_only_selects_allowed_failed_apps():
    pods = {
        "items": [
            {
                "metadata": {"name": "spark-worker-old", "labels": {"app": "spark-worker"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "starrocks-fe-old", "labels": {"app": "starrocks-fe"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "starrocks-cn-old", "labels": {"app": "starrocks-cn"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "postgres-old", "labels": {"app": "postgres"}},
                "status": {"phase": "Failed", "reason": "Evicted"},
            },
            {
                "metadata": {"name": "spark-worker-live", "labels": {"app": "spark-worker"}},
                "status": {"phase": "Running"},
            },
        ]
    }

    assert pipeline_maintenance.failed_pods_for_cleanup(
        pods,
        allowed_apps={"spark-worker", "starrocks-fe", "starrocks-cn"},
    ) == ["spark-worker-old", "starrocks-cn-old", "starrocks-fe-old"]


def test_delete_failed_pods_dry_run_does_not_call_kubectl(monkeypatch, capsys):
    called = False

    def fake_kubectl(args, timeout=30):
        nonlocal called
        called = True

    monkeypatch.setattr(pipeline_maintenance, "kubectl", fake_kubectl)

    pipeline_maintenance.delete_failed_pods(["spark-worker-old"], dry_run=True)

    assert called is False
    assert "dry run" in capsys.readouterr().out


def test_delete_failed_pods_calls_kubectl_with_names(monkeypatch):
    calls = []

    monkeypatch.setattr(pipeline_maintenance, "kubectl", lambda args, timeout=30: calls.append(args))

    pipeline_maintenance.delete_failed_pods(["spark-worker-old", "starrocks-fe-old"], dry_run=False)

    assert calls == [
        [
            "delete",
            "pods",
            "-n",
            "chess",
            "spark-worker-old",
            "starrocks-fe-old",
        ]
    ]


def test_missing_polaris_catalog_error_requires_catalog_name():
    assert pipeline_maintenance.missing_polaris_catalog_error(
        RuntimeError("ERROR 5078 (42000): Unknown catalog 'polaris_catalog'")
    )
    assert not pipeline_maintenance.missing_polaris_catalog_error(
        RuntimeError("ERROR 5078 (42000): Unknown catalog 'other_catalog'")
    )


def test_repair_polaris_catalog_noops_when_catalog_is_healthy(monkeypatch, capsys):
    calls = []

    def fake_starrocks_sql(sql, timeout=60):
        calls.append(sql)
        return "prod\n"

    monkeypatch.setattr(pipeline_maintenance, "starrocks_sql", fake_starrocks_sql)

    pipeline_maintenance.repair_polaris_catalog_if_missing(dry_run=False)

    assert calls == ["SHOW DATABASES FROM polaris_catalog;"]
    assert "is healthy" in capsys.readouterr().out


def test_repair_polaris_catalog_dry_run_does_not_recreate(monkeypatch, capsys):
    calls = []

    def fake_starrocks_sql(sql, timeout=60):
        calls.append(sql)
        raise RuntimeError("ERROR 5078 (42000): Unknown catalog 'polaris_catalog'")

    monkeypatch.setattr(pipeline_maintenance, "starrocks_sql", fake_starrocks_sql)

    pipeline_maintenance.repair_polaris_catalog_if_missing(dry_run=True)

    assert calls == ["SHOW DATABASES FROM polaris_catalog;"]
    assert "would be recreated" in capsys.readouterr().out


def test_repair_polaris_catalog_recreates_and_refreshes(monkeypatch):
    calls = []
    monkeypatch.setenv("POLARIS_ETL_CLIENT_ID", "client")
    monkeypatch.setenv("POLARIS_ETL_CLIENT_SECRET", "secret")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minio")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minio-secret")

    def fake_starrocks_sql(sql, timeout=60):
        calls.append(sql)
        if sql == "SHOW DATABASES FROM polaris_catalog;":
            raise RuntimeError("ERROR 5078 (42000): Unknown catalog 'polaris_catalog'")
        return ""

    monkeypatch.setattr(pipeline_maintenance, "starrocks_sql", fake_starrocks_sql)

    pipeline_maintenance.repair_polaris_catalog_if_missing(dry_run=False)

    assert calls[0] == "SHOW DATABASES FROM polaris_catalog;"
    assert calls[1].startswith("DROP CATALOG IF EXISTS polaris_catalog;")
    assert "CREATE EXTERNAL CATALOG IF NOT EXISTS polaris_catalog" in calls[1]
    assert calls[2:] == [
        f"REFRESH EXTERNAL TABLE polaris_catalog.prod.{table};"
        for table in pipeline_maintenance.STARROCKS_REFRESH_TABLES
    ]
