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
        allowed_apps={"spark-worker", "starrocks-fe"},
    ) == ["spark-worker-old", "starrocks-fe-old"]


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
