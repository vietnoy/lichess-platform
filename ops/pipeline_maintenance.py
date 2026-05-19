#!/usr/bin/env python3
"""Small safe maintenance tasks for the Lichess data platform."""

from __future__ import annotations

import argparse
import json
import subprocess


NAMESPACE = "chess"
DEFAULT_CLEANUP_APPS = ("spark-worker", "starrocks-fe")


def run_cmd(args: list[str], timeout: int = 30) -> str:
    completed = subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(message)
    return completed.stdout


def kubectl(args: list[str], timeout: int = 30) -> str:
    return run_cmd(["kubectl", *args], timeout=timeout)


def pod_json() -> dict:
    return json.loads(kubectl(["get", "pods", "-n", NAMESPACE, "-o", "json"], timeout=30))


def pod_app_name(pod: dict) -> str:
    labels = pod.get("metadata", {}).get("labels", {})
    if labels.get("app"):
        return labels["app"]
    name = pod.get("metadata", {}).get("name", "unknown")
    return name.rsplit("-", 2)[0] if "-" in name else name


def failed_pods_for_cleanup(pods: dict, allowed_apps: set[str]) -> list[str]:
    names = []
    for item in pods.get("items", []):
        status = item.get("status", {})
        metadata = item.get("metadata", {})
        if status.get("phase") != "Failed":
            continue
        if pod_app_name(item) not in allowed_apps:
            continue
        names.append(metadata["name"])
    return sorted(names)


def delete_failed_pods(names: list[str], dry_run: bool) -> None:
    if not names:
        print("no failed pods to delete")
        return
    print("failed pods selected for cleanup: " + ", ".join(names))
    if dry_run:
        print("dry run; no pods deleted")
        return
    kubectl(["delete", "pods", "-n", NAMESPACE, *names], timeout=120)
    print(f"deleted {len(names)} failed pods")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe Lichess pipeline maintenance")
    parser.add_argument(
        "--cleanup-app",
        action="append",
        dest="cleanup_apps",
        help="app label allowed for failed-pod cleanup; repeatable",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    allowed_apps = set(args.cleanup_apps or DEFAULT_CLEANUP_APPS)
    names = failed_pods_for_cleanup(pod_json(), allowed_apps)
    delete_failed_pods(names, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
