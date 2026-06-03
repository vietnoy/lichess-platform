#!/usr/bin/env python3
"""Small safe maintenance tasks for the Lichess data platform."""

from __future__ import annotations

import argparse
import json
import os
import subprocess


NAMESPACE = "chess"
DEFAULT_CLEANUP_APPS = ("spark-worker", "starrocks-fe", "starrocks-cn")
POLARIS_CATALOG = "polaris_catalog"
STARROCKS_REFRESH_TABLES = (
    "chess_move_events",
    "player_games",
    "move_context_by_ply",
    "move_evaluations",
    "move_evaluations_ondemand",
    "critical_positions",
    "player_weakness_summary",
    "player_opening_stats",
    "player_phase_stats",
    "player_insight_cards",
)


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


def mysql_args(use_password: bool) -> list[str]:
    args = [
        "mysql",
        "--connect-timeout=10",
        "-h",
        os.getenv("STARROCKS_HOST", "starrocks-fe"),
        "-P",
        os.getenv("STARROCKS_PORT", "9030"),
        "-u",
        os.getenv("STARROCKS_USER", "root"),
        "-N",
        "-B",
    ]
    password = os.getenv("STARROCKS_PASSWORD", "")
    if use_password and password:
        args.insert(-2, f"-p{password}")
    return args


def starrocks_sql(sql: str, timeout: int = 60) -> str:
    use_password = bool(os.getenv("STARROCKS_PASSWORD", ""))
    try:
        return run_mysql(sql, use_password=use_password, timeout=timeout)
    except RuntimeError as exc:
        if not use_password or "Access denied" not in str(exc):
            raise
        return run_mysql(sql, use_password=False, timeout=timeout)


def run_mysql(sql: str, use_password: bool, timeout: int) -> str:
    completed = subprocess.run(
        mysql_args(use_password),
        input=sql,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(message)
    return completed.stdout


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required for Polaris catalog repair")
    return value


def sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_polaris_catalog_sql(drop_first: bool) -> str:
    prefix = f"DROP CATALOG IF EXISTS {POLARIS_CATALOG};\n" if drop_first else ""
    credential = f"{required_env('POLARIS_ETL_CLIENT_ID')}:{required_env('POLARIS_ETL_CLIENT_SECRET')}"
    return (
        prefix
        + f"""
CREATE EXTERNAL CATALOG IF NOT EXISTS {POLARIS_CATALOG}
PROPERTIES (
  'type'='iceberg',
  'iceberg.catalog.type'='rest',
  'iceberg.catalog.uri'='http://polaris:8181/api/catalog',
  'iceberg.catalog.warehouse'='chess_warehouse',
  'iceberg.catalog.credential'='{sql_literal(credential)}',
  'iceberg.catalog.scope'='PRINCIPAL_ROLE:ALL',
  'aws.s3.use_instance_profile'='false',
  'aws.s3.access_key'='{sql_literal(required_env('MINIO_ACCESS_KEY'))}',
  'aws.s3.secret_key'='{sql_literal(required_env('MINIO_SECRET_KEY'))}',
  'aws.s3.endpoint'='http://minio:9000',
  'aws.s3.enable_path_style_access'='true'
);
"""
    )


def missing_polaris_catalog_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "unknown catalog" in message and POLARIS_CATALOG.lower() in message


def refresh_polaris_tables() -> None:
    for table in STARROCKS_REFRESH_TABLES:
        try:
            starrocks_sql(f"REFRESH EXTERNAL TABLE {POLARIS_CATALOG}.prod.{table};", timeout=120)
            print(f"refreshed {POLARIS_CATALOG}.prod.{table}")
        except RuntimeError as exc:
            print(f"warning: could not refresh {POLARIS_CATALOG}.prod.{table}: {exc}")


def repair_polaris_catalog_if_missing(dry_run: bool) -> None:
    try:
        starrocks_sql(f"SHOW DATABASES FROM {POLARIS_CATALOG};", timeout=30)
        print(f"{POLARIS_CATALOG} is healthy")
        return
    except RuntimeError as exc:
        if not missing_polaris_catalog_error(exc):
            raise
        print(f"{POLARIS_CATALOG} is missing")

    if dry_run:
        print("dry run; Polaris catalog would be recreated")
        return

    starrocks_sql(build_polaris_catalog_sql(drop_first=True), timeout=60)
    print(f"recreated {POLARIS_CATALOG}")
    refresh_polaris_tables()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe Lichess pipeline maintenance")
    parser.add_argument(
        "--cleanup-app",
        action="append",
        dest="cleanup_apps",
        help="app label allowed for failed-pod cleanup; repeatable",
    )
    parser.add_argument(
        "--repair-polaris-catalog",
        action="store_true",
        help="recreate the StarRocks Polaris external catalog if it is missing",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    allowed_apps = set(args.cleanup_apps or DEFAULT_CLEANUP_APPS)
    names = failed_pods_for_cleanup(pod_json(), allowed_apps)
    delete_failed_pods(names, args.dry_run)
    if args.repair_polaris_catalog:
        repair_polaris_catalog_if_missing(args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
