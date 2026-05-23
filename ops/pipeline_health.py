#!/usr/bin/env python3
"""Read-only health checks for the Lichess data pipeline.

The script shells out to kubectl and inspects the live cluster. It does not
restart pods, delete files, trigger DAGs, or write data.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Iterable


NAMESPACE = "chess"
RAW_TOPICS = {
    "lichess.game_start": "game_start",
    "lichess.game_end": "game_end",
    "lichess.moves": "moves",
}
AIRFLOW_DAGS = ("kafka_to_minio", "process_to_polaris")
AIRFLOW_IN_PROGRESS_STATES = {"queued", "running", "scheduled"}
SERVING_TABLES = (
    "polaris_catalog.prod.chess_move_events",
    "polaris_catalog.prod.player_games",
)
CRITICAL_POSITIONS_TABLE = "polaris_catalog.prod.critical_positions"
DEFAULT_INGESTOR_SSH_TARGET = "root@160.187.0.108"


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str

    @property
    def failed(self) -> bool:
        return self.status == "FAIL"


def run_cmd(args: list[str], timeout: int = 30) -> str:
    completed = subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        message = stderr or stdout or f"exit code {completed.returncode}"
        raise RuntimeError(message)
    return completed.stdout


def kubectl(args: list[str], timeout: int = 30) -> str:
    return run_cmd(["kubectl", *args], timeout=timeout)


def pod_json() -> dict:
    return json.loads(kubectl(["get", "pods", "-n", NAMESPACE, "-o", "json"], timeout=30))


def pod_ready(pod: dict) -> bool:
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return bool(statuses) and all(status.get("ready") for status in statuses)


def find_pod(pods: dict, prefix: str) -> str:
    candidates = []
    for item in pods.get("items", []):
        name = item["metadata"]["name"]
        phase = item.get("status", {}).get("phase")
        if name.startswith(prefix) and phase == "Running" and pod_ready(item):
            candidates.append(name)
    if not candidates:
        raise RuntimeError(f"no ready running pod found with prefix {prefix!r}")
    return sorted(candidates)[0]


def kafka_offsets(topic: str) -> dict[int, int]:
    output = kubectl(
        [
            "exec",
            "-n",
            NAMESPACE,
            "kafka-0",
            "--",
            "sh",
            "-c",
            f"/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic {topic}",
        ],
        timeout=30,
    )
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        parts = line.strip().split(":")
        if len(parts) != 3:
            continue
        _, partition, offset = parts
        offsets[int(partition)] = int(offset)
    if not offsets:
        raise RuntimeError(f"no offsets returned for {topic}")
    return offsets


def check_kafka_offsets(offset_wait_s: int) -> CheckResult:
    before = {topic: kafka_offsets(topic) for topic in RAW_TOPICS}
    time.sleep(offset_wait_s)
    after = {topic: kafka_offsets(topic) for topic in RAW_TOPICS}

    deltas = {
        topic: sum(after[topic].values()) - sum(before[topic].values())
        for topic in RAW_TOPICS
    }
    total_delta = sum(deltas.values())
    detail = ", ".join(f"{topic} +{delta}" for topic, delta in deltas.items())
    if total_delta <= 0:
        return CheckResult("kafka_offsets_advancing", "FAIL", f"no offset growth in {offset_wait_s}s ({detail})")
    return CheckResult("kafka_offsets_advancing", "OK", detail)


def count_ingestor_delivery_failures(journal_output: str) -> int:
    return sum(1 for line in journal_output.splitlines() if "Kafka delivery failed:" in line)


def check_ingestor_delivery_failures(
    ssh_target: str | None,
    lookback_minutes: int,
) -> CheckResult:
    if not ssh_target:
        return CheckResult(
            "ingestor_kafka_delivery",
            "WARN",
            "not configured; set --ingestor-ssh-target or INGESTOR_SSH_TARGET",
        )

    output = run_cmd(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            ssh_target,
            f"journalctl -u chess-ingestor --since '{lookback_minutes} minutes ago' --no-pager",
        ],
        timeout=30,
    )
    failures = count_ingestor_delivery_failures(output)
    if failures:
        return CheckResult(
            "ingestor_kafka_delivery",
            "FAIL",
            f"{failures} delivery failures in last {lookback_minutes}m",
        )
    return CheckResult(
        "ingestor_kafka_delivery",
        "OK",
        f"no Kafka delivery failures in last {lookback_minutes}m",
    )


def check_minio_partitions(minio_pod: str, bucket: str, dates: Iterable[str]) -> list[CheckResult]:
    results = []
    date_list = list(dates)
    for prefix in RAW_TOPICS.values():
        output = kubectl(
            [
                "exec",
                "-n",
                NAMESPACE,
                minio_pod,
                "--",
                "sh",
                "-c",
                f"ls -1 /data/{bucket}/{prefix} 2>/dev/null || true",
            ],
            timeout=30,
        )
        partitions = set(output.splitlines())
        present = [date for date in date_list if f"date={date}" in partitions]
        if present:
            results.append(
                CheckResult(
                    f"minio_{prefix}_fresh_partition",
                    "OK",
                    f"found {', '.join(present)}",
                )
            )
        else:
            results.append(
                CheckResult(
                    f"minio_{prefix}_fresh_partition",
                    "FAIL",
                    f"missing all checked dates: {', '.join(date_list)}",
                )
            )
    return results


def airflow_json_from_output(output: str, dag_id: str) -> list[dict]:
    # Airflow may print warnings before JSON. Keep the first line that starts a
    # JSON array rather than matching log fragments like "[warning]".
    lines = output.splitlines()
    start_idx = next((idx for idx, line in enumerate(lines) if line.lstrip().startswith("[")), None)
    if start_idx is None:
        raise RuntimeError(f"airflow returned no JSON for {dag_id}")
    return json.loads("\n".join(lines[start_idx:]))


def airflow_json_local(dag_id: str, start_date: str) -> list[dict]:
    output = run_cmd(
        ["airflow", "dags", "list-runs", dag_id, "-s", start_date, "-o", "json"],
        timeout=60,
    )
    return airflow_json_from_output(output, dag_id)


def airflow_json_via_kubectl(scheduler_pod: str, dag_id: str, start_date: str) -> list[dict]:
    output = kubectl(
        [
            "exec",
            "-n",
            NAMESPACE,
            scheduler_pod,
            "-c",
            "airflow-scheduler",
            "--",
            "airflow",
            "dags",
            "list-runs",
            dag_id,
            "-s",
            start_date,
            "-o",
            "json",
        ],
        timeout=60,
    )
    return airflow_json_from_output(output, dag_id)


def check_airflow_runs(scheduler_pod: str | None, lookback_hours: int, local_cli: bool) -> list[CheckResult]:
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    results = []
    for dag_id in AIRFLOW_DAGS:
        if local_cli:
            runs = airflow_json_local(dag_id, start)
        else:
            if scheduler_pod is None:
                raise RuntimeError("scheduler pod is required when Airflow local CLI is disabled")
            runs = airflow_json_via_kubectl(scheduler_pod, dag_id, start)
        if not runs:
            results.append(CheckResult(f"airflow_{dag_id}_recent_run", "FAIL", f"no runs since {start} UTC"))
            continue
        sorted_runs = sorted(runs, key=lambda row: row.get("run_after") or row.get("logical_date") or "")
        latest = sorted_runs[-1]
        state = latest.get("state")
        run_id = latest.get("dag_run_id") or latest.get("run_id")
        if state == "success":
            results.append(CheckResult(f"airflow_{dag_id}_latest_run", "OK", f"{run_id} state=success"))
        elif state in AIRFLOW_IN_PROGRESS_STATES:
            latest_success = next(
                (row for row in reversed(sorted_runs) if row.get("state") == "success"),
                None,
            )
            if latest_success:
                success_id = latest_success.get("dag_run_id") or latest_success.get("run_id")
                results.append(
                    CheckResult(
                        f"airflow_{dag_id}_latest_run",
                        "WARN",
                        f"{run_id} state={state}; last success={success_id}",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        f"airflow_{dag_id}_latest_run",
                        "FAIL",
                        f"{run_id} state={state}; no successful run since {start} UTC",
                    )
                )
        else:
            results.append(CheckResult(f"airflow_{dag_id}_latest_run", "FAIL", f"{run_id} state={state}"))
    return results


def check_spark_workers(pods: dict) -> CheckResult:
    running = []
    bad = []
    for item in pods.get("items", []):
        name = item["metadata"]["name"]
        if not name.startswith("spark-worker-"):
            continue
        phase = item.get("status", {}).get("phase")
        if phase == "Running" and pod_ready(item):
            running.append(name)
        elif phase in {"Failed", "Unknown"} or item.get("status", {}).get("reason") == "Evicted":
            bad.append(name)

    if not running:
        return CheckResult("spark_workers_ready", "FAIL", f"no ready workers; bad pods={len(bad)}")
    status = "WARN" if bad else "OK"
    return CheckResult(
        "spark_workers_ready",
        status,
        f"ready={len(running)} bad_or_evicted={len(bad)}",
    )


def pod_app_name(pod: dict) -> str:
    labels = pod.get("metadata", {}).get("labels", {})
    if labels.get("app"):
        return labels["app"]
    name = pod.get("metadata", {}).get("name", "unknown")
    return name.rsplit("-", 2)[0] if "-" in name else name


def check_failed_pods(pods: dict) -> CheckResult:
    counts: dict[str, int] = {}
    reasons: dict[str, set[str]] = {}
    for item in pods.get("items", []):
        status = item.get("status", {})
        if status.get("phase") != "Failed":
            continue
        app = pod_app_name(item)
        reason = status.get("reason") or "Failed"
        counts[app] = counts.get(app, 0) + 1
        reasons.setdefault(app, set()).add(reason)

    if not counts:
        return CheckResult("failed_pods", "OK", "none")

    detail = ", ".join(
        f"{app}={counts[app]} ({'/'.join(sorted(reasons[app]))})"
        for app in sorted(counts)
    )
    return CheckResult("failed_pods", "WARN", detail)


def check_analyzer_backlog(pods: dict, threshold: int) -> CheckResult:
    postgres_pod = find_pod(pods, "postgres-")
    sql = "SELECT count(*) FROM move_evaluations_ondemand;"
    output = kubectl(
        [
            "exec",
            "-n",
            NAMESPACE,
            postgres_pod,
            "--",
            "sh",
            "-c",
            f'PGPASSWORD="$POSTGRES_PASSWORD" psql -U "$POSTGRES_USER" -d chess_analyzer_db -Atc "{sql}"',
        ],
        timeout=30,
    )
    count = int(output.strip().splitlines()[-1])
    if count > threshold:
        return CheckResult("analyzer_staging_backlog", "FAIL", f"{count} rows > threshold {threshold}")
    return CheckResult("analyzer_staging_backlog", "OK", f"{count} rows <= threshold {threshold}")


def mysql_shell_command(sql: str, use_password: bool) -> str:
    escaped_sql = sql.replace('"', '\\"')
    password_arg = '-p"$STARROCKS_PASSWORD" ' if use_password else ""
    return (
        'mysql --connect-timeout=10 -h "$STARROCKS_HOST" -P "$STARROCKS_PORT" '
        f'-u "$STARROCKS_USER" {password_arg}-N -B -e '
        f'"{escaped_sql}"'
    )


def run_starrocks_query_command(command: str, scheduler_pod: str | None, local_cli: bool) -> str:
    if local_cli:
        return run_cmd(["bash", "-lc", command], timeout=60)
    if scheduler_pod is None:
        raise RuntimeError("scheduler pod is required when StarRocks local CLI is disabled")
    return kubectl(
        [
            "exec",
            "-n",
            NAMESPACE,
            scheduler_pod,
            "-c",
            "airflow-scheduler",
            "--",
            "bash",
            "-lc",
            command,
        ],
        timeout=60,
    )


def starrocks_query(sql: str, scheduler_pod: str | None, local_cli: bool) -> str:
    try:
        return run_starrocks_query_command(mysql_shell_command(sql, use_password=True), scheduler_pod, local_cli)
    except RuntimeError as exc:
        if "Access denied" not in str(exc):
            raise
        return run_starrocks_query_command(mysql_shell_command(sql, use_password=False), scheduler_pod, local_cli)


def parse_date_counts(output: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in output.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        date_value, count_value = parts
        counts[date_value] = int(count_value)
    return counts


def parse_single_int(output: str) -> int:
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if stripped:
            return int(stripped)
    return 0


def summarize_error(exc: Exception) -> str:
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("ERROR "):
            return line
    return lines[-1] if lines else str(exc)


def check_serving_rows(
    scheduler_pod: str | None,
    dates: Iterable[str],
    local_cli: bool,
) -> list[CheckResult]:
    date_list = list(dates)
    quoted_dates = ", ".join(f"'{date}'" for date in date_list)
    results = []
    for table in SERVING_TABLES:
        sql = f"SELECT date, COUNT(*) FROM {table} WHERE date IN ({quoted_dates}) GROUP BY date;"
        check_name = f"starrocks_{table.rsplit('.', 1)[-1]}_fresh_rows"
        try:
            counts = parse_date_counts(starrocks_query(sql, scheduler_pod, local_cli))
        except RuntimeError as exc:
            results.append(CheckResult(check_name, "FAIL", summarize_error(exc)))
            continue
        positive = {date: count for date, count in counts.items() if count > 0}
        if positive:
            detail = ", ".join(f"{date}={count}" for date, count in sorted(positive.items()))
            results.append(CheckResult(check_name, "OK", detail))
        else:
            results.append(
                CheckResult(check_name, "FAIL", f"no rows for checked dates: {', '.join(date_list)}")
            )
    return results


def check_critical_positions_integrity(
    scheduler_pod: str | None,
    dates: Iterable[str],
    local_cli: bool,
) -> list[CheckResult]:
    date_list = list(dates)
    quoted_dates = ", ".join(f"'{date}'" for date in date_list)
    results = []

    row_sql = (
        f"SELECT date, COUNT(*) FROM {CRITICAL_POSITIONS_TABLE} "
        f"WHERE date IN ({quoted_dates}) GROUP BY date;"
    )
    try:
        counts = parse_date_counts(starrocks_query(row_sql, scheduler_pod, local_cli))
    except RuntimeError as exc:
        return [
            CheckResult(
                "starrocks_critical_positions_fresh_rows",
                "FAIL",
                summarize_error(exc),
            )
        ]

    positive = {date: count for date, count in counts.items() if count > 0}
    if positive:
        detail = ", ".join(f"{date}={count}" for date, count in sorted(positive.items()))
        results.append(CheckResult("starrocks_critical_positions_fresh_rows", "OK", detail))
    else:
        results.append(
            CheckResult(
                "starrocks_critical_positions_fresh_rows",
                "WARN",
                f"no rows for checked dates: {', '.join(date_list)}",
            )
        )

    dupe_sql = (
        "SELECT COUNT(*) AS duplicate_groups FROM ("
        f"SELECT game_id, ply, player_id, COUNT(*) AS n FROM {CRITICAL_POSITIONS_TABLE} "
        f"WHERE date IN ({quoted_dates}) "
        "GROUP BY game_id, ply, player_id HAVING n > 1"
        ") d;"
    )
    try:
        duplicate_groups = parse_single_int(starrocks_query(dupe_sql, scheduler_pod, local_cli))
    except RuntimeError as exc:
        results.append(
            CheckResult(
                "starrocks_critical_positions_duplicate_keys",
                "FAIL",
                summarize_error(exc),
            )
        )
        return results

    if duplicate_groups:
        results.append(
            CheckResult(
                "starrocks_critical_positions_duplicate_keys",
                "FAIL",
                f"{duplicate_groups} duplicate key groups",
            )
        )
    else:
        results.append(CheckResult("starrocks_critical_positions_duplicate_keys", "OK", "none"))
    return results


def default_dates() -> list[str]:
    now = dt.datetime.now(dt.timezone.utc).date()
    return [(now - dt.timedelta(days=offset)).isoformat() for offset in (0, 1)]


def print_results(results: list[CheckResult]) -> int:
    exit_code = 0
    for result in results:
        print(f"{result.status:4} {result.name}: {result.detail}")
        if result.failed:
            exit_code = 1
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only Lichess pipeline health checks")
    parser.add_argument("--bucket", default="chess-dev", help="MinIO bucket/prefix root to inspect")
    parser.add_argument(
        "--date",
        action="append",
        dest="dates",
        help="date partition to require, repeatable; defaults to UTC today and yesterday",
    )
    parser.add_argument("--offset-wait-s", type=int, default=20, help="seconds between Kafka offset samples")
    parser.add_argument("--airflow-lookback-hours", type=int, default=30)
    parser.add_argument("--analyzer-backlog-threshold", type=int, default=1_000_000)
    parser.add_argument(
        "--ingestor-ssh-target",
        default=os.environ.get("INGESTOR_SSH_TARGET", DEFAULT_INGESTOR_SSH_TARGET),
        help="SSH target for the VPS-hosted chess-ingestor service; empty disables the check",
    )
    parser.add_argument("--ingestor-lookback-minutes", type=int, default=10)
    parser.add_argument("--skip-kafka-advance", action="store_true")
    parser.add_argument("--skip-ingestor", action="store_true")
    parser.add_argument("--skip-serving", action="store_true", help="skip StarRocks serving table checks")
    parser.add_argument("--skip-critical-positions", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dates = args.dates or default_dates()
    local_airflow_cli = os.environ.get("PIPELINE_HEALTH_LOCAL_AIRFLOW") == "1"
    results: list[CheckResult] = []

    try:
        pods = pod_json()
        scheduler_pod = None if local_airflow_cli else find_pod(pods, "airflow-scheduler-")
        minio_pod = find_pod(pods, "minio-")

        if not args.skip_kafka_advance:
            results.append(check_kafka_offsets(args.offset_wait_s))
        if not args.skip_ingestor:
            results.append(
                check_ingestor_delivery_failures(
                    args.ingestor_ssh_target or None,
                    args.ingestor_lookback_minutes,
                )
            )
        results.extend(check_minio_partitions(minio_pod, args.bucket, dates))
        results.extend(check_airflow_runs(scheduler_pod, args.airflow_lookback_hours, local_airflow_cli))
        if not args.skip_serving:
            results.extend(check_serving_rows(scheduler_pod, dates, local_airflow_cli))
        if not args.skip_critical_positions:
            results.extend(check_critical_positions_integrity(scheduler_pod, dates, local_airflow_cli))
        results.append(check_failed_pods(pods))
        results.append(check_spark_workers(pods))
        results.append(check_analyzer_backlog(pods, args.analyzer_backlog_threshold))
    except Exception as exc:
        results.append(CheckResult("pipeline_health_runtime", "FAIL", str(exc)))

    return print_results(results)


if __name__ == "__main__":
    sys.exit(main())
