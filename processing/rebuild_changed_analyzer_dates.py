import argparse
import os
import subprocess

import psycopg2


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "chess_analyzer_db"


ICEBERG_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
)


DERIVED_BUILDERS = [
    "/git/repo/processing/build_critical_positions.py",
    "/git/repo/processing/build_player_weakness_summary.py",
    "/git/repo/processing/build_player_opening_stats.py",
    "/git/repo/processing/build_player_phase_stats.py",
]

ANALYZER_TABLES = [
    "move_evaluations_ondemand",
    "critical_positions",
    "player_weakness_summary",
    "player_opening_stats",
    "player_phase_stats",
]


def starrocks_mysql_command(sql: str) -> list[str]:
    command = [
        "mysql",
        "--connect-timeout=30",
        "-h",
        os.getenv("STARROCKS_HOST", "starrocks-fe"),
        "-P",
        os.getenv("STARROCKS_PORT", "9030"),
        "-u",
        os.getenv("STARROCKS_USER", "root"),
        "-N",
        "-B",
    ]
    password = os.getenv("STARROCKS_PASSWORD")
    if password:
        command.append(f"-p{password}")
    command.extend(["-e", sql])
    return command


def ensure_change_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analyzer_partition_changes (
            date DATE PRIMARY KEY,
            first_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            last_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            new_eval_rows BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            processed_at TIMESTAMPTZ,
            error TEXT
        )
        """
    )


def claim_pending_dates(cur, max_dates: int) -> list[str]:
    ensure_change_table(cur)
    cur.execute(
        """
        SELECT date
        FROM analyzer_partition_changes
        WHERE status IN ('pending', 'failed')
        ORDER BY date
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (max_dates,),
    )
    dates = [str(row[0]) for row in cur.fetchall()]
    if not dates:
        return []

    cur.execute(
        """
        UPDATE analyzer_partition_changes
        SET status = 'processing',
            error = NULL
        WHERE date = ANY(%s::date[])
        """,
        (dates,),
    )
    return dates


def spark_submit_command(script: str, date_str: str) -> list[str]:
    return [
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--packages",
        ICEBERG_PACKAGES,
        "--conf",
        "spark.driver.host=airflow-scheduler",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        "--conf",
        "spark.driver.extraJavaOptions=-Daws.region=us-east-1",
        "--conf",
        "spark.executor.extraJavaOptions=-Daws.region=us-east-1",
        "--conf",
        "spark.executorEnv.AWS_REGION=us-east-1",
        "--conf",
        "spark.cores.max=4",
        "--conf",
        "spark.executor.instances=2",
        "--conf",
        "spark.executor.cores=2",
        "--conf",
        "spark.executor.memory=2g",
        "--conf",
        "spark.executor.memoryOverhead=512m",
        "--conf",
        "spark.driver.memory=2g",
        "--conf",
        "spark.driver.memoryOverhead=512m",
        "--conf",
        "spark.rpc.lookupTimeout=300s",
        "--conf",
        "spark.network.timeout=300s",
        "--conf",
        "spark.executor.heartbeatInterval=60s",
        "--conf",
        "spark.executorEnv.PYSPARK_PYTHON=python3.13",
        script,
        date_str,
    ]


def starrocks_query(sql: str) -> list[list[str]]:
    result = subprocess.run(
        starrocks_mysql_command(sql),
        check=True,
        text=True,
        capture_output=True,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def refresh_starrocks_tables() -> None:
    for table in ANALYZER_TABLES:
        subprocess.run(
            starrocks_mysql_command(f"REFRESH EXTERNAL TABLE polaris_catalog.prod.{table};"),
            check=True,
        )


def starrocks_scalar(sql: str) -> int:
    rows = starrocks_query(sql)
    if not rows:
        return 0
    return int(rows[0][0])


def starrocks_row(sql: str) -> list[int]:
    rows = starrocks_query(sql)
    if not rows:
        return []
    return [int(value) for value in rows[0]]


def validate_date(date_str: str) -> None:
    critical_duplicates = starrocks_scalar(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT game_id, ply, player_id, COUNT(*) AS c
            FROM polaris_catalog.prod.critical_positions
            WHERE date = DATE '{date_str}'
            GROUP BY game_id, ply, player_id
            HAVING COUNT(*) > 1
        ) d
        """
    )
    if critical_duplicates:
        raise RuntimeError(f"critical_positions duplicate keys for {date_str}: {critical_duplicates}")

    weakness_duplicates = starrocks_scalar(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT player_id, date, COUNT(*) AS c
            FROM polaris_catalog.prod.player_weakness_summary
            WHERE date = DATE '{date_str}'
            GROUP BY player_id, date
            HAVING COUNT(*) > 1
        ) d
        """
    )
    if weakness_duplicates:
        raise RuntimeError(
            f"player_weakness_summary duplicate player/date keys for {date_str}: "
            f"{weakness_duplicates}"
        )

    critical_players, summary_players = starrocks_row(
        f"""
        SELECT
            (SELECT COUNT(DISTINCT player_id)
             FROM polaris_catalog.prod.critical_positions
             WHERE date = DATE '{date_str}') AS critical_players,
            (SELECT COUNT(*)
             FROM polaris_catalog.prod.player_weakness_summary
             WHERE date = DATE '{date_str}') AS summary_players
        """,
    )
    if critical_players != summary_players:
        raise RuntimeError(
            f"player_weakness_summary coverage mismatch for {date_str}: "
            f"critical_players={critical_players} summary_players={summary_players}"
        )


def mark_clean(cur, date_str: str) -> None:
    cur.execute(
        """
        UPDATE analyzer_partition_changes
        SET status = 'clean',
            processed_at = now(),
            error = NULL
        WHERE date = %s
        """,
        (date_str,),
    )


def mark_failed(cur, date_str: str, error: str) -> None:
    cur.execute(
        """
        UPDATE analyzer_partition_changes
        SET status = 'failed',
            error = %s
        WHERE date = %s
        """,
        (error[:2000], date_str),
    )


def process_date(cur, date_str: str) -> None:
    try:
        for script in DERIVED_BUILDERS:
            subprocess.run(spark_submit_command(script, date_str), check=True)
        refresh_starrocks_tables()
        validate_date(date_str)
    except Exception as exc:
        mark_failed(cur, date_str, str(exc))
        raise
    mark_clean(cur, date_str)


def run(max_dates: int) -> int:
    processed = 0
    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            dates = claim_pending_dates(cur, max_dates)
            conn.commit()
            for date_str in dates:
                try:
                    process_date(cur, date_str)
                except Exception:
                    conn.commit()
                    raise
                else:
                    conn.commit()
                    processed += 1
    return processed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-dates", type=int, default=4)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.max_dates)
