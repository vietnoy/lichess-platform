import argparse
import os
import subprocess

import psycopg2

from rebuild_changed_analyzer_dates import ensure_change_table, starrocks_mysql_command


POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "chess_analyzer_db"


def starrocks_rows(sql: str) -> list[list[str]]:
    result = subprocess.run(
        starrocks_mysql_command(sql),
        check=True,
        text=True,
        capture_output=True,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def stale_dates_sql(lookback_days: int | None) -> str:
    eval_date_filter = ""
    if lookback_days:
        eval_date_filter = f"AND date >= DATE_SUB(CURDATE(), INTERVAL {lookback_days} DAY)"

    return f"""
    WITH eval_source AS (
        SELECT date, COUNT(*) AS eval_teachable_keys
        FROM (
            SELECT date, game_id, ply, player_id
            FROM polaris_catalog.prod.move_evaluations_ondemand
            WHERE classification IN ('blunder', 'mistake', 'inaccuracy')
            {eval_date_filter}
            GROUP BY date, game_id, ply, player_id
        ) e
        GROUP BY date
    ),
    critical AS (
        SELECT
            date,
            COUNT(*) AS critical_rows,
            COUNT(DISTINCT player_id) AS critical_players
        FROM polaris_catalog.prod.critical_positions
        GROUP BY date
    ),
    summary AS (
        SELECT date, COUNT(*) AS summary_players
        FROM polaris_catalog.prod.player_weakness_summary
        GROUP BY date
    )
    SELECT
        e.date
    FROM eval_source e
    LEFT JOIN critical c ON e.date = c.date
    LEFT JOIN summary s ON e.date = s.date
    WHERE e.eval_teachable_keys <> COALESCE(c.critical_rows, 0)
       OR COALESCE(c.critical_players, 0) <> COALESCE(s.summary_players, 0)
    ORDER BY e.date
    """


def find_stale_dates(lookback_days: int | None) -> list[str]:
    return [row[0] for row in starrocks_rows(stale_dates_sql(lookback_days))]


def enqueue_dates(cur, dates: list[str]) -> None:
    ensure_change_table(cur)
    for date_str in dates:
        cur.execute(
            """
            INSERT INTO analyzer_partition_changes (
                date,
                first_changed_at,
                last_changed_at,
                new_eval_rows,
                status,
                processed_at,
                error
            )
            VALUES (%s, now(), now(), 0, 'pending', NULL, NULL)
            ON CONFLICT (date) DO UPDATE SET
                last_changed_at = now(),
                status = 'pending',
                processed_at = NULL,
                error = NULL
            """,
            (date_str,),
        )


def run(lookback_days: int | None) -> int:
    dates = find_stale_dates(lookback_days)
    if not dates:
        return 0

    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            enqueue_dates(cur, dates)
    return len(dates)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=90)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.lookback_days)
