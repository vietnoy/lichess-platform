"""One-shot bootstrap: seed analyzer_cursor from StarRocks player_games.

Invoke as: python -m services.analyzer.bootstrap
Designed to run as a k8s Job using the same image as the worker.
"""

from __future__ import annotations

import logging
import os
from contextlib import closing
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extensions import connection as Connection

log = logging.getLogger(__name__)

_ELIGIBILITY_SQL = """
SELECT player_id
FROM polaris_catalog.prod.player_games
WHERE player_id NOT REGEXP '(?i)bot|stockfish|maia|leela'
GROUP BY player_id
HAVING COUNT(*) >= 5 AND MAX(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
"""

def fetch_eligible_player_ids(sr: Any) -> list[str]:
    """Run the eligibility query against StarRocks; return a list of player_ids."""
    with closing(sr.cursor(dictionary=True)) as c:
        c.execute(_ELIGIBILITY_SQL)
        rows = c.fetchall()
    log.info("starrocks query returned %d eligible players", len(rows))
    return [r["player_id"] for r in rows]


def seed_analyzer_cursor(
    pg: "Connection", player_ids: list[str], batch_size: int = 1000
) -> int:
    """INSERT player_ids into analyzer_cursor, ignoring conflicts.

    Batches via executemany with explicit slicing so page_size is respected and
    no single round-trip sends >batch_size rows. Commits once at the end.
    Returns the number of rows attempted (not the number actually inserted).
    """
    if not player_ids:
        return 0

    sql = (
        "INSERT INTO analyzer_cursor (player_id, games_processed, throttle_until)"
        " VALUES (%s, 0, NULL)"
        " ON CONFLICT (player_id) DO NOTHING"
    )
    rows = [(pid,) for pid in player_ids]

    with pg.cursor() as c:
        for i in range(0, len(rows), batch_size):
            c.executemany(sql, rows[i : i + batch_size])

    pg.commit()
    log.info("inserted %d rows into analyzer_cursor (ON CONFLICT DO NOTHING)", len(player_ids))
    return len(player_ids)


def main() -> None:
    """Connect to both databases, run the eligibility query, seed the cursor table."""
    import mysql.connector  # noqa: PLC0415
    import psycopg2  # noqa: PLC0415

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    pg_dsn = os.environ["ANALYZER_PG_DSN"]
    sr_host = os.environ["STARROCKS_HOST"]
    sr_port = int(os.getenv("STARROCKS_PORT", "9030"))
    sr_user = os.getenv("STARROCKS_USER", "root")
    sr_password = os.getenv("STARROCKS_PASSWORD", "")

    sr = mysql.connector.connect(
        host=sr_host, port=sr_port, user=sr_user, password=sr_password,
        autocommit=True,
    )
    pg = psycopg2.connect(pg_dsn)

    try:
        player_ids = fetch_eligible_player_ids(sr)
        n = seed_analyzer_cursor(pg, player_ids)
        log.info("bootstrap complete: %d rows attempted", n)
    finally:
        sr.close()
        pg.close()


if __name__ == "__main__":
    main()
