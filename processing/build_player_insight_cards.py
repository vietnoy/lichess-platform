from __future__ import annotations

import logging
import os
import sys

from dotenv import find_dotenv, load_dotenv
from pyspark.sql import SparkSession

load_dotenv(find_dotenv(usecwd=True))

MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
POLARIS_URI        = os.getenv("POLARIS_URI")
POLARIS_CREDENTIAL = f"{os.getenv('POLARIS_ETL_CLIENT_ID')}:{os.getenv('POLARIS_ETL_CLIENT_SECRET')}"
POLARIS_WAREHOUSE  = os.getenv("POLARIS_WAREHOUSE")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.iceberg:iceberg-aws-bundle:1.5.0",
    ]
    return (
        SparkSession.builder
        .appName("chess-build-player-insight-cards")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", POLARIS_URI)
        .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_WAREHOUSE)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.polaris.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS polaris.prod.player_insight_cards (
            player_id     STRING NOT NULL,
            as_of_date    DATE   NOT NULL,
            window_days   INT    NOT NULL,
            rank          INT    NOT NULL,
            insight_type  STRING NOT NULL,
            score         INT,
            title         STRING,
            evidence      STRING,
            action        STRING,
            data_json     STRING,
            updated_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (as_of_date)
        """
    )


def latest_source_date_sql() -> str:
    return """
    SELECT CAST(MIN(max_date) AS STRING) AS latest_source_date
    FROM (
        SELECT MAX(date) AS max_date FROM polaris.prod.player_weakness_summary
        UNION ALL
        SELECT MAX(date) AS max_date FROM polaris.prod.player_phase_stats
        UNION ALL
        SELECT MAX(date) AS max_date FROM polaris.prod.player_opening_stats
        UNION ALL
        SELECT MAX(date) AS max_date FROM polaris.prod.player_games
    ) source_dates
    """


def latest_source_date(spark: SparkSession) -> str | None:
    rows = spark.sql(latest_source_date_sql()).collect()
    if not rows:
        return None

    value = rows[0]["latest_source_date"]
    return str(value) if value else None


def resolve_as_of_date(requested_date: str | None, source_date: str | None) -> str | None:
    if not source_date:
        return None

    if not requested_date or requested_date == "--latest-source":
        return source_date

    if requested_date > source_date:
        logger.warning(
            "Requested player_insight_cards as_of_date=%s is newer than source data date=%s; using source date",
            requested_date,
            source_date,
        )
        return source_date

    return requested_date


def delete_future_partitions(spark: SparkSession, source_date: str) -> None:
    spark.sql(
        f"DELETE FROM polaris.prod.player_insight_cards WHERE as_of_date > DATE '{source_date}'"
    )
    logger.info("Cleared player_insight_cards partitions newer than source date=%s", source_date)


def build_player_insight_cards_sql(as_of_date: str) -> str:
    return f"""
    WITH windows AS (
        SELECT explode(array(14, 30, 60, 0)) AS window_days
    ),
    weakness AS (
        SELECT
            w.window_days,
            s.player_id,
            COUNT(*) AS days_with_summary,
            SUM(s.critical_positions) AS critical_positions,
            SUM(s.blunders) AS blunders,
            SUM(s.mistakes) AS mistakes,
            SUM(s.inaccuracies) AS inaccuracies,
            SUM(s.time_pressure_positions) AS time_pressure_positions,
            ROUND(AVG(s.avg_eval_swing_cp), 1) AS avg_eval_swing_cp
        FROM windows w
        JOIN polaris.prod.player_weakness_summary s
          ON s.date <= DATE '{as_of_date}'
         AND (w.window_days = 0 OR s.date >= date_sub(DATE '{as_of_date}', w.window_days - 1))
        GROUP BY w.window_days, s.player_id
    ),
    phase_rollup AS (
        SELECT
            w.window_days,
            s.player_id,
            s.phase,
            SUM(s.games_with_positions) AS games_with_positions,
            SUM(s.critical_positions) AS critical_positions,
            SUM(s.blunders) AS blunders,
            SUM(s.mistakes) AS mistakes,
            SUM(s.inaccuracies) AS inaccuracies,
            SUM(s.time_pressure_positions) AS time_pressure_positions,
            ROUND(AVG(s.avg_eval_swing_cp), 1) AS avg_eval_swing_cp,
            MAX(s.max_eval_swing_cp) AS max_eval_swing_cp
        FROM windows w
        JOIN polaris.prod.player_phase_stats s
          ON s.date <= DATE '{as_of_date}'
         AND (w.window_days = 0 OR s.date >= date_sub(DATE '{as_of_date}', w.window_days - 1))
        GROUP BY w.window_days, s.player_id, s.phase
    ),
    phase_ranked AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY window_days, player_id
                ORDER BY critical_positions DESC, blunders DESC, mistakes DESC, phase
            ) AS rn
        FROM phase_rollup
    ),
    phase_cards AS (
        SELECT
            player_id,
            DATE '{as_of_date}' AS as_of_date,
            window_days,
            CAST(1 AS INT) AS rank,
            'phase_weakness' AS insight_type,
            CAST(LEAST(100, 35 + critical_positions * 2 + blunders * 5 + mistakes * 2) AS INT) AS score,
            concat('Bạn đang mất điểm nhiều nhất ở ', phase) AS title,
            concat(CAST(critical_positions AS STRING), ' critical positions, ', CAST(blunders AS STRING), ' blunders và ', CAST(mistakes AS STRING), ' mistakes.') AS evidence,
            'Ưu tiên drill theo phase này trước khi học thêm opening mới.' AS action,
            to_json(named_struct(
                'phase', phase,
                'critical_positions', critical_positions,
                'blunders', blunders,
                'mistakes', mistakes,
                'inaccuracies', inaccuracies,
                'time_pressure_positions', time_pressure_positions,
                'avg_eval_swing_cp', avg_eval_swing_cp,
                'max_eval_swing_cp', max_eval_swing_cp
            )) AS data_json
        FROM phase_ranked
        WHERE rn = 1 AND critical_positions > 0
    ),
    opening_rollup AS (
        SELECT
            w.window_days,
            s.player_id,
            COALESCE(s.opening_eco, '') AS opening_eco,
            COALESCE(s.opening_name, 'Unknown') AS opening_name,
            s.color,
            SUM(s.games) AS games,
            SUM(s.wins) AS wins,
            SUM(s.losses) AS losses,
            SUM(s.draws) AS draws,
            ROUND(SUM(s.wins) * 100.0 / NULLIF(SUM(s.games), 0), 1) AS win_rate_pct,
            SUM(s.critical_positions) AS critical_positions,
            SUM(s.blunders) AS blunders,
            SUM(s.mistakes) AS mistakes,
            SUM(s.inaccuracies) AS inaccuracies,
            ROUND(AVG(s.avg_eval_swing_cp), 1) AS avg_eval_swing_cp
        FROM windows w
        JOIN polaris.prod.player_opening_stats s
          ON s.date <= DATE '{as_of_date}'
         AND (w.window_days = 0 OR s.date >= date_sub(DATE '{as_of_date}', w.window_days - 1))
        GROUP BY w.window_days, s.player_id, COALESCE(s.opening_eco, ''), COALESCE(s.opening_name, 'Unknown'), s.color
        HAVING SUM(s.games) >= 2
    ),
    opening_ranked AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY window_days, player_id
                ORDER BY blunders DESC, mistakes DESC, critical_positions DESC, games DESC, opening_name
            ) AS rn
        FROM opening_rollup
        WHERE critical_positions > 0 OR win_rate_pct < 45
    ),
    opening_cards AS (
        SELECT
            player_id,
            DATE '{as_of_date}' AS as_of_date,
            window_days,
            CAST(10 + rn AS INT) AS rank,
            'opening_leak' AS insight_type,
            CAST(LEAST(100, 25 + games * 2 + critical_positions * 3 + blunders * 6 + mistakes * 2 + CASE WHEN win_rate_pct < 45 THEN 15 ELSE 0 END) AS INT) AS score,
            concat(CASE WHEN opening_eco = '' THEN '-' ELSE opening_eco END, ' · ', opening_name, ' cần review') AS title,
            concat(CAST(games AS STRING), ' games, win rate ', CAST(win_rate_pct AS STRING), '%, ', CAST(critical_positions AS STRING), ' critical positions.') AS evidence,
            'Review 3-5 game gần nhất trong opening này rồi tạo drill từ các critical positions.' AS action,
            to_json(named_struct(
                'opening_eco', NULLIF(opening_eco, ''),
                'opening_name', opening_name,
                'color', color,
                'games', games,
                'wins', wins,
                'losses', losses,
                'draws', draws,
                'win_rate_pct', win_rate_pct,
                'critical_positions', critical_positions,
                'blunders', blunders,
                'mistakes', mistakes,
                'inaccuracies', inaccuracies,
                'avg_eval_swing_cp', avg_eval_swing_cp
            )) AS data_json
        FROM opening_ranked
        WHERE rn <= 3
    ),
    time_pressure_cards AS (
        SELECT
            player_id,
            DATE '{as_of_date}' AS as_of_date,
            window_days,
            CAST(20 AS INT) AS rank,
            'time_pressure' AS insight_type,
            CAST(LEAST(100, 30 + time_pressure_positions * 4) AS INT) AS score,
            'Time pressure đang tạo lỗi đáng kể' AS title,
            concat(CAST(time_pressure_positions AS STRING), '/', CAST(critical_positions AS STRING), ' critical positions xảy ra dưới áp lực thời gian.') AS evidence,
            'Tập drill có timer và ưu tiên quyết định candidate moves nhanh hơn.' AS action,
            to_json(named_struct(
                'time_pressure_positions', time_pressure_positions,
                'critical_positions', critical_positions,
                'share_pct', ROUND(time_pressure_positions * 100.0 / NULLIF(critical_positions, 0), 1)
            )) AS data_json
        FROM weakness
        WHERE critical_positions > 0
          AND (ROUND(time_pressure_positions * 100.0 / NULLIF(critical_positions, 0), 1) >= 20 OR time_pressure_positions >= 5)
    ),
    color_stats AS (
        SELECT
            w.window_days,
            g.player_id,
            g.color,
            COUNT(*) AS games,
            SUM(CASE
                WHEN (g.color = 'White' AND g.winner = 'white')
                  OR (g.color = 'Black' AND g.winner = 'black')
                THEN 1 ELSE 0
            END) AS wins
        FROM windows w
        JOIN polaris.prod.player_games g
          ON g.date <= DATE '{as_of_date}'
         AND (w.window_days = 0 OR g.date >= date_sub(DATE '{as_of_date}', w.window_days - 1))
        GROUP BY w.window_days, g.player_id, g.color
    ),
    color_pivot AS (
        SELECT
            window_days,
            player_id,
            MAX(CASE WHEN color = 'White' THEN games ELSE 0 END) AS white_games,
            MAX(CASE WHEN color = 'Black' THEN games ELSE 0 END) AS black_games,
            MAX(CASE WHEN color = 'White' THEN ROUND(wins * 100.0 / NULLIF(games, 0), 1) ELSE NULL END) AS white_win_pct,
            MAX(CASE WHEN color = 'Black' THEN ROUND(wins * 100.0 / NULLIF(games, 0), 1) ELSE NULL END) AS black_win_pct
        FROM color_stats
        GROUP BY window_days, player_id
    ),
    color_cards AS (
        SELECT
            player_id,
            DATE '{as_of_date}' AS as_of_date,
            window_days,
            CAST(30 AS INT) AS rank,
            'color_gap' AS insight_type,
            CAST(LEAST(100, 25 + CAST(ABS(white_win_pct - black_win_pct) AS INT) + LEAST(white_games, black_games)) AS INT) AS score,
            CASE WHEN white_win_pct < black_win_pct
                THEN 'Hiệu suất cầm White thấp hơn rõ rệt'
                ELSE 'Hiệu suất cầm Black thấp hơn rõ rệt'
            END AS title,
            concat('Win rate lệch ', CAST(ROUND(ABS(white_win_pct - black_win_pct), 1) AS STRING), ' điểm phần trăm giữa White và Black.') AS evidence,
            'So sánh repertoire và chọn một opening ổn định hơn cho màu quân yếu.' AS action,
            to_json(named_struct(
                'white_games', white_games,
                'black_games', black_games,
                'white_win_pct', white_win_pct,
                'black_win_pct', black_win_pct,
                'gap_pct', ROUND(ABS(white_win_pct - black_win_pct), 1)
            )) AS data_json
        FROM color_pivot
        WHERE white_games >= 5
          AND black_games >= 5
          AND ABS(white_win_pct - black_win_pct) >= 15
    ),
    cards AS (
        SELECT * FROM phase_cards
        UNION ALL
        SELECT * FROM opening_cards
        UNION ALL
        SELECT * FROM time_pressure_cards
        UNION ALL
        SELECT * FROM color_cards
    ),
    ranked_cards AS (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY player_id, window_days
                ORDER BY score DESC, rank ASC, insight_type
            ) AS final_rank
        FROM cards
    )
    SELECT
        player_id,
        as_of_date,
        CAST(window_days AS INT) AS window_days,
        CAST(final_rank AS INT) AS rank,
        insight_type,
        CAST(score AS INT) AS score,
        title,
        evidence,
        action,
        data_json,
        current_timestamp() AS updated_at
    FROM ranked_cards
    WHERE final_rank <= 6
    """


def run(as_of_date: str | None) -> int:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_table(spark)
        source_date = latest_source_date(spark)
        resolved_as_of_date = resolve_as_of_date(as_of_date, source_date)
        if not resolved_as_of_date:
            logger.warning("No source data found for player_insight_cards; skipping build")
            return 0

        delete_future_partitions(spark, source_date)
        as_of_date = resolved_as_of_date
        logger.info("Building player_insight_cards for as_of_date=%s", as_of_date)
        output = spark.sql(build_player_insight_cards_sql(as_of_date))
        row_count = output.count()
        logger.info("player_insight_cards output rows=%s", row_count)
        if row_count == 0:
            spark.sql(f"DELETE FROM polaris.prod.player_insight_cards WHERE as_of_date = DATE '{as_of_date}'")
            logger.info("cleared empty player_insight_cards partition for as_of_date=%s", as_of_date)
            return 0

        output.writeTo("polaris.prod.player_insight_cards").overwritePartitions()
        logger.info("Done")
        return row_count
    finally:
        spark.stop()


def resolve_date_arg(argv: list[str]) -> str | None:
    return argv[1] if len(argv) > 1 else None


if __name__ == "__main__":
    run(resolve_date_arg(sys.argv))
