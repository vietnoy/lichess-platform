import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

load_dotenv()

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
        .appName("chess-build-player-games")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", "8")
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
        CREATE TABLE IF NOT EXISTS polaris.prod.player_games (
            game_id       STRING NOT NULL,
            player_id     STRING NOT NULL,
            color         STRING,
            opponent_id   STRING,
            my_rating     INT,
            opp_rating    INT,
            speed         STRING,
            perf          STRING,
            opening_eco   STRING,
            opening_name  STRING,
            winner        STRING,
            end_status    STRING,
            date          DATE NOT NULL,
            tournament_id STRING
        )
        USING iceberg
        PARTITIONED BY (date)
        """
    )


def run(date_str: str | None) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)

    # One row per game (use move_number=1 to dedupe). Iceberg parquet stats on
    # move_number make this filter cheap even across the full table.
    source = spark.table("polaris.prod.chess_move_events").where(col("move_number") == 1)
    if date_str:
        source = source.where(col("date") == lit(date_str))
        logger.info(f"Building player_games for date={date_str}")
    else:
        logger.info("Building player_games for ALL dates in chess_move_events")

    # chess_move_events stores date as STRING (lit("YYYY-MM-DD")); cast for the
    # DATE-partitioned player_games table.
    common = [
        col("speed"),
        col("perf"),
        col("opening_eco"),
        col("opening_name"),
        col("winner"),
        col("end_status"),
        to_date(col("date")).alias("date"),
        col("tournament_id"),
    ]

    white = source.select(
        col("game_id"),
        col("white_id").alias("player_id"),
        lit("white").alias("color"),
        col("black_id").alias("opponent_id"),
        col("white_rating").alias("my_rating"),
        col("black_rating").alias("opp_rating"),
        *common,
    ).where(col("player_id").isNotNull())

    black = source.select(
        col("game_id"),
        col("black_id").alias("player_id"),
        lit("black").alias("color"),
        col("white_id").alias("opponent_id"),
        col("black_rating").alias("my_rating"),
        col("white_rating").alias("opp_rating"),
        *common,
    ).where(col("player_id").isNotNull())

    # sortWithinPartitions puts each parquet file in player_id order, so the
    # min/max stats stored in the footer let StarRocks skip whole files when
    # filtering WHERE player_id=?. This is what makes the profile query fast.
    output = white.unionByName(black).sortWithinPartitions("player_id")
    output.writeTo("polaris.prod.player_games").overwritePartitions()
    logger.info("Done")
    spark.stop()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(arg)
