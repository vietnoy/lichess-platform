"""One-off: rebuild the player watchlist from raw Iceberg parquet in MinIO.

Bypasses Polaris (currently broken on credential vending). Reads data files
directly, computes per-player min/max date, writes a single CSV back to MinIO.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

spark = (
    SparkSession.builder.appName("recover_players")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

src = "s3a://chess-prod/iceberg/prod/chess_move_events/data/"
df = spark.read.parquet(src)

whites = df.select(F.col("white_id").alias("player"), F.col("date"))
blacks = df.select(F.col("black_id").alias("player"), F.col("date"))
both = whites.unionByName(blacks).where(F.col("player").isNotNull())

result = (
    both.groupBy("player")
    .agg(F.min("date").alias("first_seen"), F.max("date").alias("last_seen"))
    .orderBy("player")
)

count = result.count()
print(f"[recover_players] distinct players: {count}")

result.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3a://chess-prod/recovery/players_csv/"
)
print("[recover_players] wrote s3a://chess-prod/recovery/players_csv/")
spark.stop()
