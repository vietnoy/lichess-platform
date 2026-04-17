import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType, StructField, StructType,
)

load_dotenv()

BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CLUSTER_API_KEY    = os.getenv("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.getenv("CLUSTER_API_SECRET")
MINIO_ENDPOINT     = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY   = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY   = os.getenv("MINIO_SECRET_KEY")
BUCKET_DEV         = os.getenv("MINIO_BUCKET_DEV")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{CLUSTER_API_KEY}" password="{CLUSTER_API_SECRET}";'
    ),
    "startingOffsets": "earliest",
    "failOnDataLoss":  "false",
}

SCHEMAS = {
    "lichess.game_start": StructType([
        StructField("game_id",       StringType()),
        StructField("timestamp",     StringType()),
        StructField("speed",         StringType()),
        StructField("rated",         BooleanType()),
        StructField("variant",       StringType()),
        StructField("white_id",      StringType()),
        StructField("white_rating",  IntegerType()),
        StructField("white_title",   StringType()),
        StructField("black_id",      StringType()),
        StructField("black_rating",  IntegerType()),
        StructField("black_title",   StringType()),
        StructField("source",        StringType()),
        StructField("tournament_id", StringType()),
    ]),
    "lichess.moves": StructType([
        StructField("id",          StringType()),
        StructField("rated",       BooleanType()),
        StructField("variant",     StringType()),
        StructField("speed",       StringType()),
        StructField("perf",        StringType()),
        StructField("createdAt",   LongType()),
        StructField("lastMoveAt",  LongType()),
        StructField("status",      StringType()),
        StructField("source",      StringType()),
        StructField("winner",      StringType()),
        StructField("moves",       StringType()),
        StructField("clocks",      StringType()),
        StructField("pgn",         StringType()),
        StructField("clock",       StringType()),
        StructField("players",     StringType()),
        StructField("opening",     StringType()),
    ]),
    "lichess.game_end": StructType([
        StructField("game_id",   StringType()),
        StructField("timestamp", StringType()),
        StructField("winner",    StringType()),
        StructField("status",    StringType()),
    ]),
}


def build_spark():
    return (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("chess-kafka-to-minio")
        .config("spark.sql.shuffle.partitions",               "4")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


logger = logging.getLogger(__name__)


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    for topic, schema in SCHEMAS.items():
        topic_key = topic.split(".")[-1]

        df = (
            spark.readStream
            .format("kafka")
            .options(**KAFKA_OPTIONS)
            .option("subscribe", topic)
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("d"))
            .select("d.*")
            .withColumn("ingested_at", current_timestamp())
        )

        if topic == "lichess.moves":
            df = df.withColumn("date", to_date((col("createdAt") / 1000).cast("timestamp")))
        else:
            df = df.withColumn("date", to_date(col("timestamp").cast("timestamp")))

        def write_batch(batch_df, batch_id, key=topic_key):
            count = batch_df.count()
            logger.info(f"[{key}] batch={batch_id} rows={count}")
            if count > 0:
                batch_df.write.mode("append").partitionBy("date").parquet(f"s3a://{BUCKET_DEV}/{key}")

        q = (
            df.writeStream
            .outputMode("append")
            .option("checkpointLocation", f"s3a://{BUCKET_DEV}/_checkpoints/{topic_key}")
            .trigger(processingTime="10 minutes")
            .foreachBatch(write_batch)
            .start()
        )

        queries.append(q)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
