import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, current_timestamp
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType,
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
        StructField("game_id",       StringType()),
        StructField("id",            StringType()),
        StructField("timestamp",     StringType()),
        StructField("move",          StringType()),
        StructField("fen",           StringType()),
        StructField("white_clock",   IntegerType()),
        StructField("black_clock",   IntegerType()),
        StructField("move_number",   IntegerType()),
        StructField("game_phase",    StringType()),
        StructField("time_spent_s",  DoubleType()),
        StructField("time_pressure", BooleanType()),
        StructField("is_check",      BooleanType()),
        StructField("moves",         StringType()),
        StructField("speed",         StringType()),
        StructField("rated",         BooleanType()),
        StructField("variant",       StringType()),
    ]),
    "lichess.game_end": StructType([
        StructField("game_id",   StringType()),
        StructField("timestamp", StringType()),
        StructField("winner",    StringType()),
        StructField("status",    StringType()),
    ]),
}

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


def build_spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("chess-kafka-to-minio")
        .config("spark.jars.packages",                        SPARK_PACKAGES)
        .config("spark.sql.shuffle.partitions",               "4")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


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
            .withColumn("date", to_date(col("timestamp").cast("timestamp")))
            .withColumn("ingested_at", current_timestamp())
        )

        q = (
            df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path",               f"s3a://{BUCKET_DEV}/{topic_key}")
            .option("checkpointLocation", f"s3a://{BUCKET_DEV}/_checkpoints/{topic_key}")
            .partitionBy("date")
            .trigger(processingTime="10 minutes")
            .start()
        )

        queries.append(q)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
