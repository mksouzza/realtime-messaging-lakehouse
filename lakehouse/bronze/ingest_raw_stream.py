"""
Bronze Layer - Raw Stream Ingestion (PySpark Structured Streaming)
Ingests unparsed JSON events from Kafka / PubSub and persists raw payloads
into the Bronze Delta Lake storage layer with append-only guarantees.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, to_date


def get_spark_session(app_name: str = "Bronze-Raw-Ingestion") -> SparkSession:
    """Initializes a Spark Session with Delta Lake and cloud connectors enabled."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .master("local[*]")
        .getOrCreate()
    )


def stream_kafka_to_bronze(
    bootstrap_servers: str,
    topic: str,
    bronze_output_path: str,
    checkpoint_location: str,
):
    """
    Consumes raw messages from Kafka and appends them to the Bronze Delta Lake.

    Args:
        bootstrap_servers: Kafka broker connection string.
        topic: Ingestion event topic.
        bronze_output_path: Storage destination URI (GCS or local).
        checkpoint_location: Checkpoint path for fault tolerance.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Starting Bronze ingestion stream from topic '{topic}'...")

    # Read stream from Kafka broker
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Extract raw value and append ingestion audit metadata
    bronze_df = (
        kafka_df.selectExpr(
            "CAST(key AS STRING) as message_key",
            "CAST(value AS STRING) as raw_payload",
            "timestamp as event_kafka_timestamp",
            "topic",
            "partition",
            "offset",
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("ingested_at")))
    )

    # Write stream to Bronze Delta table partitioned by date
    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", checkpoint_location)
        .start(bronze_output_path)
    )

    return query


if __name__ == "__main__":
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC_MESSAGING_EVENTS", "blip.omnichannel.events.v1")
    bronze_path = os.getenv("BRONZE_DELTA_PATH", "./data/lakehouse/bronze")
    checkpoint_path = os.getenv("BRONZE_CHECKPOINT_PATH", "./data/checkpoints/bronze")

    print(f"Target Bronze Path: {bronze_path}")
    print(f"Kafka Source: {kafka_brokers} | Topic: {kafka_topic}")
