"""
Silver Layer - Sanitization & Schema Enforcement (PySpark)
Parses raw JSON strings from Bronze, applies schema enforcement, deduplicates events
over a tumbling watermark window, and materializes trusted conversational records.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_date,
    to_timestamp,
    coalesce,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DoubleType,
)

# Explicit schema matching the OmnichannelMessageEvent Pydantic model
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("conversation_id", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("direction", StringType(), False),
    StructField("status", StringType(), False),
    StructField("message_type", StringType(), True),
    StructField("content_length", IntegerType(), True),
    StructField("timestamp", StringType(), False),
    StructField("user_id_hash", StringType(), False),
    StructField("bot_id", StringType(), False),
    StructField("is_handoff_to_human", BooleanType(), True),
    StructField("error_code", StringType(), True),
    StructField(
        "llm_telemetry",
        StructType([
            StructField("model_name", StringType(), True),
            StructField("prompt_tokens", IntegerType(), True),
            StructField("completion_tokens", IntegerType(), True),
            StructField("latency_ms", DoubleType(), True),
            StructField("intent_detected", StringType(), True),
            StructField("confidence_score", DoubleType(), True),
        ]),
        True,
    ),
])


def get_spark_session(app_name: str = "Silver-Sanitization") -> SparkSession:
    """Initializes Spark Session for Delta Lake operations."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )


def process_bronze_to_silver(
    spark: SparkSession,
    bronze_path: str,
    silver_output_path: str,
    checkpoint_location: str,
):
    """
    Transforms Bronze raw records into cleaned, typed Silver Delta tables.
    """
    print(f"[*] Reading Bronze stream from: {bronze_path}")

    # Read stream from Bronze Delta table
    bronze_stream = (
        spark.readStream.format("delta")
        .load(bronze_path)
    )

    # Parse JSON payload against contract schema
    parsed_df = (
        bronze_stream.withColumn("parsed", from_json(col("raw_payload"), EVENT_SCHEMA))
        .select("parsed.*", "ingested_at")
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    # Apply 15-minute watermark and deduplicate late-arriving events
    deduped_df = (
        parsed_df.withWatermark("event_timestamp", "15 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
    )

    # Flatten nested LLM telemetry for query optimization
    silver_df = deduped_df.select(
        col("event_id"),
        col("conversation_id"),
        col("channel"),
        col("direction"),
        col("status"),
        col("message_type"),
        col("content_length"),
        col("event_timestamp"),
        col("event_date"),
        col("user_id_hash"),
        col("bot_id"),
        col("is_handoff_to_human"),
        col("error_code"),
        col("llm_telemetry.model_name").alias("llm_model"),
        col("llm_telemetry.prompt_tokens").alias("llm_prompt_tokens"),
        col("llm_telemetry.completion_tokens").alias("llm_completion_tokens"),
        col("llm_telemetry.latency_ms").alias("llm_latency_ms"),
        col("llm_telemetry.intent_detected").alias("detected_intent"),
        col("llm_telemetry.confidence_score").alias("intent_confidence"),
        col("ingested_at"),
    )

    # Materialize to Silver Delta table partitioned by channel and date
    query = (
        silver_df.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("channel", "event_date")
        .option("checkpointLocation", checkpoint_location)
        .start(silver_output_path)
    )

    return query


if __name__ == "__main__":
    print("[*] Silver transform module ready for streaming pipeline execution.")
