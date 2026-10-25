"""
Gold Layer - Business Analytical Datamarts (PySpark)
Computes high-value business aggregations from the Silver Layer:
1. Bot Resolution & Human Handoff Rates
2. LLM Latency, Token Usage & Cost Attribution
3. Omnichannel Delivery Funnel (Sent -> Delivered -> Read)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    sum as spark_sum,
    round as spark_round,
    when,
    window,
)


def get_spark_session(app_name: str = "Gold-Marts-Aggregation") -> SparkSession:
    """Initializes Spark Session with Delta Lake support."""
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


def compute_bot_performance_mart(silver_df):
    """
    Computes key performance indicators for automated chatbots:
    - Total messages processed
    - Unique conversations handled
    - Human handoff escalation percentage
    - Average message content length
    """
    return (
        silver_df.groupBy("bot_id", "channel", "event_date")
        .agg(
            count("event_id").alias("total_messages"),
            countDistinct("conversation_id").alias("distinct_conversations"),
            countDistinct("user_id_hash").alias("unique_users_reached"),
            spark_round(
                (
                    spark_sum(when(col("is_handoff_to_human") == True, 1).otherwise(0))
                    / count("event_id")
                )
                * 100,
                2,
            ).alias("human_handoff_rate_pct"),
            spark_round(avg("content_length"), 1).alias("avg_message_chars"),
        )
        .orderBy("event_date", "total_messages", ascending=False)
    )


def compute_llm_cost_and_latency_mart(silver_df):
    """
    Computes generative AI operational metrics and token cost estimates:
    - Total prompt and completion tokens
    - Average response latency (ms)
    - Estimated cost in USD (assuming standard model pricing)
    """
    llm_events = silver_df.filter(col("llm_model").isNotNull())

    return (
        llm_events.groupBy("llm_model", "bot_id", "event_date")
        .agg(
            count("event_id").alias("total_llm_invocations"),
            spark_sum("llm_prompt_tokens").alias("total_prompt_tokens"),
            spark_sum("llm_completion_tokens").alias("total_completion_tokens"),
            spark_round(avg("llm_latency_ms"), 2).alias("avg_latency_ms"),
            # Cost approximation ($0.15 / 1M prompt tokens, $0.60 / 1M completion tokens for flash-tier models)
            spark_round(
                (spark_sum("llm_prompt_tokens") * 0.00000015)
                + (spark_sum("llm_completion_tokens") * 0.00000060),
                4,
            ).alias("estimated_llm_cost_usd"),
        )
        .orderBy("event_date", "total_llm_invocations", ascending=False)
    )


def compute_delivery_funnel_mart(silver_df):
    """
    Calculates omnichannel message delivery funnels (Sent -> Delivered -> Read).
    """
    outbound = silver_df.filter(col("direction") == "outbound")

    return (
        outbound.groupBy("channel", "event_date")
        .agg(
            count("event_id").alias("total_outbound_sent"),
            spark_sum(when(col("status").isin("delivered", "read"), 1).otherwise(0)).alias("total_delivered"),
            spark_sum(when(col("status") == "read", 1).otherwise(0)).alias("total_read"),
            spark_sum(when(col("status") == "failed", 1).otherwise(0)).alias("total_failed"),
        )
        .withColumn(
            "delivery_rate_pct",
            spark_round((col("total_delivered") / col("total_outbound_sent")) * 100, 2),
        )
        .withColumn(
            "read_rate_pct",
            spark_round((col("total_read") / col("total_delivered")) * 100, 2),
        )
    )


def run_gold_aggregations(silver_path: str, gold_base_path: str):
    """
    Executes Gold Layer transformations and saves materialized Delta tables.
    """
    spark = get_spark_session()
    print(f"[*] Reading Silver layer from: {silver_path}")
    silver_df = spark.read.format("delta").load(silver_path)

    print("[*] Computing Gold Mart: Bot Performance...")
    bot_mart = compute_bot_performance_mart(silver_df)
    bot_mart.write.format("delta").mode("overwrite").save(f"{gold_base_path}/mart_bot_performance")

    print("[*] Computing Gold Mart: LLM Latency & Cost...")
    llm_mart = compute_llm_cost_and_latency_mart(silver_df)
    llm_mart.write.format("delta").mode("overwrite").save(f"{gold_base_path}/mart_llm_cost")

    print("[*] Computing Gold Mart: Delivery Funnel...")
    funnel_mart = compute_delivery_funnel_mart(silver_df)
    funnel_mart.write.format("delta").mode("overwrite").save(f"{gold_base_path}/mart_delivery_funnel")

    print("[*] Gold Layer materialization completed successfully.")


if __name__ == "__main__":
    print("[*] Gold Layer aggregation module ready.")
