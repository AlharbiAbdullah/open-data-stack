"""Spark Structured Streaming job for processing stock prices from Kafka."""

from pathlib import Path

import structlog

from open_data_stack.config import get_settings

logger = structlog.get_logger(__name__)


def create_spark_session(app_name: str = "StockPriceStreaming"):
    """
    Create a Spark session configured for streaming.

    Args:
        app_name: Name for the Spark application

    Returns:
        SparkSession instance
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("spark_session_created", app_name=app_name)
    return spark


def get_kafka_stream(spark, bootstrap_servers: str | None = None, topic: str | None = None):
    """
    Create a streaming DataFrame from Kafka.

    Args:
        spark: SparkSession instance
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic to consume

    Returns:
        Streaming DataFrame
    """
    settings = get_settings()
    servers = bootstrap_servers or settings.kafka_bootstrap_servers
    kafka_topic = topic or settings.kafka_topic_stock_prices

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    logger.info("kafka_stream_created", topic=kafka_topic)
    return df


def parse_stock_prices(df):
    """
    Parse JSON messages from Kafka into structured columns.

    Args:
        df: Raw Kafka streaming DataFrame

    Returns:
        Parsed DataFrame with stock price columns
    """
    from pyspark.sql.functions import col, from_json, to_timestamp
    from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

    # Define schema for stock price messages
    schema = StructType([
        StructField("stock_price_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("price", StringType(), False),
        StructField("volume", LongType(), True),
        StructField("bid", StringType(), True),
        StructField("ask", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("source", StringType(), True),
    ])

    parsed = (
        df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "kafka_key",
            "kafka_timestamp",
            col("data.stock_price_id").alias("stock_price_id"),
            col("data.symbol").alias("symbol"),
            col("data.price").cast(DecimalType(12, 4)).alias("price"),
            col("data.volume").alias("volume"),
            col("data.bid").cast(DecimalType(12, 4)).alias("bid"),
            col("data.ask").cast(DecimalType(12, 4)).alias("ask"),
            to_timestamp(col("data.timestamp")).alias("timestamp"),
            col("data.source").alias("source"),
        )
    )

    return parsed


def add_streaming_metrics(df):
    """
    Add real-time metrics using windowed aggregations.

    Args:
        df: Parsed stock price DataFrame

    Returns:
        DataFrame with additional metrics
    """
    from pyspark.sql.functions import avg, col, count, max, min, window

    # Add windowed aggregations (5-minute windows)
    windowed = (
        df.withWatermark("timestamp", "1 minute")
        .groupBy(
            col("symbol"),
            window(col("timestamp"), "5 minutes", "1 minute"),
        )
        .agg(
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            count("*").alias("trade_count"),
        )
    )

    return windowed


def write_to_console(df, output_mode: str = "append"):
    """
    Write streaming output to console (for debugging).

    Args:
        df: Streaming DataFrame
        output_mode: Output mode ('append', 'complete', 'update')

    Returns:
        StreamingQuery instance
    """
    query = (
        df.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("console_output_started", output_mode=output_mode)
    return query


def write_to_parquet(df, output_path: str, checkpoint_path: str):
    """
    Write streaming output to Parquet files.

    Args:
        df: Streaming DataFrame
        output_path: Path for output Parquet files
        checkpoint_path: Path for checkpoint data

    Returns:
        StreamingQuery instance
    """
    query = (
        df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="30 seconds")
        .partitionBy("symbol")
        .start()
    )

    logger.info("parquet_output_started", output_path=output_path)
    return query


def write_to_memory(df, table_name: str = "stock_prices"):
    """
    Write streaming output to in-memory table (for testing).

    Args:
        df: Streaming DataFrame
        table_name: Name of the in-memory table

    Returns:
        StreamingQuery instance
    """
    query = (
        df.writeStream.outputMode("append")
        .format("memory")
        .queryName(table_name)
        .trigger(processingTime="5 seconds")
        .start()
    )

    logger.info("memory_output_started", table_name=table_name)
    return query


def run_streaming_job(
    output_mode: str = "console",
    output_path: str | None = None,
    await_termination: bool = True,
):
    """
    Run the complete Spark Streaming job.

    Args:
        output_mode: Where to write output ('console', 'parquet', 'memory')
        output_path: Path for file output (required for 'parquet' mode)
        await_termination: Whether to block until termination

    Returns:
        StreamingQuery instance
    """
    logger.info("starting_spark_streaming_job", output_mode=output_mode)

    # Create Spark session
    spark = create_spark_session()

    # Read from Kafka
    kafka_df = get_kafka_stream(spark)

    # Parse messages
    parsed_df = parse_stock_prices(kafka_df)

    # Start output
    if output_mode == "console":
        query = write_to_console(parsed_df)
    elif output_mode == "parquet":
        if not output_path:
            output_path = str(Path("data") / "streaming_output")
        checkpoint_path = str(Path("data") / "checkpoints" / "streaming")
        query = write_to_parquet(parsed_df, output_path, checkpoint_path)
    elif output_mode == "memory":
        query = write_to_memory(parsed_df)
    else:
        raise ValueError(f"Unknown output mode: {output_mode}")

    logger.info("streaming_job_started", output_mode=output_mode)

    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("streaming_job_interrupted")
            query.stop()

    return query


def run_aggregation_job(await_termination: bool = True):
    """
    Run a streaming aggregation job with windowed metrics.

    Args:
        await_termination: Whether to block until termination

    Returns:
        StreamingQuery instance
    """
    logger.info("starting_aggregation_job")

    spark = create_spark_session("StockPriceAggregation")
    kafka_df = get_kafka_stream(spark)
    parsed_df = parse_stock_prices(kafka_df)
    aggregated_df = add_streaming_metrics(parsed_df)

    query = write_to_console(aggregated_df, output_mode="update")

    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("aggregation_job_interrupted")
            query.stop()

    return query


if __name__ == "__main__":
    run_streaming_job(output_mode="console")
