"""Spark Structured Streaming entrypoint (Phase 2b scaffold).

For now this proves the Kafka -> Spark plumbing: it reads the vehicle_positions
topic, parses the ingester's envelope, and logs per-batch counts. Phases 2c/2d
replace the body of `_process_batch` with the ground-truth derivation and the
curated-Parquet write — the foreachBatch shape is deliberately kept so that
swap is local.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from services.stream_processor.config import StreamConfig, StreamConfigError
from services.stream_processor.session import build_spark_session

logger = logging.getLogger(__name__)

# The envelope the ingester wraps every Kafka message in (see
# services/ingester/models.py). We parse only the envelope here; the GTFS-RT
# payload schema is introduced in Phase 2c where the ground-truth logic needs it.
_ENVELOPE_SCHEMA = StructType(
    [
        StructField("ingested_at", StringType(), nullable=True),
        StructField("source_feed", StringType(), nullable=True),
        StructField("ingester_version", StringType(), nullable=True),
        StructField("feed_timestamp", LongType(), nullable=True),
    ]
)


def read_vehicle_positions(spark: SparkSession, config: StreamConfig) -> DataFrame:
    """Open the Kafka vehicle_positions topic as a streaming DataFrame.

    Returns a DataFrame with the parsed envelope fields plus the Kafka key.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.vehicle_positions_topic)
        .option("startingOffsets", config.starting_offsets)
        .load()
    )

    # Kafka value is the JSON the ingester produced: {"envelope": {...}, "payload": {...}}.
    return (
        raw.select(
            F.col("key").cast("string").alias("vehicle_key"),
            F.from_json(F.col("value").cast("string"), _wrapped_schema()).alias("msg"),
        )
        .select("vehicle_key", "msg.envelope.*")
    )


def _wrapped_schema() -> StructType:
    """Schema for the full Kafka message: an envelope plus an opaque payload."""
    return StructType(
        [
            StructField("envelope", _ENVELOPE_SCHEMA, nullable=True),
            # payload parsed in Phase 2c; kept as a raw string for now.
            StructField("payload", StringType(), nullable=True),
        ]
    )


def _process_batch(batch: DataFrame, epoch_id: int) -> None:
    """Per-micro-batch handler. Phase 2b: just count and sample."""
    count = batch.count()
    logger.info("Batch epoch=%d vehicle_position_records=%d", epoch_id, count)
    if count:
        batch.select("vehicle_key", "source_feed", "feed_timestamp").show(5, truncate=False)


def run(spark: SparkSession, config: StreamConfig) -> None:
    """Start the streaming query and block until termination."""
    stream = read_vehicle_positions(spark, config)
    query = (
        stream.writeStream.foreachBatch(_process_batch)
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    logger.info(
        "Streaming started topic=%s offsets=%s trigger=%s checkpoint=%s",
        config.vehicle_positions_topic,
        config.starting_offsets,
        config.trigger_interval,
        config.checkpoint_location,
    )
    query.awaitTermination()


def main() -> int:
    """Build the session and run the stream. Returns a process exit code."""
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    try:
        config = StreamConfig.from_env()
    except StreamConfigError as exc:
        print(f"FATAL stream-processor configuration error: {exc}", file=sys.stderr)
        return 1

    spark = build_spark_session(log_level=config.log_level)
    try:
        run(spark, config)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
