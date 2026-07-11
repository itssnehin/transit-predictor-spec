"""Spark Structured Streaming entrypoint: Kafka -> ground truth -> Parquet.

The full Phase 2 pipeline. Spark owns Kafka ingestion, offset checkpointing,
micro-batch scheduling, and the Parquet sink; the ground-truth derivation
runs on the driver (ADR 0011) via :class:`ArrivalPipeline`, which holds the
cross-batch trip state.

Each micro-batch:

1. Collect the batch's raw JSON strings to the driver. At Brisbane scale
   (~3,000 records/min peak, 60 s trigger) a batch is a few thousand small
   dicts — well within driver memory, and the deliberate consequence of the
   driver-side-state decision.
2. Feed them to ArrivalPipeline (vehicle positions + trip-update cancels).
3. Write any derived arrivals to the curated layer as Parquet, partitioned
   by dt (UTC date of observed_arrival_ts):
   ``s3a://transit-curated/curated/arrivals/dt=YYYY-MM-DD/``
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from services.stream_processor.config import StreamConfig, StreamConfigError
from services.stream_processor.pipeline import ARRIVAL_COLUMNS, ArrivalPipeline, event_to_row
from services.stream_processor.schedule_repo import ScheduleRepository
from services.stream_processor.session import build_spark_session

logger = logging.getLogger(__name__)

# Spark schema for the curated arrivals table (docs/DATA.md), in
# ARRIVAL_COLUMNS order. `dt` is the partition column.
_ARRIVALS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("trip_id", StringType(), nullable=False),
        StructField("route_id", StringType(), nullable=False),
        StructField("stop_id", StringType(), nullable=False),
        StructField("stop_sequence", IntegerType(), nullable=False),
        StructField("scheduled_arrival_ts", TimestampType(), nullable=False),
        StructField("observed_arrival_ts", TimestampType(), nullable=False),
        StructField("observed_delay_s", IntegerType(), nullable=False),
        StructField("observed_delay_imputed", BooleanType(), nullable=False),
        StructField("day_of_week", IntegerType(), nullable=False),
        StructField("hour_of_day", IntegerType(), nullable=False),
        # Nullable until the Queensland calendar CSVs exist (flagged spec gap).
        StructField("is_school_day", BooleanType(), nullable=True),
        StructField("is_public_holiday", BooleanType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
        StructField("dt", StringType(), nullable=False),
    ]
)

assert tuple(f.name for f in _ARRIVALS_SCHEMA.fields) == ARRIVAL_COLUMNS


def read_stream(spark: SparkSession, config: StreamConfig) -> DataFrame:
    """Open both GTFS-RT topics as one streaming DataFrame of JSON strings."""
    topics = f"{config.vehicle_positions_topic},{config.trip_updates_topic}"
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", config.starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) AS raw")
    )


def run(spark: SparkSession, config: StreamConfig) -> None:
    """Wire the pipeline and block on the streaming query."""
    repo = ScheduleRepository(config.pg_conninfo)
    repo.open()
    pipeline = ArrivalPipeline(
        repo, watermark=timedelta(minutes=config.watermark_minutes)
    )

    def process_batch(batch: DataFrame, epoch_id: int) -> None:
        raw_rows = [row["raw"] for row in batch.collect()]
        records = []
        bad = 0
        for raw in raw_rows:
            try:
                records.append(json.loads(raw))
            except json.JSONDecodeError:
                bad += 1
        if bad:
            logger.warning("Batch epoch=%d skipped %d undecodable records", epoch_id, bad)

        events = pipeline.process_records(records)
        logger.info(
            "Batch epoch=%d records=%d arrivals=%d active_trips=%d",
            epoch_id,
            len(records),
            len(events),
            pipeline.active_trips,
        )
        if not events:
            return

        rows = [event_to_row(e) for e in events]
        (
            spark.createDataFrame(rows, schema=_ARRIVALS_SCHEMA)
            # One file per partition per batch: avoids the small-file problem
            # at our scale (spec implementation note).
            .coalesce(1)
            .write.mode("append")
            .partitionBy("dt")
            .parquet(config.arrivals_path)
        )
        logger.info(
            "Batch epoch=%d wrote %d arrivals to %s", epoch_id, len(rows), config.arrivals_path
        )

    query = (
        read_stream(spark, config)
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", config.checkpoint_location)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    logger.info(
        "Streaming started topics=%s,%s offsets=%s trigger=%s sink=%s",
        config.vehicle_positions_topic,
        config.trip_updates_topic,
        config.starting_offsets,
        config.trigger_interval,
        config.arrivals_path,
    )
    try:
        query.awaitTermination()
    finally:
        repo.close()


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

    spark = build_spark_session(
        log_level=config.log_level,
        s3_endpoint=config.s3_endpoint,
        s3_access_key=config.s3_access_key,
        s3_secret_key=config.s3_secret_key,
    )
    try:
        run(spark, config)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
