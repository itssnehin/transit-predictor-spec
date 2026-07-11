"""SparkSession construction for local[*] mode.

Isolated from the application logic so the session config (master URL, connector
packages, tuning) lives in one place and can be swapped for a cluster master URL
in the cloud without touching the streaming code.
"""

from __future__ import annotations

import logging

import pyspark
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# Spark 3.5 pip builds are compiled against Scala 2.12.
_SCALA_VERSION = "2.12"


def kafka_package(spark_version: str = pyspark.__version__) -> str:
    """Return the Maven coordinate for the Kafka connector matching this Spark.

    Pinning the connector to the running PySpark version avoids the classic
    "connector built for a different Spark" runtime failure.
    """
    return f"org.apache.spark:spark-sql-kafka-0-10_{_SCALA_VERSION}:{spark_version}"


def build_spark_session(
    app_name: str = "transit-stream-processor",
    *,
    log_level: str = "WARN",
    extra_packages: tuple[str, ...] = (),
) -> SparkSession:
    """Build a SparkSession in local[*] mode with the Kafka connector.

    Args:
        app_name: Spark application name (shown in the Spark UI).
        log_level: Spark log4j level (Spark is very chatty at INFO).
        extra_packages: Additional Maven coordinates to resolve (e.g. hadoop-aws
            for S3A in later phases).

    Returns:
        A configured, started SparkSession.
    """
    packages = ",".join((kafka_package(), *extra_packages))

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", packages)
        # Local mode: a handful of shuffle partitions, not the 200 default.
        .config("spark.sql.shuffle.partitions", "4")
        # Treat timestamps consistently regardless of host timezone.
        .config("spark.sql.session.timeZone", "UTC")
    )

    logger.info("Building SparkSession app=%s packages=%s", app_name, packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
