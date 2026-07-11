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

# The Spark 3.5 line bundles Hadoop 3.3.4 client libraries; hadoop-aws (the
# S3A filesystem) must match that version exactly or classloading fails.
_HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


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
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
) -> SparkSession:
    """Build a SparkSession in local[*] mode with Kafka (and optionally S3A).

    Args:
        app_name: Spark application name (shown in the Spark UI).
        log_level: Spark log4j level (Spark is very chatty at INFO).
        extra_packages: Additional Maven coordinates to resolve.
        s3_endpoint: Object-store endpoint. When set, the S3A filesystem is
            configured for it (MinIO locally; in AWS this stays unset and S3A
            uses the real S3 endpoint + IAM).
        s3_access_key: Object-store access key (required with s3_endpoint).
        s3_secret_key: Object-store secret key (required with s3_endpoint).

    Returns:
        A configured, started SparkSession.
    """
    packages = [kafka_package(), *extra_packages]
    if s3_endpoint:
        packages.append(_HADOOP_AWS_PACKAGE)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        # Local mode: a handful of shuffle partitions, not the 200 default.
        .config("spark.sql.shuffle.partitions", "4")
        # Treat timestamps consistently regardless of host timezone.
        .config("spark.sql.session.timeZone", "UTC")
    )

    if s3_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key or "")
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key or "")
            # MinIO serves buckets at the path level (http://host/bucket/...),
            # not as virtual hosts (http://bucket.host/...): required for MinIO.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    logger.info("Building SparkSession app=%s packages=%s", app_name, packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark
