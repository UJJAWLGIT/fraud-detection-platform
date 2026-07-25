"""Shared Spark session for all streaming jobs."""
from __future__ import annotations

import os
from pyspark.sql import SparkSession

KAFKA_BOOT  = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC",     "payment-decisions")
MINIO_URL   = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_USER  = os.getenv("MINIO_USER",      "minioadmin")
MINIO_PASS  = os.getenv("MINIO_PASS",      "minioadmin")
REDIS_URL   = os.getenv("REDIS_URL",       "redis://localhost:6379/0")

BRONZE_PATH = "s3a://fraud-platform/bronze/decisions"
SILVER_PATH = "s3a://fraud-platform/silver/user_features"
GOLD_PATH   = "s3a://fraud-platform/gold/fraud_alerts"
CKPT_BASE   = "s3a://fraud-platform/checkpoints"


def get_spark(app: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.shuffle.partitions",                  "20")
        .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_URL)
        .config("spark.hadoop.fs.s3a.access.key",                MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",                MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
