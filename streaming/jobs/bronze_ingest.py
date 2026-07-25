"""
ASYNC PATH — Job 1: Kafka decisions → Delta Bronze (raw, immutable).

spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/jobs/bronze_ingest.py
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from conf.spark_conf import BRONZE_PATH, CKPT_BASE, KAFKA_BOOT, KAFKA_TOPIC, get_spark

spark = get_spark("fraud-bronze-ingest")
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOT)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Keep raw JSON + kafka metadata — bronze is always append-only
bronze = raw.select(
    F.col("timestamp").alias("kafka_ts"),
    F.col("value").cast("string").alias("payload"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
).withColumn("ingest_date", F.to_date("kafka_ts"))

(
    bronze.writeStream
    .format("delta")
    .option("checkpointLocation", CKPT_BASE + "/bronze")
    .partitionBy("ingest_date")
    .outputMode("append")
    .start(BRONZE_PATH)
    .awaitTermination()
)
