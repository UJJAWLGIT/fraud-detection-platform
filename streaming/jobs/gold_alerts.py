"""
ASYNC PATH — Job 3: Filter BLOCK/REVIEW decisions → Delta Gold fraud_alerts table.
Analysts query this table for fraud investigation and model retraining.
"""
from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from conf.spark_conf import GOLD_PATH, CKPT_BASE, KAFKA_BOOT, KAFKA_TOPIC, get_spark

spark = get_spark("fraud-gold-alerts")
spark.sparkContext.setLogLevel("WARN")

SCHEMA = """
    txn_id STRING, user_id STRING, amount DOUBLE, merchant_id STRING,
    decision STRING, ml_score DOUBLE, triggered_rules ARRAY<STRING>,
    latency_ms DOUBLE, model_version STRING
"""

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOT)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

alerts = (
    raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
    .select("e.*")
    .filter(F.col("decision").isin("BLOCK", "REVIEW"))
    .withColumn("alert_date", F.current_date())
)

(
    alerts.writeStream
    .format("delta")
    .option("checkpointLocation", CKPT_BASE + "/gold")
    .partitionBy("alert_date")
    .outputMode("append")
    .start(GOLD_PATH)
    .awaitTermination()
)
