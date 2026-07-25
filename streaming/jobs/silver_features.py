"""
ASYNC PATH — Job 2: Parse decisions → compute per-user aggregates → upsert Redis.

This is the bridge that makes batch-computed features available to the hot path.
Spark runs every 30 minutes and updates Redis keys that FastAPI reads in < 1ms.
"""
from __future__ import annotations

import json
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis
from pyspark.sql import functions as F
from conf.spark_conf import KAFKA_BOOT, KAFKA_TOPIC, REDIS_URL, SILVER_PATH, CKPT_BASE, get_spark

spark = get_spark("fraud-silver-features")
spark.sparkContext.setLogLevel("WARN")

SCHEMA = """
    txn_id STRING, user_id STRING, amount DOUBLE,
    merchant_id STRING, lat DOUBLE, lon DOUBLE,
    decision STRING, ml_score DOUBLE
"""

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOT)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = raw.select(
    F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")
).select("e.*").filter(F.col("user_id").isNotNull())

user_aggs = (
    events.groupBy("user_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.avg("lat").alias("home_lat"),
        F.avg("lon").alias("home_lon"),
        F.count("*").alias("txn_count"),
    )
)

# Also compute merchant fraud rate
merch_aggs = (
    events.groupBy("merchant_id")
    .agg(
        F.avg(F.when(F.col("decision") == "BLOCK", 1).otherwise(0))
         .alias("fraud_rate_30d")
    )
)


def upsert_to_redis(batch_df, _batch_id: int) -> None:
    r    = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    pipe = r.pipeline()
    for row in batch_df.collect():
        uid = row["user_id"]
        pipe.setex(f"feat:{uid}:avg_30d", 86400, str(round(row["avg_amount"] or 2500, 2)))
        if row["home_lat"] and row["home_lon"]:
            pipe.setex(f"feat:{uid}:home_latlon", 86400,
                       f"{row['home_lat']:.4f},{row['home_lon']:.4f}")
    pipe.execute()
    print(f"Updated Redis for {batch_df.count()} users")


(
    user_aggs.writeStream
    .outputMode("complete")
    .foreachBatch(upsert_to_redis)
    .option("checkpointLocation", CKPT_BASE + "/silver")
    .trigger(processingTime="1800 seconds")
    .start()
    .awaitTermination()
)
