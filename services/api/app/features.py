"""
Redis-backed feature store for real-time fraud scoring.

Two categories of features:
  HOT  (updated every request)  — velocity windows via Redis sorted sets
  WARM (updated by Spark job)   — 30-day aggregates, home location, merchant risk

Both are read in a single Redis pipeline to stay well under 5ms.
"""
from __future__ import annotations

import math
import time
from typing import Dict, Optional, Tuple

import redis.asyncio as aioredis

from .schemas import PaymentRequest

# Used when a user has no Redis history at all (true new user)
_COLD_START_AVG   = 2500.0
_COLD_START_HOME  = (12.9716, 77.5946)   # Bengaluru as default home


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two (lat, lon) points in km."""
    r = 6_371.0
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a  = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class FeatureStore:
    def __init__(self, redis_url: str) -> None:
        self._redis = aioredis.from_url(redis_url, decode_responses=True)

    async def close(self) -> None:
        await self._redis.aclose()

    async def get_and_update(self, tx: PaymentRequest) -> Dict[str, float]:
        """
        Fetch all features for one transaction and update velocity counters.
        Returns a flat dict ready for the ML model.
        """
        now      = time.time()
        user_key = f"vel:user:{tx.user_id}"
        dev_key  = f"user:{tx.user_id}:devices"
        avg_key  = f"feat:{tx.user_id}:avg_30d"
        home_key = f"feat:{tx.user_id}:home_latlon"
        mrisk_key = f"feat:merch:{tx.merchant_id}:fraud_rate_30d"

        # All reads in one round-trip
        pipe = self._redis.pipeline(transaction=False)
        pipe.zremrangebyscore(user_key, 0, now - 3600)            # drop stale
        pipe.zadd(user_key, {f"{tx.txn_id}:{tx.amount}": now})    # add this tx
        pipe.expire(user_key, 3600)
        pipe.zcount(user_key, now - 60,   now)                    # 1-min count
        pipe.zcount(user_key, now - 300,  now)                    # 5-min count
        pipe.zcount(user_key, now - 3600, now)                    # 1-hr count
        pipe.get(avg_key)
        pipe.get(home_key)
        pipe.sismember(dev_key, tx.device_id)
        pipe.get(mrisk_key)
        results = await pipe.execute()

        txn_count_1m  = int(results[3])
        txn_count_5m  = int(results[4])
        txn_count_1h  = int(results[5])
        avg_raw       = results[6]
        home_raw: Optional[str] = results[7]
        device_known  = bool(results[8])
        merch_risk_raw = results[9]

        is_new_user = avg_raw is None
        avg_30d     = float(avg_raw) if avg_raw else _COLD_START_AVG
        amount_ratio = tx.amount / avg_30d if avg_30d > 0 else 1.0

        # Geographic distance from user's known home location
        if home_raw:
            hlat, hlon = map(float, home_raw.split(","))
        else:
            hlat, hlon = _COLD_START_HOME
            # First time we see this user — set their home as this location
            if tx.lat and tx.lon:
                await self._redis.setex(home_key, 86400, f"{tx.lat},{tx.lon}")

        geo_km = 0.0
        if tx.lat and tx.lon:
            geo_km = _haversine_km(tx.lat, tx.lon, hlat, hlon)

        # Mark this device as known (30-day TTL)
        await self._redis.sadd(dev_key, tx.device_id)
        await self._redis.expire(dev_key, 86400 * 30)

        return {
            "amount":           float(tx.amount),
            "txn_count_1m":     float(txn_count_1m),
            "txn_count_5m":     float(txn_count_5m),
            "txn_count_1h":     float(txn_count_1h),
            "amount_ratio":     float(amount_ratio),
            "avg_30d":          float(avg_30d),
            "new_device":       0.0 if device_known else 1.0,
            "is_new_user":      1.0 if is_new_user else 0.0,
            "geo_distance_km":  float(geo_km),
            "is_foreign_ip":    0.0 if tx.ip_country == "IN" else 1.0,
            "merch_risk_30d":   float(merch_risk_raw or 0.03),
        }

    async def upsert_batch_features(
        self,
        user_id:  str,
        avg_30d:  float,
        home:     Optional[Tuple[float, float]] = None,
        merch_id: Optional[str]  = None,
        merch_fraud_rate: Optional[float] = None,
    ) -> None:
        """Called by the Spark Silver job to push slowly-changing features."""
        pipe = self._redis.pipeline(transaction=False)
        pipe.setex(f"feat:{user_id}:avg_30d", 86400, str(avg_30d))
        if home:
            pipe.setex(f"feat:{user_id}:home_latlon", 86400, f"{home[0]},{home[1]}")
        if merch_id and merch_fraud_rate is not None:
            pipe.setex(f"feat:merch:{merch_id}:fraud_rate_30d", 86400, str(merch_fraud_rate))
        await pipe.execute()
