"""
Synthetic payment traffic generator.
Calls the scoring API directly (not Kafka) so you can watch decisions in real time.

Run locally:   python services/producer/producer.py
In Docker:     docker compose --profile traffic up producer
"""
from __future__ import annotations

import asyncio
import os
import random
import time
import uuid

import httpx

API_URL        = os.getenv("API_URL",        "http://localhost:8000/v1/score")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))
DURATION_SEC   = int(os.getenv("DURATION_SEC",  "0"))     # 0 = run forever

USERS     = [f"user_{i}" for i in range(1, 501)]
MERCHANTS = ["merch_amazon", "merch_uber", "merch_swiggy", "merch_flipkart", "merch_zomato"]
DEVICES   = [f"dev_{i}" for i in range(1, 80)]


def _build_event(force_fraud: bool = False) -> dict:
    fraud = force_fraud or random.random() < 0.03
    user  = random.choice(USERS)

    if fraud:
        amount = random.uniform(25_000, 90_000)
        device = f"dev_new_{uuid.uuid4().hex[:8]}"
        lat, lon = random.uniform(40, 55), random.uniform(-5, 30)   # Europe
        ip_country = random.choice(["RU", "CN", "NG", "RO"])
    else:
        amount = random.lognormvariate(6.5, 0.7)
        device = random.choice(DEVICES)
        lat    = 12.97 + random.gauss(0, 0.3)
        lon    = 77.59 + random.gauss(0, 0.3)
        ip_country = "IN"

    return {
        "txn_id":     f"txn_{uuid.uuid4().hex[:12]}",
        "user_id":    user,
        "amount":     round(amount, 2),
        "merchant_id": random.choice(MERCHANTS),
        "device_id":  device,
        "ip_country": ip_country,
        "lat":        round(lat, 4),
        "lon":        round(lon, 4),
    }


async def main() -> None:
    delay = 1.0 / max(EVENTS_PER_SEC, 0.01)
    start = time.time()

    async with httpx.AsyncClient(timeout=5.0) as client:
        # Wait for API to come up
        for attempt in range(60):
            try:
                r = await client.get(API_URL.replace("/v1/score", "/health"))
                if r.status_code == 200:
                    print(f"API ready: {r.json()}")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("API did not come up in 60s")
            return

        sent = blocks = reviews = 0
        while True:
            if DURATION_SEC > 0 and time.time() - start > DURATION_SEC:
                break
            payload = _build_event()
            try:
                resp = await client.post(API_URL, json=payload)
                body = resp.json()
                d    = body.get("decision", "?")
                if d == "BLOCK":  blocks  += 1
                if d == "REVIEW": reviews += 1
                print(
                    f"{payload['txn_id']}  {payload['amount']:>10.2f}  "
                    f"{d:<8}  score={body.get('ml_score'):.3f}  "
                    f"rules={body.get('triggered_rules')}  "
                    f"{body.get('latency_ms')}ms"
                )
            except Exception as exc:
                print(f"error: {exc}")
            sent += 1
            await asyncio.sleep(delay)

    print(f"\nSent {sent}  BLOCK={blocks}  REVIEW={reviews}  other={sent-blocks-reviews}")


if __name__ == "__main__":
    asyncio.run(main())
