"""
Locust load test for /v1/score.

Run:
  locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 50 -r 10 -t 60s
  # or open http://localhost:8089 for the UI
"""
from __future__ import annotations

import random
import uuid
from locust import HttpUser, between, task


class FraudUser(HttpUser):
    wait_time = between(0.01, 0.05)

    @task(95)
    def score_normal(self):
        self.client.post("/v1/score", json={
            "txn_id":      f"txn_{uuid.uuid4().hex[:12]}",
            "user_id":     f"user_{random.randint(1, 500)}",
            "amount":      round(random.lognormvariate(6.5, 0.8), 2),
            "merchant_id": random.choice(["merch_amazon", "merch_uber", "merch_swiggy"]),
            "device_id":   f"dev_{random.randint(1, 80)}",
            "ip_country":  "IN",
            "lat":         12.97 + random.gauss(0, 0.2),
            "lon":         77.59 + random.gauss(0, 0.2),
        }, name="/v1/score [normal]")

    @task(5)
    def score_suspicious(self):
        self.client.post("/v1/score", json={
            "txn_id":      f"txn_{uuid.uuid4().hex[:12]}",
            "user_id":     f"user_{random.randint(1, 50)}",
            "amount":      round(random.uniform(60_000, 200_000), 2),
            "merchant_id": "merch_unknown",
            "device_id":   f"dev_new_{uuid.uuid4().hex[:6]}",
            "ip_country":  random.choice(["RU", "CN", "NG"]),
            "lat":         random.uniform(48, 56),
            "lon":         random.uniform(10, 40),
        }, name="/v1/score [suspicious]")
