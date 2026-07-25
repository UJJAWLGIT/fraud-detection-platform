# Real-Time Fraud Detection Platform

Real-time payment fraud scoring with < 100ms latency.

**Sync path**: FastAPI + Redis + XGBoost + rules → APPROVE / REVIEW / BLOCK  
**Async path**: Kafka + Spark Structured Streaming + Delta Lake Medallion

[![CI](https://github.com/UJJAWLGIT/fraud-detection-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/UJJAWLGIT/fraud-detection-platform/actions)

---

## Architecture

```
Payment  →  POST /v1/score
                ↓
           Redis  (velocity features, < 5ms)
                ↓
           XGBoost model  (fraud probability, < 10ms)
                ↓
           Rule engine  (12 hard rules, < 1ms)
                ↓
           APPROVE / REVIEW / BLOCK  (total < 100ms)
                ↓ (async, non-blocking)
           Kafka → Spark → Delta Lake (Bronze / Silver / Gold)
```

Spark is **not** on the scoring path — micro-batch latency is seconds, not ms.

---

## Quick start

```bash
# 1. Train model
pip install numpy pandas scikit-learn xgboost joblib
cd ml && python train_xgboost.py && cd ..

# 2. Start everything
docker compose up --build -d

# 3. Verify
curl -s http://localhost:8000/health | jq

# 4. Score a normal transaction
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{"txn_id":"t1","user_id":"u1","amount":1200,
       "merchant_id":"merch_amazon","device_id":"dev_5",
       "ip_country":"IN","lat":12.97,"lon":77.59}' | jq .

# 5. Score a suspicious transaction (should BLOCK)
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{"txn_id":"t2","user_id":"u1","amount":250000,
       "merchant_id":"merch_unknown","device_id":"dev_NEW_xyz",
       "ip_country":"RU","lat":55.75,"lon":37.62}' | jq .
```

---

## Services

| Service     | URL                        | Notes                    |
|-------------|----------------------------|--------------------------|
| API         | http://localhost:8000/docs | Swagger UI               |
| Grafana     | http://localhost:3000      | admin / admin            |
| Prometheus  | http://localhost:9090      |                          |
| MinIO       | http://localhost:9001      | minioadmin / minioadmin  |

---

## Stack

| Layer        | Technology                  |
|--------------|-----------------------------|
| API          | FastAPI + aiokafka (async)  |
| Feature store| Redis 7 (sorted sets, TTL)  |
| ML model     | XGBoost 2 → sklearn → heuristic fallback |
| Streaming    | Spark Structured Streaming  |
| Lake         | Delta Lake (Bronze/Silver/Gold) |
| Storage      | MinIO (S3-compatible, local) |
| Monitoring   | Prometheus + Grafana        |
| CI/CD        | GitHub Actions              |

---

## Load test

```bash
pip install locust
locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 50 -r 10 -t 60s
```

Local results (MacBook M2): P50 ~12ms · P95 ~28ms · P99 ~50ms

---

## Author

Ujjawl Kumar — Senior Data Engineer  
[github.com/UJJAWLGIT](https://github.com/UJJAWLGIT) · [linkedin.com/in/theujjawlkumar](https://linkedin.com/in/theujjawlkumar)
