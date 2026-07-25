# Interview Demo Guide

## Will they test it live?

| What they ask               | How often  | What to prepare                                 |
|-----------------------------|------------|--------------------------------------------------|
| Architecture deep-dive      | Very common | Sync vs async, < 100ms path, failure modes       |
| Screen-share repo walkthrough | Common    | README, `main.py`, `rules.py`, `features.py`    |
| Live `curl` against running API | Occasional | Keep `docker compose up` running before call  |
| Full install from zero in 45 min | Rare   | Offer recorded demo + local commands instead     |
| Take-home extension         | Common     | Extend rules, add a feature, retrain model        |

---

## Before every interview call

```bash
# 1. Start the stack (do this 5 min before the call)
docker compose up -d

# 2. Verify API is healthy
curl -s http://localhost:8000/health | jq
# → {"status":"ok","model_backend":"xgboost","model_version":"xgb-..."}

# 3. Have these tabs open:
#    http://localhost:8000/docs      (Swagger UI)
#    http://localhost:3000           (Grafana admin/admin)
```

---

## 60-second architecture pitch

> I built a real-time payment fraud platform with two separate paths.
>
> The **sync path** is FastAPI + Redis + XGBoost + a rule engine.
> A payment request comes in, we look up velocity features from Redis in
> under 5ms, run them through XGBoost, apply 12 hard rules, and return
> APPROVE / REVIEW / BLOCK — all in under 100ms.
>
> The **async path** publishes every decision to Kafka.
> Spark Structured Streaming reads that topic and writes to a Delta Lake
> Medallion — Bronze for raw events, Silver for user aggregates which also
> feeds back into Redis, and Gold for BLOCK/REVIEW alerts.
>
> The key design decision: Spark is **not** on the scoring path.
> Spark's micro-batch minimum latency is 5-30 seconds.
> Pre-authorization requires under 100ms, so scoring must be synchronous.

---

## Live demo script (5 minutes)

```bash
# 1. Show health + model backend
curl -s http://localhost:8000/health | jq

# 2. Normal transaction → APPROVE
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{"txn_id":"demo-1","user_id":"user_1","amount":1200,
       "merchant_id":"merch_amazon","device_id":"dev_5",
       "ip_country":"IN","lat":12.97,"lon":77.59}' | jq .

# 3. Fraud transaction → BLOCK
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{"txn_id":"demo-2","user_id":"user_1","amount":250000,
       "merchant_id":"merch_unknown","device_id":"dev_NEW_xyz",
       "ip_country":"RU","lat":55.75,"lon":37.62}' | jq .

# Expected: decision=BLOCK, rules=[EXTREME_AMOUNT, NEW_DEVICE, FOREIGN_IP, GEO_ANOMALY]

# 4. Start synthetic traffic
docker compose --profile traffic up producer -d

# 5. Show Grafana — http://localhost:3000 → Fraud Detection Overview
```

---

## Top interview questions

**Q: Why is Spark not on the scoring path?**
Spark Structured Streaming has a minimum micro-batch latency of ~5 seconds.
Pre-authorization decisions need < 100ms, so scoring must be synchronous in-process.
Spark runs asynchronously to compute slow features (30-day averages) and persist
events to the Delta Lake.

**Q: How do you get exactly-once in the Delta path?**
Kafka delivers at-least-once. The Silver job uses `foreachBatch` with a checkpoint.
For Gold alerts I use `outputMode("append")` with Delta — Delta's transaction log
prevents duplicate files from concurrent writes.

**Q: What happens if Redis is empty (cold start / new user)?**
`avg_30d` key missing → `is_new_user = 1.0` and `avg_30d = 2500` (population median).
The `is_new_user` feature increases the model's fraud sensitivity for unknown users.
The geo home defaults to Bengaluru. The API never errors — it always returns a score.

**Q: How would you reduce false positives?**
Threshold tuning (currently 0.45 for REVIEW, 0.85 for BLOCK), shadow mode before
enforcing new rules, A/B test rule changes, weekly review of REVIEW queue outcomes.

**Q: How would you scale to 1M events/sec?**
Horizontally scale FastAPI replicas behind a load balancer, Redis Cluster for
sharded velocity counts, increase Kafka partitions, EMR Serverless for Spark
with auto-scaling, replace Python GIL-bound scorer with a Go/Rust sidecar.
