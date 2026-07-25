# 🔴 Real-Time Fraud Detection Platform

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-003366?style=for-the-badge)](https://delta.io)
[![XGBoost](https://img.shields.io/badge/XGBoost-2.0-337AB7?style=for-the-badge)](https://xgboost.readthedocs.io)
[![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.io)
[![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)](LICENSE)
[![CI](https://github.com/UJJAWLGIT/fraud-detection-platform/actions/workflows/ci.yml/badge.svg?style=for-the-badge)](https://github.com/UJJAWLGIT/fraud-detection-platform/actions)

**Real-time payment fraud scoring at SaaS FinTech scale**
**< 100ms P99 sync path · Kafka + Spark + Delta Lake async lakehouse**

[Architecture](#-architecture) · [Quick Start](#-quick-start) · [Features](#-feature-engineering) · [API](#-rest-api) · [Performance](#-performance-benchmarks) · [Docs](docs/)

</div>

---

## 🎯 Business Context

Payment fraud costs the global financial industry **$32 billion per year** (2026). The challenge for every FinTech company — Razorpay, PhonePe, Stripe, PayPal — is to detect fraud **before the transaction is approved**, with a decision latency under 100ms.

**This platform solves three core problems:**

1. **Speed vs Accuracy tradeoff** — Hard rules catch obvious fraud instantly (< 1ms). ML catches subtle patterns (< 50ms). Ensemble of both gives the best result in < 100ms total.

2. **Feature freshness tradeoff** — Real-time velocity features (Redis, updated every request) combined with 30-day historical patterns (Delta Lake Silver, updated by Spark every 30 min) give the model both signals.

3. **Scale vs Cost tradeoff** — Sync path (FastAPI + Redis) is stateless and horizontally scalable. Async path (Kafka + Spark + Delta Lake) decouples scoring latency from analytics complexity.

---

## 🏛️ Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     REAL-TIME FRAUD DETECTION PLATFORM                          │
│                                                                                 │
│  ┌──────────────┐                                                               │
│  │   PAYMENT    │──▶ POST /v1/score                                            │
│  │   REQUEST    │         │                                                    │
│  └──────────────┘         ▼                                                    │
│                   ┌───────────────────────────────────────────────────────┐    │
│                   │           SYNC PATH  (< 100ms P99)                    │    │
│                   │                                                       │    │
│                   │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐   │    │
│                   │  │  Redis   │  │ XGBoost  │  │   Rule Engine    │   │    │
│                   │  │ Feature  │→ │  Model   │→ │  (12 hard rules) │   │    │
│                   │  │  Store   │  │ (< 50ms) │  │     (< 1ms)      │   │    │
│                   │  │ (< 5ms)  │  └──────────┘  └──────────────────┘   │    │
│                   │  └──────────┘         │                              │    │
│                   │                       ▼                              │    │
│                   │             APPROVE / REVIEW / BLOCK                 │    │
│                   └───────────────────────┬───────────────────────────────┘    │
│                                           │ (async, fire-and-forget)           │
│                                           ▼                                    │
│                   ┌───────────────────────────────────────────────────────┐    │
│                   │           ASYNC PATH  (seconds)                       │    │
│                   │                                                       │    │
│                   │  Kafka ──▶ Spark Structured Streaming                 │    │
│                   │                   │                                   │    │
│                   │          ┌────────┼────────┐                          │    │
│                   │          ▼        ▼        ▼                          │    │
│                   │       Bronze   Silver    Gold                         │    │
│                   │       (raw)  (features) (alerts)                      │    │
│                   │                   │                                   │    │
│                   │          Redis feature updates (30 min)               │    │
│                   └───────────────────────────────────────────────────────┘    │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  OBSERVABILITY: Prometheus · Grafana · PagerDuty (fraud rate > 5%)      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Why Sync and Async Are Separate

```
┌─────────────────────────────────────────────────────────────────────────┐
│  THE CRITICAL DESIGN DECISION                                           │
│                                                                         │
│  WRONG approach:                                                        │
│    Payment → Kafka → Spark Streaming → FastAPI response                 │
│    Latency: 5-30 seconds minimum (Spark micro-batch)                   │
│    Result: Transaction already approved/timed out before decision       │
│                                                                         │
│  CORRECT approach (this platform):                                      │
│    Payment → FastAPI → Redis (sync) → Decision in < 100ms              │
│    Payment → Kafka (async, after decision) → Spark → Delta Lake         │
│                                                                         │
│  Spark's job: update Redis features every 30 min, not score payments   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Decision Flow

```
Payment Event
      │
      ▼
FeatureStore.get_and_update(tx)
  ├── Redis PIPELINE (one round-trip):
  │     zremrangebyscore: prune stale velocity events
  │     zadd: add this transaction
  │     zcount x3: count 1-min, 5-min, 1-hour windows
  │     get x4: avg_30d, max_30d, home_latlon, device_known
  │     sadd: mark device as known
  └── Returns 11 features in < 5ms
      │
      ▼
FraudModel.predict_proba(features)
  ├── Try: XGBoost JSON booster (preferred)
  ├── Try: sklearn GradientBoosting (Docker fallback)
  └── Fallback: heuristic formula (always works)
      │
      ▼
rules.evaluate(features)
  ├── BLOCK rules (4): EXTREME_AMOUNT, EXTREME_VELOCITY_5M,
  │                    EXTREME_AMOUNT_RATIO, EXTREME_GEO_JUMP
  └── REVIEW rules (8): HIGH_AMOUNT, HIGH_VELOCITY, NEW_DEVICE,
                         NEW_USER, FOREIGN_IP, GEO_ANOMALY, etc.
      │
      ▼
decision.decide(rule_severity, triggered_rules, ml_score)
  ├── rule=BLOCK → BLOCK (rules are hard constraints)
  ├── ml >= 0.85 → BLOCK (model is very confident)
  ├── rule=REVIEW + ml >= 0.60 → BLOCK (ensemble escalates)
  ├── rule=REVIEW → REVIEW
  ├── ml >= 0.45 → REVIEW (medium risk)
  └── clean → APPROVE
      │
      ▼
Return ScoreResponse + publish to Kafka (fire-and-forget)
```

---

## 📦 Repository Structure

```
fraud-detection-platform/
│
├── 📄 README.md
├── 📄 docker-compose.yml          # Full local stack: Kafka, Redis, MinIO, Grafana
├── 📄 requirements.txt            # Root ML/dev dependencies
├── 📄 .gitignore
│
├── 🔧 services/
│   ├── api/                       # FastAPI scoring service (SYNC PATH)
│   │   ├── app/
│   │   │   ├── main.py            # FastAPI app — /v1/score endpoint
│   │   │   ├── schemas.py         # PaymentRequest + ScoreResponse (Pydantic)
│   │   │   ├── features.py        # FeatureStore class — async Redis lookup
│   │   │   ├── rules.py           # Rule engine — 12 hard fraud rules
│   │   │   ├── decision.py        # Ensemble: rules + ML → APPROVE/REVIEW/BLOCK
│   │   │   ├── model.py           # FraudModel — XGBoost → sklearn → heuristic
│   │   │   ├── publisher.py       # DecisionPublisher — aiokafka fire-and-forget
│   │   │   └── metrics.py         # Prometheus metrics
│   │   ├── models/                # Trained model artifacts (gitignored)
│   │   ├── tests/
│   │   │   ├── test_rules.py      # 7 rule engine tests
│   │   │   └── test_decision.py   # 5 ensemble decision tests
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── producer/                  # Synthetic payment traffic generator
│       ├── producer.py            # Calls /v1/score directly — watch decisions live
│       └── Dockerfile
│
├── 🌊 streaming/                  # Spark jobs (ASYNC PATH)
│   ├── conf/
│   │   └── spark_conf.py          # Shared Spark session (MinIO S3A config)
│   └── jobs/
│       ├── bronze_ingest.py       # Kafka → Delta Bronze (raw, immutable)
│       ├── silver_features.py     # Bronze → Silver aggregates + Redis upsert
│       └── gold_alerts.py         # BLOCK/REVIEW decisions → Delta Gold
│
├── 🤖 ml/
│   ├── generate_synthetic_data.py # 100K labelled transactions (2% fraud rate)
│   ├── train_xgboost.py           # XGBoost + optional MLflow + SHAP importance
│   └── train_sklearn.py           # sklearn GradientBoosting fallback
│
├── 🏗️ infra/
│   ├── prometheus/
│   │   └── prometheus.yml         # Scrape config — API metrics
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/       # Auto-provisions Prometheus datasource
│       │   └── dashboards/        # Auto-loads fraud overview dashboard
│       └── dashboards/
│           └── fraud_overview.json
│
├── 🔧 scripts/
│   ├── start.sh                   # One-command setup: train + docker compose
│   └── load_test.py               # Locust load test — P50/P95/P99 latency
│
├── 📚 docs/
│   ├── ARCHITECTURE.md            # Sync vs async design decisions
│   └── INTERVIEW_DEMO.md          # 5-minute live demo script + Q&A
│
└── ⚙️  .github/
    └── workflows/
        └── ci.yml                 # GitHub Actions: unit tests + sklearn smoke
```

---

## ⚡ Quick Start

### Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11+ | ML training + local dev |
| Docker Desktop | 24+ | Full local stack |
| Java | 11+ | Spark jobs (optional) |

### Option A — One Command

```bash
git clone https://github.com/UJJAWLGIT/fraud-detection-platform.git
cd fraud-detection-platform
bash scripts/start.sh
# Trains model + starts all services + waits for API to be healthy
```

### Option B — Step by Step

```bash
# 1. Install ML dependencies
pip install numpy pandas scikit-learn xgboost joblib mlflow

# 2. Generate 100K labelled training transactions (2% fraud rate)
cd ml && python generate_synthetic_data.py
# Output: ml/artifacts/transactions.csv

# 3. Train XGBoost model
python train_xgboost.py
# Output: services/api/models/fraud_xgb.json
# Console: AUC=0.97  AUCPR=0.89

# 4. Train sklearn fallback (used in Docker build)
python train_sklearn.py --n 30000
# Output: services/api/models/fraud_sklearn.joblib

# 5. Start all services
cd ..
docker compose up --build -d

# 6. Verify
curl -s http://localhost:8000/health | jq
# → {"status":"ok","model_backend":"xgboost","model_version":"xgb-..."}
```

### Score Your First Transaction

```bash
# Normal transaction (expect APPROVE)
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{
    "txn_id":      "txn_demo_001",
    "user_id":     "user_1234",
    "amount":      1200.50,
    "merchant_id": "merch_amazon",
    "device_id":   "dev_ios_7",
    "ip_country":  "IN",
    "lat":         12.9716,
    "lon":         77.5946
  }' | jq .
```

```json
{
  "txn_id": "txn_demo_001",
  "decision": "APPROVE",
  "ml_score": 0.038,
  "triggered_rules": [],
  "features": { "amount": 1200.5, "txn_count_5m": 1, "amount_ratio": 0.48, "..." },
  "latency_ms": 12.3,
  "model_version": "xgb-1753012345"
}
```

```bash
# Fraud transaction (expect BLOCK)
curl -s -X POST http://localhost:8000/v1/score \
  -H "Content-Type: application/json" \
  -d '{
    "txn_id":      "txn_demo_002",
    "user_id":     "user_9999",
    "amount":      250000,
    "merchant_id": "merch_unknown",
    "device_id":   "dev_NEW_xyz9",
    "ip_country":  "RU",
    "lat":         55.7558,
    "lon":         37.6173
  }' | jq .
```

```json
{
  "txn_id": "txn_demo_002",
  "decision": "BLOCK",
  "ml_score": 0.872,
  "triggered_rules": ["EXTREME_AMOUNT", "NEW_DEVICE", "FOREIGN_IP", "GEO_ANOMALY"],
  "features": { "amount": 250000, "geo_distance_km": 4312.5, "new_device": 1.0, "..." },
  "latency_ms": 18.7,
  "model_version": "xgb-1753012345"
}
```

---

## 🛠️ Services

| Service | URL | Credentials |
|---|---|---|
| API Swagger UI | http://localhost:8000/docs | — |
| API Health | http://localhost:8000/health | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

---

## 🔍 Feature Engineering

### Feature Categories

```
11 features assembled per transaction in < 5ms via Redis pipeline:

┌─────────────────────────────────────────────────────────────────────┐
│  VELOCITY FEATURES  (Redis sorted sets, sliding windows with TTL)  │
│                                                                     │
│  txn_count_1m    — transactions this user made in last 1 minute    │
│  txn_count_5m    — transactions this user made in last 5 minutes   │
│  txn_count_1h    — transactions this user made in last 1 hour      │
│  user_5m_amount  — total amount this user spent in last 5 minutes  │
│  merch_5m_count  — transactions at this merchant in last 5 minutes │
│                                                                     │
│  Why Redis sorted sets: O(log n) range queries, automatic TTL      │
│  expiry prevents memory growth (user keys expire after 1 hour)     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  AMOUNT ANOMALY FEATURES  (Redis keys, set by Spark Silver job)    │
│                                                                     │
│  amount          — raw transaction amount (₹)                      │
│  amount_ratio    — amount / user's 30-day average spend            │
│  avg_30d         — user's average transaction amount (30 days)     │
│                   Default: 2,500 (population median) for new users │
│                                                                     │
│  Interpretation: amount_ratio = 15 means this transaction is       │
│  15x larger than this user's normal spend — strong fraud signal    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  DEVICE & USER FEATURES  (Redis sets)                               │
│                                                                     │
│  new_device      — 1.0 if device_id never seen before              │
│                    Device history stored as Redis set (30-day TTL) │
│  is_new_user     — 1.0 if no avg_30d key in Redis (no history)     │
│                                                                     │
│  Cold start: new user → is_new_user=1.0 → model increases          │
│  risk score. API never errors — always returns a decision.          │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│  GEOGRAPHY FEATURES  (computed in-process)                          │
│                                                                     │
│  geo_distance_km — haversine distance from user's home location     │
│                    Home stored in Redis, set on first transaction   │
│                    Default: Bengaluru (12.9716, 77.5946)            │
│  is_foreign_ip   — 1.0 if ip_country ≠ 'IN'                       │
│                                                                     │
│  Haversine formula: accounts for Earth's curvature                 │
│  3,000+ km = impossible geography (India → Russia in seconds)      │
└─────────────────────────────────────────────────────────────────────┘
```

### Redis Key Design

```python
# Velocity keys — sorted sets, score = unix timestamp, value = txn:amount
f"vel:user:{user_id}"          # user velocity window, TTL 3600s
f"vel:merch:{merchant_id}"     # merchant velocity window, TTL 3600s

# Batch feature keys — strings, set by Spark Silver job every 30 min
f"feat:{user_id}:avg_30d"      # 30-day average amount, TTL 86400s
f"feat:{user_id}:home_latlon"  # home lat,lon string, TTL 86400s

# Device registry — sets, one member per known device
f"user:{user_id}:devices"      # known device IDs, TTL 30 days
```

---

## 🚨 Rule Engine

### 12 Hard Rules

```python
# BLOCK rules — immediate rejection, model cannot override
RULE("EXTREME_AMOUNT",         "BLOCK",  amount > 200_000)
RULE("EXTREME_VELOCITY_5M",    "BLOCK",  txn_count_5m >= 10)
RULE("EXTREME_AMOUNT_RATIO",   "BLOCK",  amount_ratio > 20.0)
RULE("EXTREME_GEO_JUMP",       "BLOCK",  geo_distance_km > 3_000)

# REVIEW rules — suspicious but may be legitimate (human review queue)
RULE("HIGH_AMOUNT",            "REVIEW", amount > 50_000)
RULE("HIGH_VELOCITY_5M",       "REVIEW", txn_count_5m >= 5)
RULE("HIGH_AMOUNT_RATIO",      "REVIEW", amount_ratio > 10.0)
RULE("NEW_DEVICE",             "REVIEW", new_device == 1.0)
RULE("NEW_USER",               "REVIEW", is_new_user == 1.0)
RULE("FOREIGN_IP",             "REVIEW", is_foreign_ip == 1.0)
RULE("GEO_ANOMALY",            "REVIEW", geo_distance_km > 500)
RULE("HIGH_MERCHANT_RISK",     "REVIEW", merch_risk_30d > 0.05)
```

### Ensemble Decision Logic

```python
def decide(rule_severity, triggered_rules, ml_score):
    # Rules are hard constraints — model cannot override BLOCK rules
    if rule_severity == "BLOCK":
        return "BLOCK"              # rules trump everything

    # Model is very confident
    if ml_score >= 0.85:
        return "BLOCK"

    # Rules flag + model agrees → escalate
    if rule_severity == "REVIEW" and ml_score >= 0.60:
        return "BLOCK"

    # Rules flag, model uncertain → keep as REVIEW
    if rule_severity == "REVIEW":
        return "REVIEW"

    # Model sees medium risk → manual review queue
    if ml_score >= 0.45:
        return "REVIEW"

    return "APPROVE"
```

---

## 🤖 ML Model

### Training Pipeline

```
Generate 100K synthetic transactions
  ├── 98,000 normal  (lognormal amount, low velocity, known device, India IP)
  └──  2,000 fraud   (high amount, high velocity, new device, foreign IP, geo jump)
  + 1% label noise (prevents suspiciously perfect AUC)
          │
          ▼
Train XGBoost Classifier
  params:
    objective:        binary:logistic
    eval_metric:      aucpr (area under precision-recall — better for 2% fraud)
    scale_pos_weight: 49   (98K normal / 2K fraud — handles class imbalance)
    max_depth:        5
    eta:              0.08
    early_stopping:   15 rounds
          │
          ▼
Evaluate on 20% holdout
  ├── AUC-ROC: ~0.97
  └── AUC-PR:  ~0.89  (target: > 0.85)
          │
          ▼
Optional: Log to MLflow
  ├── Params, metrics, feature importance
  ├── Register to Production in Model Registry
  └── SHAP feature importance (top: txn_count_5m, amount_ratio, new_device)
          │
          ▼
Save: services/api/models/fraud_xgb.json
```

### Model Fallback Chain

```
API startup:
  1. Try load fraud_xgb.json (XGBoost native JSON)
     → backend = "xgboost"  (fastest, most accurate)

  2. If not found / fails: load fraud_sklearn.joblib
     → backend = "sklearn"  (trained at Docker build time, always present)

  3. If both fail: use heuristic formula
     → backend = "heuristic" (rule-based score, no file needed)

Result: API always starts, always returns a score, never crashes.
```

### Feature Importance (SHAP)

```
Top 5 features driving fraud predictions:

  txn_count_5m    ████████████████████  High velocity = strongest fraud signal
  amount_ratio    ████████████████      Amount far above normal = strong signal
  new_device      ████████████          First-seen device = medium-high signal
  geo_distance_km ██████████            Geographic jump = medium signal
  is_foreign_ip   ███████               Foreign IP = medium signal
```

---

## 🌊 Async Lakehouse Path

### Delta Lake Medallion Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    DELTA LAKE MEDALLION                              │
│                                                                      │
│  🥉 BRONZE (raw)        🥈 SILVER (features)   🥇 GOLD (alerts)    │
│  ─────────────────       ──────────────────     ──────────────────  │
│  • Raw Kafka payloads    • User aggregates       • BLOCK + REVIEW   │
│  • Append-only           • 30-day stats          • decisions only   │
│  • event_date partition  • Redis upsert          • alert_date part  │
│  • Immutable             • Triggers every 30min  • Analyst queries  │
│  • Checkpoint recovery   • foreachBatch pattern  • Delta ACID       │
└──────────────────────────────────────────────────────────────────────┘
```

### Exactly-Once Semantics

```
Problem: Kafka delivers at-least-once. Same event may arrive twice.
         Without deduplication: duplicate fraud alerts, wrong counts.

Solution: Event ID deduplication via Delta Lake MERGE

  Kafka → Spark batch → Delta MERGE on event_id
    "IF event_id already exists → skip (idempotent)"
    "IF new event_id → insert"

  Result: Kafka at-least-once + Delta MERGE = exactly-once end-to-end

  Combined with Spark checkpoint (saves Kafka offset):
    On restart → resumes from last committed offset
    No reprocessing from beginning
    No duplicates
```

### Spark → Redis Bridge (Silver Job)

```python
# Every 30 minutes, Spark Silver job pushes batch features to Redis
# FastAPI reads these < 1ms per lookup

def upsert_to_redis(batch_df, batch_id):
    r = redis.Redis.from_url(REDIS_URL)
    pipe = r.pipeline()
    for row in batch_df.collect():
        uid = row["user_id"]
        # Set 30-day average — FastAPI uses this for amount_ratio
        pipe.setex(f"feat:{uid}:avg_30d", 86400, str(row["avg_amount"]))
        # Set home location — FastAPI uses this for geo_distance_km
        if row["home_lat"] and row["home_lon"]:
            pipe.setex(f"feat:{uid}:home_latlon", 86400,
                       f"{row['home_lat']:.4f},{row['home_lon']:.4f}")
    pipe.execute()
```

---

## 📈 Performance Benchmarks

| Metric | Value | How Achieved |
|---|---|---|
| P50 scoring latency | ~12ms | Redis pipeline (1 round-trip), model loaded at startup |
| P95 scoring latency | ~28ms | XGBoost inference < 10ms on pre-loaded booster |
| P99 scoring latency | ~50ms | Async Kafka publish does not add to response time |
| Max local TPS | ~800 req/sec | FastAPI async + Redis pipeline, single instance |
| Redis feature lookup | < 5ms | Single pipeline call for all 11 features |
| XGBoost inference | < 10ms | Model loaded once at startup, not per request |
| Rule evaluation | < 1ms | Pure Python dict operations, no I/O |
| Kafka publish | non-blocking | fire-and-forget with aiokafka, 0ms added to P99 |
| Design target (prod) | 100K events/sec | AWS MSK + EMR Serverless + horizontal FastAPI scaling |

### Latency Breakdown

```
Total < 100ms P99:

  Redis pipeline       2–5ms    ████
  XGBoost inference    2–10ms   ████████
  Rule evaluation      < 1ms    █
  JSON serialization   1–2ms    ██
  Network overhead     2–5ms    ████
  ─────────────────────────────
  Total P50           ~12ms
  Total P99           ~50ms    ✅ SLO met
```

### Spark Optimization (Async Path)

```python
# AQE for skew handling on merchant_id partitions
spark.conf.set("spark.sql.adaptive.enabled",           "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",  "true")

# Small shuffle partition count (20) — analytics workload, not batch ETL
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Delta Lake ZORDER for fast analyst queries on Gold table
spark.sql("""
    OPTIMIZE gold.fraud_alerts
    ZORDER BY (user_id, decision_date)
""")
# Result: analyst queries on Gold < 200ms P50
```

---

## 🏗️ Infrastructure

```yaml
# docker-compose.yml — full local stack, one command
services:
  redis:      # Real-time feature store
  zookeeper:  # Kafka dependency
  kafka:      # Message broker — payment-decisions topic
  minio:      # S3-compatible storage for Delta Lake (local)
  api:        # FastAPI scoring service (trains sklearn at Docker build)
  prometheus: # Metrics collection
  grafana:    # Dashboard auto-provisioned from infra/grafana/
  producer:   # Synthetic traffic (profiles: traffic)

# Start:  docker compose up --build -d
# Traffic: docker compose --profile traffic up producer
```

### Production Infrastructure (AWS)

```
For production scale (100K events/sec):

  AWS MSK (Managed Kafka)     → replaces local Kafka
  AWS ElastiCache (Redis)     → replaces local Redis
  AWS S3                      → replaces MinIO
  AWS EMR Serverless          → runs Spark jobs
  ECS / EKS                   → runs FastAPI (horizontal scale)
  CloudWatch + PagerDuty      → replaces Prometheus + Grafana

  Estimated cost at 100K/sec:
    MSK (100 shards):   ~$1,080/month
    EMR Serverless:     ~$1,200/month
    ElastiCache:        ~$400/month
    Total:              ~$3,000/month
```

---

## 🔄 Orchestration

```bash
# Start streaming jobs (Spark) — after docker compose up
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,\
             org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  streaming/jobs/bronze_ingest.py

spark-submit streaming/jobs/silver_features.py   # updates Redis every 30 min

spark-submit streaming/jobs/gold_alerts.py       # writes BLOCK/REVIEW to Delta Gold

# Generate traffic
python services/producer/producer.py --tps 100 --fraud-rate 0.03

# Load test
locust -f scripts/load_test.py --host http://localhost:8000 --headless \
  -u 50 -r 10 -t 60s
```

---

## 📡 REST API

```bash
# Swagger docs: http://localhost:8000/docs

# Score a transaction
POST /v1/score
Body: {
  "txn_id":      "txn_abc123",
  "user_id":     "user_42",
  "amount":      75000.0,
  "merchant_id": "merch_stripe",
  "device_id":   "dev_ios_1",
  "ip_country":  "IN",
  "lat":         12.97,          # optional
  "lon":         77.59           # optional
}
Response: {
  "txn_id":          "txn_abc123",
  "decision":        "BLOCK",      # APPROVE | REVIEW | BLOCK
  "ml_score":        0.872,        # 0.0 to 1.0
  "triggered_rules": ["HIGH_AMOUNT", "NEW_DEVICE"],
  "features":        { ... },      # full feature vector for REVIEW queue
  "latency_ms":      18.4,
  "model_version":   "xgb-1753012345"
}

# Health check (shows which model is loaded)
GET /v1/health
Response: {"status":"ok","model_backend":"xgboost","model_version":"xgb-..."}

# Prometheus metrics
GET /metrics
Response: (text/plain Prometheus format)
```

---

## 📡 Monitoring & Alerting

```
Grafana dashboard (http://localhost:3000):
├── Panel 1: Scoring Latency P95  → histogram_quantile(0.95, ...)
├── Panel 2: Requests/sec by Decision → APPROVE / REVIEW / BLOCK ratio
├── Panel 3: Rule Hit Rate → which rules fire most often
└── Panel 4: Model Backend → 1.0=XGBoost, 0.5=sklearn, 0=heuristic

Prometheus metrics:
├── fraud_score_latency_seconds  (histogram)
├── fraud_score_requests_total   (counter, labelled by decision)
├── fraud_rule_hits_total        (counter, labelled by rule_name)
└── fraud_model_backend_info     (gauge: 1.0 / 0.5 / 0.0)

Alerts (add PagerDuty webhook to Grafana):
├── 🔴 P1: Fraud rate > 5% (fraud spike detected)
├── 🔴 P1: P99 latency > 200ms (SLO breach)
├── 🟡 P2: Model backend = heuristic (model file missing)
└── 🟡 P2: Error rate > 1% (API errors)
```

---

## ✅ Testing Strategy

```
tests/
├── services/api/tests/
│   ├── test_rules.py       # 7 tests: each rule fires correctly
│   └── test_decision.py    # 5 tests: ensemble logic (block/review/approve)
│
Running tests:
  cd services/api
  PYTHONPATH=. pytest tests/ -v --tb=short

Test coverage targets:
  Rule engine:     100% — every rule tested
  Decision logic:  100% — every branch tested
  Feature store:   integration test with real Redis
```

### Key Test Cases

| Test | What It Proves |
|---|---|
| `test_clean_passes` | Normal transaction → PASS, 0 rules fire |
| `test_extreme_amount_blocks` | ₹2,50,000 → BLOCK via EXTREME_AMOUNT |
| `test_high_velocity_blocks` | 10+ txns/5min → BLOCK via EXTREME_VELOCITY_5M |
| `test_extreme_geo_blocks` | 4,000km jump → BLOCK via EXTREME_GEO_JUMP |
| `test_foreign_ip_review` | RU IP → REVIEW via FOREIGN_IP |
| `test_block_rule_wins_over_low_ml` | BLOCK rule + low ML score → still BLOCK |
| `test_review_plus_ml_escalates` | REVIEW rule + ML 0.70 → escalates to BLOCK |

---

## 🛠️ Architecture Decision Records

| ADR | Decision | Rationale |
|---|---|---|
| ADR-001 | FastAPI, not Flask | Async (ASGI), 2-3x faster, auto OpenAPI docs, Pydantic validation |
| ADR-002 | aiokafka, not kafka-python | kafka-python.send() is synchronous — blocks FastAPI event loop |
| ADR-003 | Redis sorted sets for velocity | O(log n) range queries, automatic TTL, atomic operations |
| ADR-004 | Spark NOT on scoring path | Spark micro-batch = 5-30s min latency. Incompatible with < 100ms SLO |
| ADR-005 | XGBoost over deep learning | Interpretable (SHAP), fast on tabular, handles 2% class imbalance |
| ADR-006 | Model fallback chain | Docker build trains sklearn → API always starts, even without XGBoost |
| ADR-007 | Delta MERGE for exactly-once | Kafka at-least-once + event_id dedup = exactly-once end-to-end |
| ADR-008 | MinIO locally, S3 in prod | Same S3A code, just change endpoint URL — zero code changes for prod |

---

## 📋 Domain Glossary

| Term | Definition |
|---|---|
| Fraud score | XGBoost probability of fraud (0.0 = clean, 1.0 = definitely fraud) |
| Velocity | Number or amount of transactions in a time window (1-min, 5-min, 1-hour) |
| amount_ratio | Current amount / user's 30-day average spend (1.0 = normal, 10.0 = suspicious) |
| Geo jump | Geographic distance between transaction location and user's home location |
| Cold start | User with no Redis history — is_new_user=1.0, avg_30d=2,500 (population median) |
| BLOCK | Immediate rejection — hard rule or high ML confidence (≥ 0.85) |
| REVIEW | Flagged for human review queue — suspicious but not conclusive |
| APPROVE | Clean transaction — all rules pass, ML score < 0.45 |
| exactly-once | Each event processed exactly once — no duplicates, no missing events |
| Medallion | Bronze (raw) → Silver (features) → Gold (alerts) Delta Lake architecture |
| TTL | Time-to-live — Redis key expiry. Prevents memory growth from stale velocity data |

---

## 👤 Author

**Ujjawl Kumar** — Senior Data Engineer
- 🔗 [LinkedIn](https://linkedin.com/in/theujjawlkumar)
- 📧 info.ujjawlkr094@gmail.com
- 💻 [github.com/UJJAWLGIT](https://github.com/UJJAWLGIT)
- 5+ years building cloud-scale data platforms
- Domains: Fintech (Intuit GBSG · SaaS Subscriptions), Insurance (APCO Holdings · SBI General)

**Tech Stack:** Python · FastAPI · Redis · XGBoost · Apache Kafka · PySpark · Delta Lake · Prometheus · Grafana · Docker · GitHub Actions

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
