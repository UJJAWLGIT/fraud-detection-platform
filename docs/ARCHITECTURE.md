# Architecture Notes

## Sync path (critical — < 100ms)

```
Client  →  POST /v1/score
              ↓
         FeatureStore.get_and_update(tx)
           • Redis pipeline: velocity windows + batch features
           • Updates velocity counters after reading
           ↓
         FraudModel.predict_proba(features)
           • XGBoost JSON (preferred)  or
           • sklearn GradientBoosting  or
           • Heuristic fallback
           ↓
         rules.evaluate(features)
           • 4 BLOCK rules (hard limits)
           • 8 REVIEW rules (suspicious signals)
           ↓
         decision.decide(features, ml_score)
           • Rules win if BLOCK
           • ML + rules combined for REVIEW/escalation
           ↓
         Return ScoreResponse  +  publish to Kafka (async, non-blocking)
```

## Async path (non-critical — seconds)

```
Kafka topic: payment-decisions
    ↓
bronze_ingest.py    → Delta Bronze  (raw payload, append-only, partitioned by date)
silver_features.py  → Delta Silver  (user aggregates) + Redis upsert every 30 min
gold_alerts.py      → Delta Gold    (BLOCK + REVIEW only, for analyst investigation)
```

## Latency budget (local, MacBook M2)

| Step                   | Budget  |
|------------------------|---------|
| Redis pipeline         | 1–4 ms  |
| XGBoost predict        | 1–8 ms  |
| Rule evaluation        | < 1 ms  |
| JSON serialize         | 1–2 ms  |
| Total                  | < 50 ms |

## Feature list

| Feature          | Source      | What it captures               |
|------------------|-------------|--------------------------------|
| amount           | request     | Raw transaction amount         |
| txn_count_1m     | Redis       | Velocity: 1-minute window      |
| txn_count_5m     | Redis       | Velocity: 5-minute window      |
| txn_count_1h     | Redis       | Velocity: 1-hour window        |
| amount_ratio     | Redis+req   | Amount / user's 30d average    |
| avg_30d          | Redis/Spark | User's average spend (30 days) |
| new_device       | Redis       | First-seen device this month   |
| is_new_user      | Redis       | No history in last 30 days     |
| geo_distance_km  | Redis+req   | Distance from home location    |
| is_foreign_ip    | request     | IP country ≠ IN                |
| merch_risk_30d   | Redis/Spark | Merchant's historical fraud %  |
