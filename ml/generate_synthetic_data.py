"""
Generate 100K labelled payment transactions for model training.

Fraud patterns modelled:
  - High velocity (many transactions in 5 minutes)
  - Amount anomaly (large amount vs user's normal spend)
  - New device + high amount
  - Geographic jump (transaction far from user's home)
  - New user with high amount

Run: python ml/generate_synthetic_data.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_COLS = [
    "amount", "txn_count_1m", "txn_count_5m", "txn_count_1h",
    "amount_ratio", "avg_30d", "new_device", "is_new_user",
    "geo_distance_km", "is_foreign_ip", "merch_risk_30d",
]


def generate(n: int = 100_000, fraud_rate: float = 0.02, seed: int = 42) -> pd.DataFrame:
    rng     = np.random.default_rng(seed)
    n_fraud = int(n * fraud_rate)
    n_legit = n - n_fraud

    legit = pd.DataFrame({
        "amount":          rng.lognormal(6.5, 0.8,   n_legit),
        "txn_count_1m":    rng.integers(0, 3,         n_legit),
        "txn_count_5m":    rng.integers(0, 5,         n_legit),
        "txn_count_1h":    rng.integers(0, 12,        n_legit),
        "amount_ratio":    rng.uniform(0.3, 2.5,      n_legit),
        "avg_30d":         rng.uniform(500, 8_000,    n_legit),
        "new_device":      rng.choice([0, 1], n_legit, p=[0.90, 0.10]),
        "is_new_user":     rng.choice([0, 1], n_legit, p=[0.95, 0.05]),
        "geo_distance_km": rng.exponential(20,        n_legit),
        "is_foreign_ip":   rng.choice([0, 1], n_legit, p=[0.97, 0.03]),
        "merch_risk_30d":  rng.beta(1.5, 30,          n_legit),
        "is_fraud":        0,
    })

    fraud = pd.DataFrame({
        "amount":          rng.lognormal(8.5, 1.1,   n_fraud),
        "txn_count_1m":    rng.integers(3, 12,        n_fraud),
        "txn_count_5m":    rng.integers(6, 20,        n_fraud),
        "txn_count_1h":    rng.integers(10, 40,       n_fraud),
        "amount_ratio":    rng.uniform(5.0, 30.0,     n_fraud),
        "avg_30d":         rng.uniform(200, 4_000,    n_fraud),
        "new_device":      rng.choice([0, 1], n_fraud, p=[0.25, 0.75]),
        "is_new_user":     rng.choice([0, 1], n_fraud, p=[0.55, 0.45]),
        "geo_distance_km": rng.exponential(600,       n_fraud),
        "is_foreign_ip":   rng.choice([0, 1], n_fraud, p=[0.25, 0.75]),
        "merch_risk_30d":  rng.beta(5, 10,            n_fraud),
        "is_fraud":        1,
    })

    df = pd.concat([legit, fraud], ignore_index=True)
    # 1% label noise — prevents suspiciously perfect AUC in demos
    flip = rng.choice(df.index, size=max(1, int(0.01 * len(df))), replace=False)
    df.loc[flip, "is_fraud"] = 1 - df.loc[flip, "is_fraud"]
    return df.sample(frac=1.0, random_state=seed).reset_index(drop=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n",   type=int, default=100_000)
    p.add_argument("--out", default="ml/artifacts/transactions.csv")
    args = p.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = generate(n=args.n)
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows  fraud_rate={df.is_fraud.mean():.3f}")


if __name__ == "__main__":
    main()
