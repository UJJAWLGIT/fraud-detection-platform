"""
Fallback sklearn model — GradientBoosting, no native lib dependencies.

Used when XGBoost is unavailable (e.g. Mac CI runners, some Docker bases).
Trained at Docker image build time so the API always has *something* to load.

Run: python ml/train_sklearn.py --n 30000
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from generate_synthetic_data import FEATURE_COLS, generate


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n",          type=int, default=50_000)
    p.add_argument("--model-out",  default="services/api/models/fraud_sklearn.joblib")
    p.add_argument("--metrics-out",default="ml/artifacts/metrics_sklearn.json")
    args = p.parse_args()

    df = generate(n=args.n)
    X, y = df[FEATURE_COLS], df["is_fraud"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    model = GradientBoostingClassifier(n_estimators=80, max_depth=4, random_state=42)
    model.fit(X_tr, y_tr)
    auc = roc_auc_score(y_te, model.predict_proba(X_te)[:, 1])

    out = Path(args.model_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "features": FEATURE_COLS}, str(out))

    Path(args.metrics_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.metrics_out).write_text(json.dumps({"auc": auc, "model_type": "sklearn_gb"}, indent=2))
    print(f"sklearn AUC={auc:.3f} → {out}")


if __name__ == "__main__":
    main()
