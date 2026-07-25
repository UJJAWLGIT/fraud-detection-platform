"""
Train XGBoost fraud classifier and export to JSON booster format.

Logs metrics to MLflow if tracking server is available.
Falls back to local metrics.json if MLflow is not running.

Run: python ml/train_xgboost.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import classification_report, roc_auc_score, average_precision_score
from sklearn.model_selection import train_test_split

from generate_synthetic_data import FEATURE_COLS, generate

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def _try_mlflow_log(params: dict, metrics: dict, model, run_name: str) -> None:
    try:
        import mlflow
        import mlflow.xgboost
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment("fraud-xgboost")
        with mlflow.start_run(run_name=run_name):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.xgboost.log_model(model, "model",
                                      registered_model_name="fraud_detection")
        log.info("Logged to MLflow: %s", MLFLOW_URI)
    except Exception as exc:
        log.info("MLflow not available (%s) — skipping experiment tracking", exc)


def train(df: pd.DataFrame, model_out: Path, metrics_out: Path) -> None:
    X, y = df[FEATURE_COLS], df["is_fraud"]
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    spw = float((y_tr == 0).sum() / max((y_tr == 1).sum(), 1))
    params = dict(
        objective="binary:logistic",
        eval_metric="aucpr",
        max_depth=5,
        eta=0.08,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        seed=42,
    )
    log.info("scale_pos_weight=%.1f  (fraud:legit ratio in training)", spw)

    dtrain = xgb.DMatrix(X_tr, label=y_tr, feature_names=FEATURE_COLS)
    dtest  = xgb.DMatrix(X_te, label=y_te, feature_names=FEATURE_COLS)

    booster = xgb.train(
        params, dtrain,
        num_boost_round=120,
        evals=[(dtrain, "train"), (dtest, "test")],
        verbose_eval=20,
        early_stopping_rounds=15,
    )

    proba = booster.predict(dtest)
    pred  = (proba >= 0.5).astype(int)

    metrics = {
        "auc":    float(roc_auc_score(y_te, proba)),
        "aucpr":  float(average_precision_score(y_te, proba)),
        "n_train": len(X_tr),
        "n_test":  len(X_te),
    }
    log.info("AUC=%.3f  AUCPR=%.3f", metrics["auc"], metrics["aucpr"])
    log.info("\n%s", classification_report(y_te, pred))

    model_out.parent.mkdir(parents=True, exist_ok=True)
    booster.save_model(str(model_out))
    metrics_out.parent.mkdir(parents=True, exist_ok=True)
    metrics_out.write_text(json.dumps({**metrics, "params": params}, indent=2))

    _try_mlflow_log(params, metrics, booster, run_name=f"xgb_n{len(df)}")
    log.info("Saved model → %s", model_out)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n",          type=int, default=100_000)
    p.add_argument("--model-out",  default="services/api/models/fraud_xgb.json")
    p.add_argument("--metrics-out",default="ml/artifacts/metrics_xgb.json")
    args = p.parse_args()

    log.info("Generating %d synthetic transactions...", args.n)
    df = generate(n=args.n)
    train(df, Path(args.model_out), Path(args.metrics_out))


if __name__ == "__main__":
    main()
