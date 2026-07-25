"""
Fraud model loader.

Priority order at startup:
  1. XGBoost JSON (if fraud_xgb.json exists)
  2. sklearn joblib (if fraud_sklearn.joblib exists — trained at Docker build time)
  3. Heuristic fallback (always works, lower accuracy)

This means the API works even on machines where XGBoost's native libs aren't
available (Mac without libomp, certain cloud instances, etc.).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

log = logging.getLogger(__name__)

FEATURE_ORDER: List[str] = [
    "amount",
    "txn_count_1m",
    "txn_count_5m",
    "txn_count_1h",
    "amount_ratio",
    "avg_30d",
    "new_device",
    "is_new_user",
    "geo_distance_km",
    "is_foreign_ip",
    "merch_risk_30d",
]


class FraudModel:
    def __init__(self, model_path: str) -> None:
        self._booster    = None
        self._sklearn    = None
        self._backend    = "heuristic"
        self._version    = "heuristic-v0"
        self._model_path = model_path

        xgb_path = Path(model_path)
        sk_path  = xgb_path.with_name("fraud_sklearn.joblib")

        if xgb_path.exists():
            try:
                import xgboost as xgb
                self._booster = xgb.Booster()
                self._booster.load_model(str(xgb_path))
                self._backend = "xgboost"
                self._version = f"xgb-{xgb_path.stat().st_mtime:.0f}"
                log.info("Loaded XGBoost model from %s", xgb_path)
                return
            except Exception as exc:
                log.warning("XGBoost load failed (%s), trying sklearn fallback", exc)

        if sk_path.exists():
            try:
                import joblib
                payload = joblib.load(str(sk_path))
                self._sklearn = payload["model"]
                self._backend = "sklearn"
                self._version = f"sklearn-{sk_path.stat().st_mtime:.0f}"
                log.info("Loaded sklearn model from %s", sk_path)
                return
            except Exception as exc:
                log.warning("sklearn load failed (%s), using heuristic", exc)

        log.warning("No model file found at %s — using heuristic scorer", model_path)

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def version(self) -> str:
        return self._version

    def predict_proba(self, features: Dict[str, float]) -> float:
        row = np.array(
            [[features.get(f, 0.0) for f in FEATURE_ORDER]], dtype=np.float32
        )

        if self._booster is not None:
            import xgboost as xgb
            dmat  = xgb.DMatrix(row, feature_names=FEATURE_ORDER)
            score = float(self._booster.predict(dmat)[0])
            return max(0.0, min(1.0, score))

        if self._sklearn is not None:
            score = float(self._sklearn.predict_proba(row)[0][1])
            return max(0.0, min(1.0, score))

        return self._heuristic(features)

    @staticmethod
    def _heuristic(f: Dict[str, float]) -> float:
        """Rough score when no trained model is available. Good enough for demos."""
        s  = 0.05
        s += min(f.get("txn_count_5m", 0) / 10.0,     0.30)
        s += min((f.get("amount_ratio", 1) - 1) * 0.03, 0.25)
        s += f.get("is_foreign_ip",  0) * 0.15
        s += f.get("new_device",     0) * 0.12
        s += f.get("is_new_user",    0) * 0.08
        s += min(f.get("geo_distance_km", 0) / 3000.0, 0.15)
        return max(0.0, min(0.99, s))
