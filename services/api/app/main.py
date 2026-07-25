"""
Real-time fraud scoring API.

Sync path (this file):
  POST /v1/score → Redis features → ML model → rules → APPROVE/REVIEW/BLOCK

Async path (separate Spark jobs):
  Decision published to Kafka → Bronze/Silver/Gold Delta tables
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from .decision   import decide
from .features   import FeatureStore
from .metrics    import LATENCY, MODEL_BACKEND, REQUESTS, RULE_HITS
from .model      import FraudModel
from .publisher  import DecisionPublisher
from .schemas    import PaymentRequest, ScoreResponse

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

# ── Config from environment ──────────────────────────────────────────────────
REDIS_URL  = os.getenv("REDIS_URL",              "redis://localhost:6379/0")
KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC",            "payment-decisions")
MODEL_PATH  = os.getenv("MODEL_PATH",             "models/fraud_xgb.json")
MODEL_VER   = os.getenv("MODEL_VERSION",          "v1")

# ── Singletons — instantiated at startup ─────────────────────────────────────
_store:     FeatureStore     | None = None
_model:     FraudModel       | None = None
_publisher: DecisionPublisher | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _store, _model, _publisher

    _store = FeatureStore(REDIS_URL)
    _model = FraudModel(MODEL_PATH)
    _publisher = DecisionPublisher(KAFKA_BOOT, KAFKA_TOPIC)
    await _publisher.start()

    backend_score = {"xgboost": 1.0, "sklearn": 0.5, "heuristic": 0.0}
    MODEL_BACKEND.set(backend_score.get(_model.backend, 0.0))
    log.info("Startup complete — model backend: %s", _model.backend)

    yield

    await _store.close()
    await _publisher.stop()
    log.info("Shutdown complete")


app = FastAPI(
    title="Fraud Detection API",
    description="Real-time payment fraud scoring — < 100ms P99",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


@app.post("/v1/score", response_model=ScoreResponse)
async def score(tx: PaymentRequest) -> ScoreResponse:
    """
    Score a payment transaction.

    Returns APPROVE / REVIEW / BLOCK with the ML score, triggered rules,
    and the feature vector used (useful for REVIEW queue investigation).
    """
    t0 = time.perf_counter()

    try:
        features = await _store.get_and_update(tx)
    except Exception as exc:
        log.error("Feature store error for %s: %s", tx.txn_id, exc)
        raise HTTPException(status_code=503, detail="Feature store unavailable") from exc

    ml_score = _model.predict_proba(features)
    decision, triggered = decide(features, ml_score)

    latency_ms = (time.perf_counter() - t0) * 1000

    REQUESTS.labels(decision=decision).inc()
    LATENCY.observe(latency_ms / 1000)
    for rule in triggered:
        RULE_HITS.labels(rule=rule).inc()

    resp = ScoreResponse(
        txn_id=tx.txn_id,
        decision=decision,
        ml_score=round(ml_score, 4),
        triggered_rules=triggered,
        features={k: round(v, 4) for k, v in features.items()},
        latency_ms=round(latency_ms, 1),
        model_version=_model.version,
    )

    # Publish async — does not add to response latency
    await _publisher.publish({
        **tx.model_dump(mode="json"),
        "decision":       decision,
        "ml_score":       ml_score,
        "triggered_rules": triggered,
        "latency_ms":     latency_ms,
        "model_version":  _model.version,
    })

    return resp


@app.get("/health")
async def health():
    return {
        "status":        "ok",
        "model_backend": _model.backend if _model else "not_loaded",
        "model_version": _model.version if _model else "none",
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
