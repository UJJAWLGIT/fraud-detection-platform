"""Prometheus metrics. Kept intentionally minimal — only what Grafana actually uses."""
from prometheus_client import Counter, Histogram, Gauge

REQUESTS = Counter(
    "fraud_score_requests_total",
    "Total scoring requests",
    ["decision"],
)

LATENCY = Histogram(
    "fraud_score_latency_seconds",
    "End-to-end scoring latency",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.2, 0.5),
)

RULE_HITS = Counter(
    "fraud_rule_hits_total",
    "Rule trigger counts by rule name",
    ["rule"],
)

MODEL_BACKEND = Gauge(
    "fraud_model_backend_info",
    "Active model backend (1=xgboost, 0.5=sklearn, 0=heuristic)",
)
