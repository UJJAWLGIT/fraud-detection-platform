#!/usr/bin/env bash
# One-shot setup for local development

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "=== Training models ==="
cd ml
python3 -m venv /tmp/fraud_venv 2>/dev/null || true
/tmp/fraud_venv/bin/pip install -q numpy pandas scikit-learn xgboost joblib
/tmp/fraud_venv/bin/python train_xgboost.py --n 100000 \
  --model-out ../services/api/models/fraud_xgb.json
cd "$ROOT"

echo "=== Starting docker compose ==="
docker compose up --build -d

echo "=== Waiting for API ==="
for i in $(seq 1 60); do
  if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
    echo ""
    echo "Ready!"
    echo "  API docs:    http://localhost:8000/docs"
    echo "  Grafana:     http://localhost:3000  (admin/admin)"
    echo "  Prometheus:  http://localhost:9090"
    echo ""
    echo "  Test it:"
    echo "  curl -s http://localhost:8000/v1/score \\"
    echo "    -H 'Content-Type: application/json' \\"
    echo "    -d '{\"txn_id\":\"t1\",\"user_id\":\"u1\",\"amount\":250000,\"merchant_id\":\"m1\",\"device_id\":\"dev_new\",\"ip_country\":\"RU\"}' | jq ."
    exit 0
  fi
  printf "."
  sleep 2
done
echo "API did not start" && exit 1
