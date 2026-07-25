FROM python:3.11-slim

WORKDIR /app

COPY services/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/api/app    ./app
COPY services/api/models ./models
COPY ml ./ml

# Train a portable sklearn model at build time.
# This runs without XGBoost's native libs (no Mac libomp issues).
# If fraud_xgb.json is also present the API will prefer it at runtime.
RUN python ml/train_sklearn.py \
      --n 30000 \
      --model-out models/fraud_sklearn.joblib \
      --metrics-out /tmp/metrics_sklearn.json

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV MODEL_PATH=/app/models/fraud_xgb.json
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
