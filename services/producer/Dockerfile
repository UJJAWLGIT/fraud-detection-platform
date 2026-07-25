FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx==0.28.1
COPY producer.py .
CMD ["python", "producer.py"]
