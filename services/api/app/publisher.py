"""
Async Kafka publisher — fire-and-forget.

Uses aiokafka (not kafka-python) because kafka-python's send() is synchronous
and will block the FastAPI event loop if Kafka is slow.

If Kafka is unavailable (e.g. during local dev without docker-compose),
the publisher silently no-ops rather than crashing the API.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer

log = logging.getLogger(__name__)


class DecisionPublisher:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._servers = bootstrap_servers
        self._topic   = topic
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._servers,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            await self._producer.start()
            log.info("Kafka producer connected: %s → %s", self._servers, self._topic)
        except Exception as exc:
            # This is expected during local dev without Kafka running
            log.warning("Kafka unavailable — decisions will not be published: %s", exc)
            self._producer = None

    async def stop(self) -> None:
        if self._producer:
            try:
                await self._producer.stop()
            except Exception:
                pass

    async def publish(self, payload: Dict[str, Any]) -> None:
        """Non-blocking publish. Silently drops if Kafka is down."""
        if self._producer is None:
            return
        try:
            await self._producer.send_and_wait(self._topic, payload)
        except Exception as exc:
            log.debug("Kafka publish failed (non-fatal): %s", exc)
