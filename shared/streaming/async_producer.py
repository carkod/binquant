from __future__ import annotations
import json
import logging
import os
from typing import Any, Optional
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class AsyncProducer:
    """
    Async Kafka Producer with low-latency tuning.
    """
    @staticmethod
    async def initialize() -> AIOKafkaProducer:
        producer = AIOKafkaProducer(
            bootstrap_servers=f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', 29092)}",
            linger_ms=5,
            acks=1,
            enable_idempotence=False,
            request_timeout_ms=15000,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            max_batch_size=8192,
            compression_type=None,
        )
        await producer.start()
        return producer

    async def send(
        producer: AIOKafkaProducer,
        topic: str,
        value: Any,
        key: Any | None = None,
        partition: Optional[int] = None,
    ) -> None:
        await producer.send_and_wait(
            topic=topic,
            value=value,
            key=None if key is None else json.dumps(key).encode("utf-8"),
            partition=partition,
        )
        return producer
