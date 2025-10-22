import json
import logging
import os

from aiokafka import AIOKafkaProducer

from database import KafkaDB

logger = logging.getLogger(__name__)


class AsyncProducer(KafkaDB):
    """Async Kafka producer wrapper using AIOKafkaProducer.

    Provides explicit async lifecycle methods and a simple send API that
    serializes values to JSON.
    """

    def __init__(self) -> None:
        super().__init__()
        self._producer: AIOKafkaProducer | None = None
        self._started = False

    async def start(self) -> AIOKafkaProducer:
        if self._started:
            assert self._producer is not None
            return self._producer
        bootstrap = (
            f"{os.getenv('KAFKA_HOST', 'localhost')}:{os.getenv('KAFKA_PORT', '29092')}"
        )
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            linger_ms=5,
            acks=1,
            enable_idempotence=False,
            request_timeout_ms=15000,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            max_batch_size=8192,
            compression_type=None,
        )
        await self._producer.start()
        self._started = True
        logger.debug("AIOKafkaProducer started (bootstrap=%s)", bootstrap)
        return self._producer

    async def send(
        self, topic: str, value: str, key: str, timestamp: int | None = None
    ) -> None:
        if not self._started or not self._producer:
            raise RuntimeError("Producer not started. Call await start() first.")
        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=str(key).encode("utf-8"),
            timestamp_ms=timestamp,
        )

    async def stop(self) -> None:
        if self._producer and self._started:
            await self._producer.stop()
            logger.info("AIOKafkaProducer stopped")
        self._producer = None
        self._started = False
