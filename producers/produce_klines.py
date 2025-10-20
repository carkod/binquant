import logging
import time

from aiokafka import AIOKafkaProducer

from database import KafkaDB
from models.klines import KlineProduceModel
from shared.enums import KafkaTopics


class KlinesProducer(KafkaDB):
    def __init__(
        self, producer: AIOKafkaProducer, symbol: str, flush_interval: int = 5
    ):
        """Async Kafka producer wrapper.

        flush_interval: seconds between manual flush markers (for logging only;
        aiokafka batches automatically, so we use it to report periodic status).
        """
        super().__init__()
        self.symbol = symbol
        self.producer: AIOKafkaProducer = producer
        self.last_flush_time = time.time()
        self.flush_interval = flush_interval

    async def store(self, result) -> None:
        data = result["k"]
        model = KlineProduceModel(
            symbol=data["s"],
            open_time=str(data["t"]),
            close_time=str(data["T"]),
            open_price=str(data["o"]),
            high_price=str(data["h"]),
            low_price=str(data["l"]),
            close_price=str(data["c"]),
            volume=str(data["v"]),
        )
        payload = model.model_dump_json().encode("utf-8")
        key = str(data["s"]).encode("utf-8")
        ts = int(data["t"])
        try:
            await self.producer.send_and_wait(
                KafkaTopics.klines_store_topic.value,
                value=payload,
                key=key,
                timestamp_ms=ts,
            )
            logging.debug(
                f"Produced kline for {data['s']} at open_time={data['t']} topic={KafkaTopics.klines_store_topic.value}"
            )
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                # aiokafka handles flushing automatically; we just log
                pending = (
                    len(self.producer._message_accumulator)
                    if hasattr(self.producer, "_message_accumulator")
                    else "?"
                )
                logging.debug(f"Periodic producer status: pending_messages={pending}")
                self.last_flush_time = current_time
        except Exception as excp:
            logging.error(f"Unexpected error during async produce: {excp}")
