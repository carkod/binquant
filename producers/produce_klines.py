import logging
import time

from confluent_kafka import Producer

from database import KafkaDB
from models.klines import KlineProduceModel
from shared.enums import KafkaTopics


class KlinesProducer(KafkaDB):
    def __init__(self, producer: Producer, symbol, flush_interval=5):
        """
        flush_interval: seconds between periodic flushes
        """
        super().__init__()
        self.symbol = symbol
        self.producer = producer
        self.last_flush_time = time.time()
        self.flush_interval = flush_interval

    def on_send_success(self, err, msg):
        if err is not None:
            logging.error(f"Message production failed: {err}")
        else:
            logging.debug(
                f"Message produced to {msg.topic()} partition {msg.partition()} offset {msg.offset()}"
            )

    def store(self, result):
        data = result["k"]
        message = KlineProduceModel(
            symbol=data["s"],
            open_time=str(data["t"]),
            close_time=str(data["T"]),
            open_price=str(data["o"]),
            high_price=str(data["h"]),
            low_price=str(data["l"]),
            close_price=str(data["c"]),
            volume=str(data["v"]),
        )
        try:
            self.producer.produce(
                topic=KafkaTopics.klines_store_topic.value,
                value=message.model_dump_json().encode("utf-8"),
                key=str(data["s"]).encode("utf-8"),
                timestamp=int(data["t"]),
                callback=self.on_send_success,
            )
            self.producer.poll(0)

            # Periodically flush to avoid message buildup
            current_time = time.time()
            if current_time - self.last_flush_time >= self.flush_interval:
                self.producer.flush(timeout=5)
                self.last_flush_time = current_time

        except BufferError:
            logging.warning("Producer local buffer full. Flushing...")
            self.producer.flush(timeout=5)
        except Exception as excp:
            logging.error(f"Unexpected error during produce: {excp}")

    def shutdown(self):
        """
        Call at application shutdown to flush remaining messages
        """
        logging.info("Flushing remaining messages before shutdown...")
        self.producer.flush(timeout=10)
