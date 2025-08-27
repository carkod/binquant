import logging

from kafka import KafkaProducer

from database import KafkaDB
from models.klines import KlineProduceModel
from shared.enums import KafkaTopics


class KlinesProducer(KafkaDB):
    def __init__(self, producer: KafkaProducer, symbol):
        super().__init__()
        self.symbol = symbol
        self.producer = producer

    def on_send_success(self, record_metadata):
        pass

    def on_send_error(self, excp):
        logging.error(f"Message production failed to send: {excp}")

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
        # Produce message with asset name
        # this is faster then MongoDB change streams
        self.producer.send(
            topic=KafkaTopics.klines_store_topic.value,
            value=message.model_dump_json(),
            timestamp_ms=int(data["t"]),
            key=str(data["t"]).encode("utf-8"),
        ).add_callback(self.on_send_success).add_errback(self.on_send_error)
