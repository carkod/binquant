import logging

from datetime import datetime
from models.klines import KlineProduceModel
from database import KafkaDB
from shared.utils import round_numbers_ceiling
from shared.enums import KafkaTopics
from confluent_kafka import Producer

class KlinesProducer(KafkaDB):
    def __init__(self, producer: Producer, symbol):
        super().__init__()
        self.symbol = symbol
        self.producer = producer

    def on_send_success(self, error, msg):
        if error:
            print(error)
        else:
            print(f'User record {msg.key()} successfully produced to {msg.topic()}')

    def on_send_error(self, excp):
        print(f"Message production failed to send: {excp}")

    def store(self, data):

        self.store_klines(data)
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
        self.producer.produce(
            topic=KafkaTopics.klines_store_topic.value,
            value=message.model_dump_json(),
            key=str(data["t"]).encode("utf-8"),
            callback=self.on_send_success,
        )
        self.producer.poll(1)
