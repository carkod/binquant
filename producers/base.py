import os
import json
import logging

from kafka import KafkaProducer
from database import KafkaDB


class BaseProducer(KafkaDB):
    def __init__(self):
        super().__init__()

    def start_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            api_version=(3, 4, 1)
        )
        return self.producer

    def on_send_success(self, record_metadata):
        logging.info(
            f"Produced: {record_metadata.topic}, {record_metadata.offset}"
        )

    def on_send_error(self, excp):
        print(f"Message production failed to send: {excp}")
