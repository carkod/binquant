import json
import logging
import os

from aiokafka import AIOKafkaProducer
from kafka import KafkaProducer

from database import KafkaDB


class BaseProducer(KafkaDB):
    def __init__(self):
        super().__init__()

    def start_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            api_version=(2, 5),
        )
        return self.producer

    def on_send_success(self, record_metadata):
        logging.info(f"Produced: {record_metadata.topic}, {record_metadata.offset}")

    def on_send_error(self, excp):
        logging.error(f"Message production failed to send: {excp}")


class AsyncProducer(KafkaDB):
    def __init__(self):
        super().__init__()
        self.producer = None

    def get_producer(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
        )
        return self.producer
