import os
import logging
import sys

from confluent_kafka import Producer
from database import KafkaDB
from shared.utils import round_numbers_ceiling
from datetime import datetime


class BaseProducer(KafkaDB):
    def __init__(self):
        super().__init__()

    def start_producer(self):
        self.producer = Producer({
            "bootstrap.servers": f'{os.environ["CONFLUENT_GCP_BOOTSTRAP_SERVER"]}:{os.environ["KAFKA_PORT"]}',
            'sasl.mechanism': 'PLAIN',
            'security.protocol': 'SASL_SSL',
            'sasl.username': os.environ["CONFLUENT_API_KEY"],
            'sasl.password': os.environ["CONFLUENT_API_SECRET"],
        })
        return self.producer

    def on_send_success(self, record_metadata, error):
        print(record_metadata, error)

    def on_send_error(self, excp):
        print(f"Message production failed to send: {excp}")
