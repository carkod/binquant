import logging
import os

from confluent_kafka import Producer

from database import KafkaDB


class BaseProducer(KafkaDB):
    def __init__(self):
        super().__init__()

    def start_producer(self):
        conf = {
            "bootstrap.servers": f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}"
        }
        self.producer = Producer(conf)
        return self.producer

    def on_send_success(self, err, msg):
        if err is not None:
            logging.error(f"Message failed delivery: {err}")
        else:
            logging.info(f"Produced: {msg.topic()}, {msg.offset()}")


# Note: confluent-kafka is not asyncio-native. AsyncProducer is now just a wrapper for compatibility.
class AsyncProducer(KafkaDB):
    def __init__(self):
        super().__init__()
        self.producer = None

    def get_producer(self):
        conf = {
            "bootstrap.servers": f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_PORT']}"
        }
        self.producer = Producer(conf)
        return self.producer
