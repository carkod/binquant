from datetime import datetime
import json
import logging

from kafka import KafkaProducer
from models.klines import KlineProduceModel
from database import KafkaDB
from shared.utils import round_numbers_ceiling
from shared.enums import KafkaTopics

class KlinesProducer(KafkaDB):
    def __init__(self, producer: KafkaProducer, symbol, partition=0):
        super().__init__()
        self.symbol = symbol
        self.topic = KafkaTopics.klines_store_topic.value
        self.current_partition = partition
        self.producer = producer
        self.set_partitions()
        self.partition_count = 0

    def set_partitions(self):
        partitions = self.get_partitions()
        if len(partitions) > 0:
            self.topic_partition = {x["topic"]: x["partition"] for x in self.topic_partition}
        else:
            self.topic_partition = {}
        pass

    def on_send_success(self, record_metadata):
        timestamp = int(round_numbers_ceiling(record_metadata.timestamp / 1000, 0))
        print(
            f"{datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')} Produced: {record_metadata.offset}"
        )

    def on_send_error(self, excp):
        print(f"Message production failed to send: {excp}")

    def store(self, data):

        # Allocate partition for each symbol and dedup
        try:
            self.topic_partition[data["s"]]
        except KeyError:
            self.topic_partition[data["s"]] = self.partition_count
            self.partition_count += 1
            pass

        try:

            if data["x"]:
                self.store_klines(data, self.topic_partition[data["s"]])
                message = KlineProduceModel(
                    symbol=data["s"],
                    open_time=str(data["t"]),
                    close_time=str(data["T"]),
                )
                # Produce message with asset name
                # this is faster then MongoDB change streams
                self.producer.send(
                    topic=self.topic,
                    partition=self.topic_partition[data["s"]],
                    value=message.model_dump_json(),
                    timestamp_ms=int(data["t"]),
                    key=str(data["t"]).encode("utf-8"),
                ).add_callback(self.on_send_success).add_errback(self.on_send_error)

        except Exception as e:
            logging.error(f"Error: {e}")
            pass

