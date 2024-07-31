from datetime import datetime
from kafka import KafkaProducer
from models.klines import KlineProduceModel
from database import KafkaDB
from shared.utils import round_numbers_ceiling
from shared.enums import KafkaTopics


class KlinesProducer(KafkaDB):
    def __init__(self, producer: KafkaProducer, symbol):
        super().__init__()
        self.symbol = symbol
        self.producer = producer

    def on_send_success(self, record_metadata):
        timestamp = int(round_numbers_ceiling(record_metadata.timestamp / 1000, 0))
        # print(
        #     f"{datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')} Produced: {record_metadata.partition}"
        # )

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
        self.producer.send(
            topic=KafkaTopics.klines_store_topic.value,
            value=message.model_dump_json(),
            timestamp_ms=int(data["t"]),
            key=str(data["t"]).encode("utf-8"),
        ).add_callback(self.on_send_success).add_errback(self.on_send_error)
