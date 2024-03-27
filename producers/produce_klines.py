import json
import os
from datetime import datetime
from kafka import KafkaProducer
from shared.utils import round_numbers_ceiling
from shared.enums import KafkaTopics
from models.klines import KlineModel, KlineProducerPayloadModel


class KlinesProducer:
    def __init__(self, symbol, partition):
        self.symbol = symbol
        # self.interval = interval # should be the difference between start and end tiimestamps
        self.topic = KafkaTopics.klines_store_topic.value
        self.producer = KafkaProducer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.current_partition = partition

    def map_websocket_klines(self, candlestick) -> KlineProducerPayloadModel:
        """
        Map Binance candlesticks to standard format (list of list)
        https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
        """
        candle = KlineModel(
            symbol=candlestick["s"],
            open_time=candlestick["t"],
            open=candlestick["o"],
            high=candlestick["h"],
            low=candlestick["l"],
            close=candlestick["c"],
            volume=candlestick["v"],
            close_time=candlestick["T"],
            candle_closed=candlestick["x"],
            interval=candlestick["i"],
        )
        producer_payload = KlineProducerPayloadModel(partition=self.current_partition, kline=candle)
        return producer_payload

    def on_send_success(self, record_metadata):
        timestamp = int(round_numbers_ceiling(record_metadata.timestamp / 1000, 0))
        print(
            f"{datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')} Messaged: {record_metadata.topic}, {record_metadata.offset}"
        )

    def on_send_error(self, excp):
        print(f"Message production failed to send: {excp}")

    def store(self, data):
        payload = self.map_websocket_klines(data)

        try:
            print("Partition: ", self.current_partition, "Symbol: ", payload.kline.symbol)
            # Produce message with asset name as key and candlestick data as value
            self.producer.send(
                topic=self.topic,
                partition=self.current_partition,
                value=json.dumps(payload.dict()),
                timestamp_ms=int(payload.kline.open_time),
            ).add_callback(self.on_send_success).add_errback(self.on_send_error)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # Flush self.producer buffer
            self.producer.close()
