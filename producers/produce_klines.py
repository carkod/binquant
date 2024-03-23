import json
import os
from kafka import KafkaProducer
from shared.enums import KafkaTopics
class KlinesProducer:
    def __init__(self, symbol):
        self.symbol = symbol
        # self.interval = interval # should be the difference between start and end tiimestamps
        self.topic = KafkaTopics.klines_store_topic.value
        self.producer = KafkaProducer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

    def map_websocket_klines(self, candlestick, symbol):
        """
        Map Binance candlesticks to standard format (list of list)
        https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
        """
        return {
            "symbol": symbol,
            "open_time": candlestick["t"],
            "open": candlestick["o"],
            "high": candlestick["h"],
            "low": candlestick["l"],
            "close": candlestick["c"],
            "volume": candlestick["v"],
            "close_time": candlestick["T"],
        }

    def on_send_success(self, record_metadata):
        print(f"Message sent successfully: {record_metadata}")
    
    def on_send_error(self, excp):
        print(f"Message failed to send: {excp}")

    def store(self, data):
        candle = self.map_websocket_klines(data)

        # Produce message with asset name as key and candlestick data as value
        self.producer.send(self.topic, key=candle["open_time"],  value=json.dumps(candle)).add_callback(self.on_send_success).add_errback(self.on_send_error)

        # Flush self.producer buffer
        self.producer.flush()