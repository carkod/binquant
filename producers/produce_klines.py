import json
import os
from kafka import KafkaProducer

class KlinesProducer:
    def __init__(self, symbol):
        self.symbol = symbol
        # self.interval = interval # should be the difference between start and end tiimestamps
        self.topic = 'candlestick_data_topic'
        self.producer = KafkaProducer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

    def map_websocket_klines(self, candlestick):
        """
        Map Binance candlesticks to standard format (list of list)
        https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Kline-Candlestick-Streams
        """
        return {
            "start_time": candlestick["e"],
            "open": candlestick["o"],
            "high": candlestick["h"],
            "low": candlestick["l"],
            "close": candlestick["c"],
            "volume": candlestick["v"],
            "close_time": candlestick["T"],
        }

    def produce(self, data):
        # Sample candlestick matrix for the same asset

        # Flatten and serialize candlestick data, then produce to Kafka topic
        candlestick_data = {
            "asset_name": self.symbol,
            "candlesticks": []
        }
        candle = self.map_websocket_klines(data)
        candlestick_data["candlesticks"].append(candle)

        # Produce message with asset name as key and candlestick data as value
        self.producer.send(self.topic, key=self.symbol, value=json.dumps(candlestick_data))

        # Flush self.producer buffer
        self.producer.flush()