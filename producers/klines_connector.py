import json
import logging

from kafka import KafkaProducer

from producers.produce_klines import KlinesProducer
from shared.apis import BinbotApi
from shared.exceptions import WebSocketError
from shared.streaming.socket_client import SpotWebsocketStreamClient


class KlinesConnector(BinbotApi):
    def __init__(self, producer: KafkaProducer, interval: str = "1m") -> None:
        logging.info("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )

        self.producer = producer
        self.autotrade_settings = self.get_autotrade_settings()
        self.exchange_info = self._exchange_info()

    def handle_close(self, message):
        logging.info(f"Closing research signals: {message}")
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )
        self.start_stream()

    def handle_error(self, socket, message):
        # logging.error(f"Error research signals: {message}")
        raise WebSocketError(message)

    def on_message(self, ws, message):
        res = json.loads(message)

        if "result" in res:
            print(f'Subscriptions: {res["result"]}')

        if "e" in res and res["e"] == "kline":
            self.process_kline_stream(res)

    def start_stream(self):
        symbols = self.get_symbols()
        markets = [f'{symbol["id"].lower()}@kline_{self.interval}' for symbol in symbols]
        self.client.klines(markets=markets)

    def process_kline_stream(self, result):
        """
        Updates market data in DB for research
        """

        symbol = result["k"]["s"]
        if symbol and "k" in result and result["k"]["x"]:
            klines_producer = KlinesProducer(self.producer, symbol)
            klines_producer.store(result)

        pass
