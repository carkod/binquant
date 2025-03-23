import json
import logging

from kafka import KafkaProducer

from producers.produce_klines import KlinesProducer
from shared.apis import BinbotApi
from shared.enums import BinanceKlineIntervals
from shared.exceptions import WebSocketError
from shared.streaming.socket_client import SpotWebsocketStreamClient


class KlinesConnector(BinbotApi):
    def __init__(
        self,
        producer: KafkaProducer,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.one_minute,
    ) -> None:
        logging.info("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        self.client: SpotWebsocketStreamClient = self.connect_client()

        self.producer = producer
        self.autotrade_settings = self.get_autotrade_settings()

    def connect_client(self):
        client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )
        return client

    def handle_close(self, message):
        logging.info(f"Closing research signals: {message}")
        self.client = self.connect_client()
        self.start_stream()

    def handle_error(self, socket, message):
        logging.error(f"Error research signals: {message}")
        raise WebSocketError(message)

    def on_message(self, ws, message):
        res = json.loads(message)

        if "e" in res and res["e"] == "kline":
            self.process_kline_stream(res)

    def start_stream(self) -> None:
        """
        Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to the current klines/candlestick every second.
        Stream Name: <symbol>@kline_<interval>
        Check BinanceKlineIntervals Enum for possible values
        Update Speed: 2000ms
        """
        symbols = self.get_symbols()
        markets = [
            f'{symbol["id"].lower()}@kline_{self.interval.value}' for symbol in symbols
        ]
        self.client.send_message_to_server(
            markets, action=self.client.ACTION_SUBSCRIBE, id=1
        )

    def process_kline_stream(self, result):
        """
        Updates market data in DB for research
        """

        symbol = result["k"]["s"]
        if symbol and "k" in result and result["k"]["x"]:
            klines_producer = KlinesProducer(self.producer, symbol)
            klines_producer.store(result)

        pass
