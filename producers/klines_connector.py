import json
import logging

from confluent_kafka import Producer

from producers.produce_klines import KlinesProducer
from shared.apis.binbot_api import BinbotApi
from shared.enums import BinanceKlineIntervals
from shared.exceptions import WebSocketError
from shared.streaming.socket_client import SpotWebsocketStreamClient


class KlinesConnector(BinbotApi):
    MAX_MARKETS_PER_CLIENT = 400

    def __init__(
        self,
        producer: Producer,
        interval: BinanceKlineIntervals = BinanceKlineIntervals.five_minutes,
    ) -> None:
        logging.debug("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        self.producer = producer
        self.autotrade_settings = self.get_autotrade_settings()
        self.clients: list[SpotWebsocketStreamClient] = []

    def connect_client(self, on_message, on_close, on_error):
        client = SpotWebsocketStreamClient(
            on_message=on_message,
            on_close=on_close,
            on_error=on_error,
        )
        return client

    def handle_close(self, idx, message):
        logging.info(f"Closing research signals for client {idx}: {message}")
        # Reconnect the client and restart its stream
        self.clients[idx] = self.connect_client(
            lambda ws, msg: self.on_message(idx, ws, msg),
            lambda msg: self.handle_close(idx, msg),
            lambda ws, msg: self.handle_error(idx, ws, msg),
        )
        self.start_stream_for_client(idx)

    def handle_error(self, idx, socket, message):
        logging.error(f"Error research signals for client {idx}: {message}")
        raise WebSocketError(message)

    def on_message(self, idx, ws, message):
        try:
            res = json.loads(message)
        except Exception as e:
            logging.error(
                f"Failed to decode message (client {idx}): {e}, raw: {message}"
            )
            return
        if "e" in res and res["e"] == "kline":
            logging.debug(f"Kline event received (client {idx}): {res}")
            self.process_kline_stream(res)
        else:
            logging.debug(f"Non-kline event received (client {idx}): {res}")

    def start_stream(self) -> None:
        """
        Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to the current klines/candlestick every second.
        Stream Name: <symbol>@kline_<interval>
        Check BinanceKlineIntervals Enum for possible values
        Update Speed: 2000ms

        Split symbols into chunks of MAX_MARKETS_PER_CLIENT
        """
        symbols = self.get_symbols()
        symbol_chunks = [
            symbols[i : i + self.MAX_MARKETS_PER_CLIENT]
            for i in range(0, len(symbols), self.MAX_MARKETS_PER_CLIENT)
        ]
        for idx, chunk in enumerate(symbol_chunks):
            markets = [
                f"{symbol['id'].lower()}@kline_{self.interval.value}"
                for symbol in chunk
            ]
            logging.debug(f"Subscribing to markets (client {idx}): {markets}")
            client = self.connect_client(
                lambda ws, msg, idx=idx: self.on_message(idx, ws, msg),
                lambda msg, idx=idx: self.handle_close(idx, msg),
                lambda ws, msg, idx=idx: self.handle_error(idx, ws, msg),
            )
            self.clients.append(client)
            self.start_stream_for_client(idx)

    def start_stream_for_client(self, idx):
        symbols = self.get_symbols()
        symbol_chunks = [
            symbols[i : i + self.MAX_MARKETS_PER_CLIENT]
            for i in range(0, len(symbols), self.MAX_MARKETS_PER_CLIENT)
        ]
        chunk = symbol_chunks[idx]
        markets = [
            f"{symbol['id'].lower()}@kline_{self.interval.value}" for symbol in chunk
        ]
        logging.debug(f"(Re)subscribing to markets (client {idx}): {markets}")

        self.clients[idx].send_message_to_server(
            markets, action=self.clients[idx].ACTION_SUBSCRIBE, id=1
        )
        logging.debug(
            f"Subscription message sent for markets (client {idx}): {markets}"
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
