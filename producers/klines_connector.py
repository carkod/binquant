import json
import logging

from kafka import KafkaProducer
from producers.produce_klines import KlinesProducer
from inbound_data.signals_base import SignalsBase
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.exceptions import WebSocketError
class KlinesConnector(SignalsBase):
    def __init__(self, producer: KafkaProducer, interval: str="1m") -> None:
        logging.info("Started Kafka producer SignalsInbound")
        super().__init__()
        self.interval = interval
        self.last_processed_kline = {}
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )

        self.symbol_partitions = []
        self.partition_count = 0
        self.producer = producer


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
        logging.info("Initializing Research signals")
        self.load_data()
        exchange_info = self._exchange_info()
        raw_symbols = set(
            coin["symbol"]
            for coin in exchange_info["symbols"]
            if coin["status"] == "TRADING"
            and coin["symbol"].endswith(self.settings["balance_to_use"])
        )

        black_list = set(x["pair"] for x in self.blacklist_data)
        market = raw_symbols - black_list
        params = []
        subscription_list = []
        for m in market:
            params.append(f"{m.lower()}")
            if m in black_list:
                subscription_list.append(
                    {
                        "_id": m,
                        "pair": m,
                        "blacklisted": True,
                    }
                )
            else:
                subscription_list.append(
                    {
                        "_id": m,
                        "pair": m,
                        "blacklisted": False,
                    }
                )

        # update DB
        self.update_subscribed_list(subscription_list)
        self.client.klines(markets=params, interval=self.interval)

    def process_kline_stream(self, result):
        """
        Updates market data in DB for research
        """

        symbol = result["k"]["s"]
        if (
            symbol
            and "k" in result
            and "s" in result["k"]
        ):
            # Allocate partition for each symbol and dedup
            # try:
            #     self.partition_obj[symbol]
            # except KeyError:
            #     self.partition_obj[symbol] = self.partition_count
            #     self.partition_count += 1
            #     pass
            klines_producer = KlinesProducer(self.producer, symbol)
            klines_producer.store(result["k"])

        pass
