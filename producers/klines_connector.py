import json
import logging
import os
from datetime import datetime
from time import sleep, time
from faststream.kafka import KafkaBroker, KafkaRouter

import numpy
import pandas as pd

from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.price_changes import price_rise_15
from algorithms.rally import rally_or_pullback
from algorithms.top_gainer_drop import top_gainers_drop
from algorithms.coinrule import buy_low_sell_high, fast_and_slow_macd

from producers.produce_klines import KlinesProducer
from producers.base import ProducerBase
from scipy import stats
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.utils import round_numbers
from shared.exceptions import KlinesConnectorSocketException
from shared.enums import KafkaTopics
from producers.base import ProducerBase


broker = KafkaBroker(f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}')

class KlinesConnector(ProducerBase):
    def __init__(self) -> None:
        logging.info("Started Kafka producer SignalsInbound")
        self.last_processed_kline = {}
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )
        super().__init__()


    def handle_close(self, message):
        logging.info(f"Closing research signals: {message}")
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )
        self.start_stream()

    def handle_error(self, socket, message):
        raise KlinesConnectorSocketException(message)

    def on_message(self, ws, message):
        res = json.loads(message)

        if "result" in res:
            if not res["result"]:
                return

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

            candlestick_data = KlinesProducer(symbol).produce(result["k"])
            broker.publisher(candlestick_data, topic=KafkaTopics.candlestick_data_topic.value)

        pass
