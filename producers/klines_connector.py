import json
import logging
import os
from datetime import datetime
from time import sleep, time

import numpy
import pandas as pd
from kafka import KafkaProducer

from algorithms.ma_candlestick import ma_candlestick_drop, ma_candlestick_jump
from algorithms.price_changes import price_rise_15
from algorithms.rally import rally_or_pullback
from algorithms.top_gainer_drop import top_gainers_drop
from algorithms.coinrule import buy_low_sell_high, fast_and_slow_macd

from producers.produce_klines import KlinesProducer
from inbound_data.signals_base import SignalsBase
from scipy import stats
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.utils import round_numbers
from shared.exceptions import WebSocketError

class KlinesConnector(SignalsBase):
    def __init__(self) -> None:
        logging.info("Started Kafka producer SignalsInbound")
        self.interval = "1m"
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
        # Sleep 1 hour because of snapshot account request weight
        if datetime.now().time().hour == 0 and datetime.now().time().minute == 0:
            sleep(1800)

        symbol = result["k"]["s"]
        if (
            symbol
            and "k" in result
            and "s" in result["k"]
            and result["k"]["x"]
        ):

            klines_producer = KlinesProducer(symbol)
            klines_producer.store(result["k"])


        pass
