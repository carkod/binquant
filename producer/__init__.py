import math
import json
import logging
import numpy
import pandas as pd
import requests

from datetime import datetime, timedelta
from logging import info
from time import sleep, time
from algorithms.ma_candlestick_drop import ma_candlestick_drop
from algorithms.ma_candlestick_jump import ma_candlestick_jump
from shared.autotrade import process_autotrade_restrictions
from shared.streaming.socket_client import SpotWebsocketStreamClient
from scipy import stats
from shared.telegram_bot import TelegramBot
from shared.utils import round_numbers
from shared.setup_signals import SetupSignals

from kafka import KafkaProducer

# Create a producer instance
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


class ResearchSignals(SetupSignals):
    def __init__(self):
        info("Started research signals")
        self.last_processed_kline = {}
        self.client = SpotWebsocketStreamClient(on_message=self.on_message, is_combined=True, on_close=self.handle_close)
        super().__init__()

    def new_tokens(self, projects) -> list:
        check_new_coin = (
            lambda coin_trade_time: (
                datetime.now() - datetime.fromtimestamp(coin_trade_time)
            ).days
            < 1
        )

        new_pairs = [
            item["rebaseCoin"] + item["asset"]
            for item in projects["data"]["completed"]["list"]
            if check_new_coin(int(item["coinTradeTime"]) / 1000)
        ]

        return new_pairs

    def handle_close(self, message):
        print(f'Closing research signals: {message}')
        self.client = SpotWebsocketStreamClient(on_message=self.on_message, is_combined=True, on_close=self.handle_close)
        self.start_stream()

    def on_message(self, ws, message):
        res = json.loads(message)

        if "result" in res:
                print(f'Subscriptions: {res["result"]}')

        if "data" in res:
            if "e" in res["data"] and res["data"]["e"] == "kline":
                self.process_kline_stream(res["data"])
            else:
                print(f'Error: {res["data"]}')
                self.client.stop()

    def start_stream(self):
        logging.info("Initializing Research signals")
        self.load_data()
        exchange_info = self._exchange_info()
        raw_symbols = set(coin["symbol"] for coin in exchange_info["symbols"] if coin["status"] == "TRADING" and coin["symbol"].endswith(self.settings["balance_to_use"]))

        black_list = set(x["pair"] for x in self.blacklist_data)
        market = raw_symbols - black_list
        params = []
        for m in market:
            params.append(f"{m.lower()}")

        total_threads = math.floor(len(market) / self.max_request) + (
            1 if len(market) % self.max_request > 0 else 0
        )
        # It's not possible to have websockets with more 950 pairs
        # So set default to max 950
        stream = params[:950]

        if total_threads > 1 or not self.max_request:
            for index in range(total_threads - 1):
                stream = params[(self.max_request + 1) :]
                if index == 0:
                    stream = params[: self.max_request]
                self.client.klines(markets=stream, interval=self.interval)
        else:
            self.client.klines(markets=stream, interval=self.interval)

    

def main():
    key = config.POLYGON_API
    my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message)
    my_client.run_async()

    my_client.subscribe("AM.RIOT, AM.NET")
    time.sleep(1)

    #my_client.close_connection()


if __name__ == "__main__":
    main()