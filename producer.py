import math
import json
import logging
import numpy
import pandas as pd
import requests

from datetime import datetime
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

        # if total_threads > 1 or not self.max_request:
        #     for index in range(total_threads - 1):
        #         stream = params[(self.max_request + 1) :]
        #         if index == 0:
        #             stream = params[: self.max_request]
        #         self.client.klines(markets=stream, interval=self.interval)
        # else:
        stream = ['BCHUSDT', 'FXSUSDT']
        self.client.klines(markets=stream, interval=self.interval)
    
    def process_kline_stream(self, result):
        """
        Updates market data in DB for research
        """
        # Sleep 1 hour because of snapshot account request weight
        if datetime.now().time().hour == 0 and datetime.now().time().minute == 0:
            sleep(3600)

        symbol = result["k"]["s"]
        if (
            symbol
            and "k" in result
            and "s" in result["k"]
            # and symbol not in self.active_symbols
            # and symbol not in self.last_processed_kline
        ):
            print(symbol)
            close_price = float(result["k"]["c"])
            open_price = float(result["k"]["o"])
            data = self._get_candlestick(symbol, self.interval, stats=True)

            df = pd.DataFrame(
                {
                    "date": data["trace"][0]["x"],
                    "close": numpy.array(data["trace"][0]["close"]).astype(float),
                }
            )
            slope, intercept, rvalue, pvalue, stderr = stats.linregress(
                df["date"], df["close"]
            )

            if "error" in data and data["error"] == 1:
                return

            ma_100 = data["trace"][1]["y"]
            ma_25 = data["trace"][2]["y"]
            ma_7 = data["trace"][3]["y"]

            if len(ma_100) == 0:
                msg = f"Not enough ma_100 data: {symbol}"
                print(msg)
                return

            # Average amplitude
            msg = None
            list_prices = numpy.array(data["trace"][0]["close"])
            sd = round_numbers((numpy.std(list_prices.astype(numpy.single))), 2)

            # historical lowest for short_buy_price
            lowest_price = numpy.min(
                numpy.array(data["trace"][0]["close"]).astype(numpy.single)
            )

            # if self.market_domination_trend == "gainers":
            # ma_candlestick_jump(
            #     self,
            #     close_price,
            #     open_price,
            #     ma_7,
            #     ma_100,
            #     ma_25,
            #     symbol,
            #     sd,
            #     self._send_msg,
            #     process_autotrade_restrictions,
            #     lowest_price,
            #     slope=slope,
            #     p_value=pvalue,
            #     r_value=rvalue,
            # )

            # if self.market_domination_trend == "losers":
            #     ma_candlestick_drop(
            #         self,
            #         close_price,
            #         open_price,
            #         ma_7,
            #         ma_100,
            #         ma_25,
            #         symbol,
            #         sd,
            #         self._send_msg,
            #         process_autotrade_restrictions,
            #         lowest_price,
            #         slope=slope,
            #         p_value=pvalue,
            #         r_value=rvalue,
            #     )

            
            self.last_processed_kline[symbol] = time()

        # If more than 6 hours passed has passed
        # Then we should resume sending signals for given symbol
        if (
            symbol in self.last_processed_kline
            and (float(time()) - float(self.last_processed_kline[symbol])) > 6000
        ):
            del self.last_processed_kline[symbol]

        self.market_domination()
        pass


if __name__ == "__main__":
    producer = ResearchSignals()
    producer.start_stream()
