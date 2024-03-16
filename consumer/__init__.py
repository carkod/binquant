from kafka import KafkaConsumer
import json
import numpy
import pandas as pd

from datetime import datetime
from logging import info
from time import sleep, time
from algorithms.ma_candlestick_drop import ma_candlestick_drop
from algorithms.ma_candlestick_jump import ma_candlestick_jump
from shared.apis import BinbotApi
from shared.autotrade import process_autotrade_restrictions
from shared.streaming.socket_client import SpotWebsocketStreamClient
from scipy import stats
from shared.telegram_bot import TelegramBot
from shared.utils import handle_binance_errors, round_numbers
from typing import Literal

# Create a consumer instance
consumer = KafkaConsumer(
    'bitcoin', 'ethereum',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

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
        and symbol not in self.active_symbols
        and symbol not in self.last_processed_kline
    ):
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
        ma_candlestick_jump(
            self,
            close_price,
            open_price,
            ma_7,
            ma_100,
            ma_25,
            symbol,
            sd,
            self._send_msg,
            process_autotrade_restrictions,
            lowest_price,
            slope=slope,
            p_value=pvalue,
            r_value=rvalue,
        )

        if self.market_domination_trend == "losers":
            ma_candlestick_drop(
                self,
                close_price,
                open_price,
                ma_7,
                ma_100,
                ma_25,
                symbol,
                sd,
                self._send_msg,
                process_autotrade_restrictions,
                lowest_price,
                slope=slope,
                p_value=pvalue,
                r_value=rvalue,
            )

        
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


# Start consuming
for message in consumer:
    print(message)
