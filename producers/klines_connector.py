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

from binquant.producers.produce_klines import KlinesProducer
from inbound_data.signals_base import SignalsBase
from scipy import stats
from shared.streaming.socket_client import SpotWebsocketStreamClient
from shared.utils import round_numbers


class KlinesConnector:
    def __init__(self) -> None:
        logging.info("Started Kafka producer SignalsInbound")
        self.last_processed_kline = {}
        self.client = SpotWebsocketStreamClient(
            on_message=self.on_message,
            on_close=self.handle_close,
            on_error=self.handle_error,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
        logging.error(f"Error research signals: {message}")
        pass

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
        ):

            klines_producer = KlinesProducer(symbol)
            klines_producer.produce(result["k"])


            close_price = float(result["k"]["c"])
            open_price = float(result["k"]["o"])
            data = self._get_candlestick(symbol, self.interval, stats=True)

            self.volatility = self.log_volatility(data)

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

            macd = data["macd"]
            macd_signal = data["macd_signal"]
            rsi = data["rsi"]

            if len(ma_100) == 0:
                msg = f"Not enough ma_100 data: {symbol}"
                print(msg)
                return

            # Average amplitude
            msg = None
            list_prices = numpy.array(data["trace"][0]["close"])
            self.sd = round_numbers(numpy.std(list_prices.astype(numpy.single)), 4)

            # historical lowest for short_buy_price
            lowest_price = numpy.min(
                numpy.array(data["trace"][0]["close"]).astype(numpy.single)
            )

            # COIN/BTC correlation: closer to 1 strong
            btc_correlation = data["btc_correlation"]

            if (
                self.market_domination_trend == "gainers"
                and self.market_domination_reversal
            ):
                buy_low_sell_high(
                    self,
                    close_price,
                    symbol,
                    rsi,
                    ma_25,
                    ma_7,
                    ma_100,
                )

                price_rise_15(
                    self,
                    close_price,
                    symbol,
                    data["trace"][0]["close"][-2],
                    p_value=pvalue,
                    r_value=rvalue,
                    btc_correlation=btc_correlation,
                )

                rally_or_pullback(
                    self,
                    close_price,
                    symbol,
                    lowest_price,
                    pvalue,
                    open_price,
                    ma_7,
                    ma_100,
                    ma_25,
                    slope,
                    btc_correlation,
                )

            fast_and_slow_macd(
                self,
                close_price,
                symbol,
                macd,
                macd_signal,
                ma_7,
                ma_25,
                ma_100,
                slope,
                intercept,
                rvalue,
                pvalue,
                stderr,
            )

            ma_candlestick_jump(
                self,
                close_price,
                open_price,
                ma_7,
                ma_100,
                ma_25,
                symbol,
                lowest_price,
                slope,
                intercept,
                rvalue,
                pvalue,
                stderr,
                btc_correlation=btc_correlation,
            )

            ma_candlestick_drop(
                self,
                close_price,
                open_price,
                ma_7,
                ma_100,
                ma_25,
                symbol,
                lowest_price,
                slope=slope,
                p_value=pvalue,
                btc_correlation=btc_correlation,
            )

            top_gainers_drop(
                self,
                close_price,
                open_price,
                ma_7,
                ma_100,
                ma_25,
                symbol,
                lowest_price,
                slope,
                btc_correlation,
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
