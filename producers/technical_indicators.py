from datetime import datetime, timedelta
import logging
import pandas
from typing import Literal

from algorithms.top_gainer_drop import top_gainers_drop
from algorithms.rally import rally_or_pullback
from shared.apis import BinbotApi
from producers.base import BaseProducer
from algorithms.ma_candlestick import ma_candlestick_jump, ma_candlestick_drop
from algorithms.coinrule import fast_and_slow_macd

class TechnicalIndicators(BinbotApi):
    def __init__(self, df, symbol) -> None:
        self.base_producer = BaseProducer()
        self.base_producer.start_producer()
        self.producer = self.base_producer.producer
        self.df = df
        self.symbol = symbol
        self.market_domination_trend = None
        self.market_domination_reversal = None
        pass

    def check_kline_gaps(self, data):
        """
        Check data consistency

        Currently not implemented with kafka streams data
        as we need to check the nature of such gaps, as now
        data needs to be aggregated for larger windows > 1m
        i.e. 15m -> aggregate 1m * 15
        """

        ot = datetime.fromtimestamp(round(int(data["open_time"]) / 1000))
        ct = datetime.fromtimestamp(round(int(data["close_time"]) / 1000))
        time_diff = ct - ot
        if self.interval == "15m":
            if time_diff > 15:
                logging.warn(f'Gap in {data["symbol"]} klines: {time_diff.min} minutes')
    
    def days(self, secs):
        return secs * 86400

    def define_strategy(self):
        """
        If market domination reversal is true, then it's already
        implied that it's an uptred (long strategy)
        from the market domination function
        """
        self.market_domination()
        trend = None
        if self.market_domination_reversal is True:
            trend = "uptrend"

        if self.market_domination_reversal is False:
            trend = "downtrend"

        if not self.market_domination and self.market_domination_reversal is None:
            trend = None

        return trend

    def calculate_slope(candlesticks):
        """
        Slope = 1: positive, the curve is going up
        Slope = -1: negative, the curve is going down
        Slope = 0: vertical movement
        """
        # Ensure the candlesticks list has at least two elements
        if len(candlesticks) < 2:
            return None

        # Calculate the slope
        previous_close = candlesticks["trace"][0]["close"]
        for candle in candlesticks["trace"][1:]:
            current_close = candle["close"]
            if current_close > previous_close:
                slope = 1
            elif current_close < previous_close:
                slope = -1
            else:
                slope = 0

            previous_close = current_close

        return slope

    def moving_averages(self, period=7):
        """
        Calculate moving averages for 7, 25, 100 days
        this also takes care of Bollinguer bands
        """
        self.df[f'ma_{period}'] = self.df["close"].rolling(window=period).mean()

    def macd(self):
        """
        Moving Average Convergence Divergence (MACD) indicator
        https://www.alpharithms.com/calculate-macd-python-272222/
        """

        k = self.df["close"].ewm(span=12, min_periods=12).mean()
        # Get the 12-day EMA of the closing price
        d = self.df["close"].ewm(span=26, min_periods=26).mean()
        # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
        macd = k - d
        # Get the 9-Day EMA of the MACD for the Trigger line
        # Get the 9-Day EMA of the MACD for the Trigger line
        macd_s = macd.ewm(span=9, min_periods=9).mean()

        self.df["macd"] = macd
        self.df["macd_signal"] = macd_s

    def rsi(self):
        """
        Relative Strength Index (RSI) indicator
        https://www.qmr.ai/relative-strength-index-rsi-in-python/
        """

        change = self.df["close"].astype(float).diff()

        gain = change.mask(change < 0, 0.0)
        loss = -change.mask(change > 0, -0.0)


        # Verify that we did not make any mistakes
        change.equals(gain + loss)

        # Calculate the rolling average of average up and average down
        avg_up = gain.rolling(14).mean()
        avg_down = loss.rolling(14).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        self.df["rsi"] = rsi


    def bollinguer_spreads(self):
        """
        Calculates spread based on bollinger bands,
        for later use in take profit and stop loss

        Returns:
        - top_band: diff between ma_25 and ma_100
        - bottom_band: diff between ma_7 and ma_25
        """

        band_1 = (abs((self.df["ma_100"] - self.df["ma_25"])) / self.df["ma_100"]) * 100
        band_2 = (abs((self.df["ma_25"] - self.df["ma_7"])) / self.df["ma_25"]) * 100

        self.df["bollinguer_band_1"] = band_1
        self.df["bollinguer_band_2"] = band_2
        print(f'band_1: {self.df["bollinguer_band_1"][-1:].value}, band_2: {self.df["bollinguer_band_2"][-1:]}')

    def log_volatility(self, window_size=7):
        """
        Volatility (standard deviation of returns) using logarithm, this normalizes data
        so it's easily comparable with other assets

        Returns:
        - Volatility in percentage
        """
        # log_volatility = numpy.log(self.df["close"].pct_change().rolling(window_size).std())
        log_volatility = pandas.Series(self.df["close"]).astype(float).pct_change().rolling(window_size).std()
        self.df["perc_volatility"] = log_volatility
    
    def market_domination(self) -> Literal["gainers", "losers", None]:
        """
        Get data from gainers and losers endpoint to analyze market trends

        We want to know when it's more suitable to do long positions
        when it's more suitable to do short positions
        For now setting threshold to 70% i.e.
        if > 70% of assets in a given market (USDT) dominated by gainers
        if < 70% of assets in a given market dominated by losers
        Establish the timing
        """
        if datetime.now().minute == 0 or self.market_domination_trend == None:
            logging.info(
                f"Performing market domination analyses. Current trend: {self.market_domination_trend}"
            )
            data = self.get_market_domination_series()
            # reverse to make latest series more important
            data["gainers_count"].reverse()
            data["losers_count"].reverse()
            gainers_count = data["gainers_count"]
            losers_count = data["losers_count"]
            self.market_domination_trend = "neutral"
            if gainers_count[-1] > losers_count[-1]:
                self.market_domination_trend = "gainers"

                # Check reversal
                if gainers_count[-2] < losers_count[-2]:
                    # Positive reversal
                    self.market_domination_reversal = True

            else:
                self.market_domination_trend = "losers"

                if gainers_count[-2] > losers_count[-2]:
                    # Negative reversal
                    self.market_domination_reversal = False

            self.btc_change_perc = self.get_latest_btc_price()
            reversal_msg = ""
            if self.market_domination_reversal is not None:
                reversal_msg = f"{'Positive reversal' if self.market_domination_reversal else 'Negative reversal'}"

            logging.info(f"Current USDT market trend is: {reversal_msg}. BTC 24hr change: {self.btc_change_perc}")
            self.market_domination_ts = datetime.now() + timedelta(hours=1)
        pass


    def publish(self):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        if self.df.close.size > 0:
            # Bolliguer bands
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.moving_averages(7)
            self.moving_averages(25)
            self.moving_averages(100)

            # Oscillators
            self.macd()
            self.rsi()

            # Bollinguer bands
            self.bollinguer_spreads()

            self.log_volatility()

            # Post-processing
            self.df.reset_index(drop=True, inplace=True)

            close_price = float(self.df.close[len(self.df.close) - 1])
            open_price = float(self.df.open[len(self.df.open) - 1])
            macd = float(self.df.macd[len(self.df.macd) - 1])
            macd_signal = float(self.df.macd_signal[len(self.df.macd_signal) - 1])

            ma_7 = float(self.df.ma_7[len(self.df.ma_7) - 1])
            ma_7_prev = float(self.df.ma_7[len(self.df.ma_7) - 2])
            ma_25 = float(self.df.ma_25[len(self.df.ma_25) - 1])
            ma_25_prev = float(self.df.ma_25[len(self.df.ma_25) - 2])
            ma_100 = float(self.df.ma_100[len(self.df.ma_100) - 1])
            ma_100_prev = float(self.df.ma_100[len(self.df.ma_100) - 2])

            volatility = float(self.df.perc_volatility[len(self.df.perc_volatility) - 1])

            fast_and_slow_macd(
                self,
                close_price,
                self.symbol,
                macd,
                macd_signal,
                ma_7,
                ma_25,
                ma_100,
                volatility
            )

            ma_candlestick_jump(
                self,
                close_price,
                open_price,
                self.symbol,
                ma_7,
                ma_25,
                ma_100,
                ma_7_prev,
                ma_25_prev,
                ma_100_prev,
                volatility
            )

            ma_candlestick_drop(
                self,
                close_price,
                open_price,
                self.symbol,
                ma_7,
                ma_25,
                ma_100,
                ma_7_prev,
                ma_25_prev,
                ma_100_prev,
                volatility
            )


            # This function calls a lot ticker24 revise it before uncommenting
            # rally_or_pullback(
            #     self,
            #     close_price,
            #     open_price,
            #     self.symbol,
            #     ma_7,
            #     ma_25,
            #     ma_100,
            #     ma_7_prev,
            #     ma_25_prev,
            #     ma_100_prev,
            #     volatility
            # )

            # top_gainers_drop(
            #     self,
            #     close_price,
            #     open_price,
            #     self.symbol,
            #     ma_7,
            #     ma_25,
            #     ma_100,
            #     ma_7_prev,
            #     ma_25_prev,
            #     ma_100_prev,
            #     volatility
            # )

        return
