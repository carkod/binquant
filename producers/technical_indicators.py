import json
from datetime import datetime
import logging
import numpy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date
from pyspark.sql.window import Window
from shared.utils import round_numbers
from producers.base import BaseProducer
from shared.enums import KafkaTopics

spark = (
    SparkSession.builder.appName("Klines Statistics analyses")
    # .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("compute.ops_on_diff_frames", "true")
    .getOrCreate()
)

class TechnicalIndicators:
    def __init__(self, df, symbol) -> None:
        base_producer = BaseProducer()
        self.producer = base_producer.start_producer()
        self.df = df
        self.symbol = symbol
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

    def log_volatility(self, data):
        """
        Volatility (standard deviation of returns) using logarithm, this normalizes data
        so it's easily comparable with other assets

        Returns:
        - Volatility in percentage
        """
        closing_prices = numpy.array(data["trace"][0]["close"]).astype(float)
        returns = numpy.log(closing_prices[1:] / closing_prices[:-1])
        volatility = numpy.std(returns)
        perc_volatility = round_numbers(volatility * 100, 6)
        return perc_volatility

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
        # Create two copies of the Closing price Series
        # change_up = change.copy()
        # change_down = change.copy()

        gain = change.mask(change < 0, 0.0)
        loss = -change.mask(change > 0, -0.0)


        # Verify that we did not make any mistakes
        change.equals(gain + loss)

        # Calculate the rolling average of average up and average down
        avg_up = gain.rolling(14).mean()
        avg_down = loss.rolling(14).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        self.df["rsi"] = rsi

    def publish(self):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        length = self.df.size
        if length > 0:
            # Bolliguer bands
            # This would be an ideal process to spark.parallelize
            # not sure what's the best way with pandas-on-spark dataframe
            self.moving_averages(7)
            self.moving_averages(25)
            self.moving_averages(100)

            # Oscillators
            self.macd()
            self.rsi()

            # Post-processing
            self.df.dropna(inplace=True)

            self.producer.send(
                KafkaTopics.technical_indicators.value, value=self.df.to_json()
            )

        pass
