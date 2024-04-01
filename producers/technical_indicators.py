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
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "")
    .getOrCreate()
)

class TechnicalIndicators:
    def __init__(self, df) -> None:
        base_producer = BaseProducer()
        self.producer = base_producer.start_producer()
        self.df = df
        pass

    def check_kline_gaps(self, data):
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

    def moving_averages(self, df, period=7):
        """
        Calculate moving averages for 7, 25, 100 days
        this also takes care of Bollinguer bands
        """
        window = Window.orderBy(col("close_time").cast("long")).rowsBetween(
            -self.days(period), 0
        )
        df = df.withColumn(f"ma_{period}", avg("close").over(window))

        return df

    def macd(self, df):
        """
        Moving Average Convergence Divergence (MACD) indicator
        https://www.alpharithms.com/calculate-macd-python-272222/
        """

        k = df[4].ewm(span=12, adjust=False, min_periods=12).mean()
        # Get the 12-day EMA of the closing price
        d = df[4].ewm(span=26, adjust=False, min_periods=26).mean()
        # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
        macd = k - d
        # Get the 9-Day EMA of the MACD for the Trigger line
        # Get the 9-Day EMA of the MACD for the Trigger line
        macd_s = macd.ewm(span=9, adjust=False, min_periods=9).mean()

        return macd, macd_s

    def rsi(self, df):
        """
        Relative Strength Index (RSI) indicator
        https://www.qmr.ai/relative-strength-index-rsi-in-python/
        """

        change = df[4].astype(float).diff()
        change.dropna(inplace=True)
        # Create two copies of the Closing price Series
        change_up = change.copy()
        change_down = change.copy()

        change_up[change_up < 0] = 0
        change_down[change_down > 0] = 0

        # Verify that we did not make any mistakes
        change.equals(change_up + change_down)

        # Calculate the rolling average of average up and average down
        avg_up = change_up.rolling(14).mean()
        avg_down = change_down.rolling(14).mean().abs()

        rsi = 100 * avg_up / (avg_up + avg_down)
        return rsi

    def publish(self):
        """
        Publish processed data with ma_7, ma_25, ma_100, macd, macd_signal, rsi

        Algorithms should consume this data
        """

        length = self.df.count()
        self.df = self.moving_averages(self.df, 7)
        self.df = self.moving_averages(self.df, 25)
        self.df = self.moving_averages(self.df, 100)

        self.producer.send(
            KafkaTopics.technical_indicators.value, value=self.df.toJSON().collect()
        )

        pass
