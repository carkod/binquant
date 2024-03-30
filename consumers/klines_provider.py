import json
import os
import logging
from datetime import datetime
from aiokafka import AIOKafkaConsumer, TopicPartition
from kafka import KafkaConsumer, KafkaProducer
from producers.base import BaseProducer
from shared.enums import KafkaTopics
from shared.utils import round_numbers
from models.klines import KlineModel, SparkKlineSchema

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window


from numpy import diff, partition
import numpy

spark = SparkSession.builder.appName("Klines Statistics analyses").config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "").getOrCreate()

class KlinesProvider:
    """
    Pools, processes, agregates and provides klines data
    """
    def __init__(self, consumer: AIOKafkaConsumer):
        super().__init__()
        self.consumer = consumer
        self.topic_partition_ids = []
        self.topic_partition = None
         # Number of klines to aggregate, 100+ for MA
        self.klines_horizon = 3
        self.interval = "15m"
        self.current_partition = 0
        self.candles = []

    def set_partitions(self, partition):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
            value_deserializer=lambda m: json.loads(m),
        )
        self.consumer.assign([TopicPartition(KafkaTopics.klines_store_topic.value, partition)])
    
    def days(self, secs):
        return secs * 86400

    def check_kline_gaps(self, data):
        ot = datetime.fromtimestamp(round(int(data["open_time"])/1000))
        ct = datetime.fromtimestamp(round(int(data["close_time"])/1000))
        time_diff = ct - ot
        if self.interval == "15m":
            if time_diff > 15:
                logging.warn(f'Gap in {data["symbol"]} klines: {time_diff.min} minutes')
    
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

    def moving_averages(self, df, symbol):
        """
        Calculate moving averages for 7, 25, 100 days
        this also takes care of Bollinguer bands
        """
        length = df.count()
        if length > 7:
            w_7 = (Window.orderBy(col("close_time").cast("long")).rowsBetween(-self.days(7), 0))
            df = df.withColumn("ma_7", avg("close").over(w_7))
        else:
            logging.info(f'{symbol} Not enough data to aggregate ma_7')

        if length > 25:
            w_25 = (Window.orderBy(col("close_time").cast("long")).rowsBetween(-self.days(25), 0))
            df = df.withColumn("ma_25", avg("close").over(w_25))
        else:
            logging.info(f'{symbol} Not enough data to aggregate ma_25')
        
        if length > 100:
            w_100 = (Window.orderBy(col("close_time").cast("long")).rowsBetween(-self.days(100), 0))
            df = df.withColumn("ma_100", avg("close").over(w_100))
        else:
            logging.info(f'{symbol} Not enough data to aggregate ma_100')

        return df

    async def technical_analyses(self):

        async for message in self.consumer:
            payload = json.loads(message.value)
            symbol = payload["symbol"]
            candles = self.raw_klines(symbol)
            
            if len(candles) == 0:
                logging.info(f'{symbol} No data to do analytics')
                return

            self.check_kline_gaps(candles)
            # self.volatility = self.log_volatility(candles)

            df = spark.createDataFrame(candles, SparkKlineSchema)
            df = self.moving_averages(df, symbol)


        pass

        # seek_offset = message.offset - self.klines_horizon
        # if seek_offset > 0:
            # self.consumer.seek(self.topic_partition, seek_offset)
        # test = await self.consumer.getone()

        # for tp, messages in data.items():
        #     print("Partition; ", payload.partition, "Current partition: ", self.current_partition, "Symbol: ", payload.kline.symbol)
        #     for msg in messages:
        #         candle = payload.kline
        #         if candle.candle_closed:
        #             self.consumer.commit()
        #         ot = datetime.fromtimestamp(round(candle.open_time/1000))
        #         ct = datetime.fromtimestamp(round(candle.close_time/1000))

        #         time_diff = ct - ot
        #         # print("Time difference: ", time_diff)
        #         # print("Consumed asset: ", candle.symbol, "Closed? ", candle.candle_closed)

        #         self.candles.append(candle)
        #         ProduceStatsAnalyses(self.candles, payload.partition).technical_analyses()
        # else:
        #     data = json.loads(message.value)
        #     # print(f'Not enough data to aggregate {data["symbol"]}')
        #     pass
    

