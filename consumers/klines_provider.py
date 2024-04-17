import json
import os
import logging
import asyncio
import pandas as pd

from aiokafka import AIOKafkaConsumer
from producers.technical_indicators import TechnicalIndicators
from database import KafkaDB
from shared.enums import KafkaTopics
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark import SparkContext

# spark = SparkSession.builder.appName("Klines Statistics analyses")\
#     .config("compute.ops_on_diff_frames", "true").getOrCreate()

# allow series and/or dataframes to be attached to different dataframes
# ps.set_option('compute.ops_on_diff_frames', True)

class KlinesProvider(KafkaDB):
    """
    Pools, processes, agregates and provides klines data
    """
    def __init__(self, consumer: AIOKafkaConsumer):
        super().__init__()
        # If we don't instantiate separately, almost no messages are received
        self.consumer = consumer

    async def aggregate_data(self, results):

        if results.value:
            payload = json.loads(results.value)
            symbol = payload["symbol"]
            candles = self.raw_klines(symbol)

            if len(candles) == 0:
                logging.info(f'{symbol} No data to do analytics')
                return

            # self.check_kline_gaps(candles)
            # Pre-process
            df = pd.DataFrame(candles)
            TechnicalIndicators(df, symbol).publish()

        pass


