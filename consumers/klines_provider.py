import json
import logging

import pandas as pd
from kafka import KafkaConsumer

from database import KafkaDB
from models.klines import KlineProduceModel
from producers.technical_indicators import TechnicalIndicators
from shared.enums import BinanceKlineIntervals

# spark = SparkSession.builder.appName("Klines Statistics analyses")\
#     .config("compute.ops_on_diff_frames", "true").getOrCreate()

# allow series and/or dataframes to be attached to different dataframes
# ps.set_option('compute.ops_on_diff_frames', True)


class KlinesProvider(KafkaDB):
    """
    Pools, processes, agregates and provides klines data
    """

    def __init__(self, consumer: KafkaConsumer):
        super().__init__()
        # If we don't instantiate separately, almost no messages are received
        self.consumer = consumer

    def aggregate_data(self, results):
        if results:
            payload = json.loads(results)
            klines = KlineProduceModel.model_validate(payload)
            symbol = klines.symbol
            candles: list[dict] = self.raw_klines(
                symbol, interval=BinanceKlineIntervals.fifteen_minutes
            )

            if len(candles) == 0:
                logging.info(f"{symbol} No data to do analytics")
                return

            # Pre-process
            self.df = pd.DataFrame(candles)
            self.df.resample("15Min", on="close_time").agg(
                {
                    "open": "first",
                    "close": "last",
                    "high": "max",
                    "low": "min",
                    "close_time": "last",
                    "open_time": "first",
                }
            )
            # reverse the order to get the oldest data first, to dropnas and use latest date for technical indicators
            self.df = self.df[::-1].reset_index(drop=True)
            TechnicalIndicators(self.df, symbol).publish()

        pass
