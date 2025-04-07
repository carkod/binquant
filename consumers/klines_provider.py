import json
import logging

import pandas as pd
from kafka import KafkaConsumer

from database import KafkaDB
from models.klines import KlineProduceModel
from producers.base import BaseProducer
from producers.technical_indicators import TechnicalIndicators
from shared.apis.binbot_api import BinanceApi, BinbotApi
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
        self.binbot_api = BinbotApi()
        self.binance_api = BinanceApi()
        self.consumer = consumer
        self.load_data_on_start()
        # 15 minutes default candles
        self.default_aggregation = {
            "open": "first",
            "close": "last",
            "high": "max",
            "low": "min",
            "close_time": "last",
            "open_time": "first",
        }
        self.df = pd.DataFrame()
        self.df_4h = pd.DataFrame()
        self.df_1h = pd.DataFrame()

    def load_data_on_start(self):
        self.active_pairs = self.binbot_api.get_active_pairs()
        self.base_producer = BaseProducer()
        self.base_producer.start_producer()
        self.producer = self.base_producer.producer

    def aggregate_data(self, results):
        if results:
            payload = json.loads(results)
            klines = KlineProduceModel.model_validate(payload)
            symbol = klines.symbol
            candles: list[KlineProduceModel] = self.raw_klines(
                symbol, interval=BinanceKlineIntervals.fifteen_minutes
            )

            if len(candles) == 0:
                logging.warning(f"{symbol} No data to do analytics")
                return

            # Pre-process
            self.df = pd.DataFrame(candles)
            self.df.resample("15Min", on="close_time").agg(self.default_aggregation)
            # Resample to 4 hour candles for TWAP
            self.df_4h = self.df.resample("4h", on="close_time").agg(
                self.default_aggregation
            )
            # Resample to 1 hour candles for Supertrend
            self.df_1h = self.df.resample("1h", on="close_time").agg(
                self.default_aggregation
            )
            # reverse the order to get the oldest data first, to dropnas and use latest date for technical indicators
            self.df = self.df[::-1].reset_index(drop=True)
            TechnicalIndicators(
                base_producer=self.base_producer,
                producer=self.producer,
                binbot_api=self.binbot_api,
                df=self.df,
                symbol=symbol,
                df_4h=self.df_4h,
                df_1h=self.df_1h,
            ).publish()

        pass
